package pod

import (
	"context"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flytestdlib/logger"

	"github.com/flyteorg/flyteplugins/go/tasks/errors"
	pluginsCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core/template"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/flytek8s"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/utils"

	"github.com/imdario/mergo"

	v1 "k8s.io/api/core/v1"
)

const (
	SidecarTaskType = "sidecar"
)

// Why, you might wonder do we recreate the generated go struct generated from the plugins.SidecarJob proto? Because
// although we unmarshal the task custom json, the PodSpec itself is not generated from a proto definition,
// but a proper go struct defined in k8s libraries. Therefore we only unmarshal the sidecar as a json, rather than jsonpb.
type sidecarJob struct {
	PodSpec              *v1.PodSpec
	PrimaryContainerName string
	Annotations          map[string]string
	Labels               map[string]string
}

type sidecarPodBuilder struct {
}

func (sidecarPodBuilder) getPrimaryContainerName(taskCtx pluginsCore.TaskExecutionContext) (string, error) {
	return getPrimaryContainerNameFromTask(taskCtx)
}

func (sidecarPodBuilder) buildPodSpec(ctx context.Context, task *core.TaskTemplate, taskCtx pluginsCore.TaskExecutionContext) (*v1.PodSpec, error) {
	var podSpec v1.PodSpec
	switch task.TaskTypeVersion {
	case 0:
		// Handles pod tasks when they are defined as Sidecar tasks and marshal the podspec using k8s proto.
		sidecarJob := sidecarJob{}
		err := utils.UnmarshalStructToObj(task.GetCustom(), &sidecarJob)
		if err != nil {
			return nil, errors.Errorf(errors.BadTaskSpecification,
				"invalid TaskSpecification [%v], Err: [%v]", task.GetCustom(), err.Error())
		}

		if sidecarJob.PodSpec == nil {
			return nil, errors.Errorf(errors.BadTaskSpecification,
				"invalid TaskSpecification, nil PodSpec [%v]", task.GetCustom())
		}

		podSpec = *sidecarJob.PodSpec
	case 1:
		// Handles pod tasks that marshal the pod spec to the task custom.
		err := utils.UnmarshalStructToObj(task.GetCustom(), &podSpec)
		if err != nil {
			return nil, errors.Errorf(errors.BadTaskSpecification,
				"Unable to unmarshal task custom [%v], Err: [%v]", task.GetCustom(), err.Error())
		}
	default:
		// Handles pod tasks that marshal the pod spec to the k8s_pod task target.
		if task.GetK8SPod() == nil || task.GetK8SPod().PodSpec == nil {
			return nil, errors.Errorf(errors.BadTaskSpecification,
				"Pod tasks with task type version > 1 should specify their target as a K8sPod with a defined pod spec")
		}

		err := utils.UnmarshalStructToObj(task.GetK8SPod().PodSpec, &podSpec)
		if err != nil {
			return nil, errors.Errorf(errors.BadTaskSpecification,
				"Unable to unmarshal task custom [%v], Err: [%v]", task.GetCustom(), err.Error())
		}
	}

	// Set the restart policy to *not* inherit from the default so that a completed pod doesn't get caught in a
	// CrashLoopBackoff after the initial job completion.
	podSpec.RestartPolicy = v1.RestartPolicyNever

	return &podSpec, nil
}

// TODO figure out if I still need this or not
func getPrimaryContainerNameFromConfig(task *core.TaskTemplate) (string, error) {
	if len(task.GetConfig()) == 0 {
		return "", errors.Errorf(errors.BadTaskSpecification,
			"invalid TaskSpecification, config needs to be non-empty and include missing [%s] key", PrimaryContainerKey)
	}

	primaryContainerName, ok := task.GetConfig()[PrimaryContainerKey]
	if !ok {
		return "", errors.Errorf(errors.BadTaskSpecification,
			"invalid TaskSpecification, config missing [%s] key in [%v]", PrimaryContainerKey, task.GetConfig())
	}

	return primaryContainerName, nil
}

func getPrimaryContainerNameFromTask(taskCtx pluginsCore.TaskExecutionContext) (string, error) {
	primaryContainerName := taskCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()
	if primaryContainerName == "" {
		return "", errors.Errorf(errors.BadTaskSpecification, "invalid TaskSpecification, missing generated name")
	}
	return primaryContainerName, nil
}

func mergeMapInto(src map[string]string, dst map[string]string) {
	for key, value := range src {
		dst[key] = value
	}
}

func (sidecarPodBuilder) updatePodMetadata(ctx context.Context, pod *v1.Pod, task *core.TaskTemplate, taskCtx pluginsCore.TaskExecutionContext) error {
	pod.Annotations = make(map[string]string)
	pod.Labels = make(map[string]string)

	logger.Info(ctx, "sidecar builder is executing")
	var primaryContainerName string
	switch task.TaskTypeVersion {
	case 0:
		// Handles pod tasks when they are defined as Sidecar tasks and marshal the podspec using k8s proto.
		sidecarJob := sidecarJob{}
		err := utils.UnmarshalStructToObj(task.GetCustom(), &sidecarJob)
		if err != nil {
			return errors.Errorf(errors.BadTaskSpecification, "invalid TaskSpecification [%v], Err: [%v]", task.GetCustom(), err.Error())
		}

		mergeMapInto(sidecarJob.Annotations, pod.Annotations)
		mergeMapInto(sidecarJob.Labels, pod.Labels)

		primaryContainerName = sidecarJob.PrimaryContainerName
	case 1:
		// Handles pod tasks that marshal the pod spec to the task custom.
		containerName, err := getPrimaryContainerNameFromConfig(task)
		if err != nil {
			return err
		}

		primaryContainerName = containerName
	default:
		// Handles pod tasks that marshal the pod spec to the k8s_pod task target.
		if task.GetK8SPod() == nil || task.GetK8SPod().Metadata != nil {
			mergeMapInto(task.GetK8SPod().Metadata.Annotations, pod.Annotations)
			mergeMapInto(task.GetK8SPod().Metadata.Labels, pod.Labels)
		}

		containerName, err := getPrimaryContainerNameFromConfig(task)
		if err != nil {
			return err
		}

		primaryContainerName = containerName
	}

	// validate pod and update resource requirements
	if err := validateAndFinalizePodSpec(ctx, taskCtx, primaryContainerName, &pod.Spec); err != nil {
		return err
	}

	pod.Annotations[PrimaryContainerKey] = primaryContainerName
	return nil
}

// This method handles templatizing primary container input args, env variables and adds a GPU toleration to the pod
// spec if necessary.
func validateAndFinalizePodSpec(ctx context.Context, taskCtx pluginsCore.TaskExecutionContext, primaryContainerName string, podSpec *v1.PodSpec) error {
	var hasPrimaryContainer bool
	var hasDefaultContainer bool
	var primaryContainer v1.Container
	var defaultContainer v1.Container

	logger.Infof(ctx, "primary container name %v", primaryContainerName)
	taskName, err := getPrimaryContainerNameFromTask(taskCtx)
	if err != nil {
		return err
	}
	logger.Infof(ctx, "task name %v", taskName)

	resReqs := make([]v1.ResourceRequirements, 0, len(podSpec.Containers))
	for index, container := range podSpec.Containers {
		var resourceMode = flytek8s.ResourceCustomizationModeEnsureExistingResourcesInRange
		logger.Infof(ctx, "checking container name %v", container.Name)
		if container.Name == primaryContainerName {
			hasPrimaryContainer = true
			resourceMode = flytek8s.ResourceCustomizationModeMergeExistingResources
			primaryContainer = container
		}
		if container.Name == DefaultContainerName {
			defaultContainer = container
		}

		templateParameters := template.Parameters{
			TaskExecMetadata: taskCtx.TaskExecutionMetadata(),
			Inputs:           taskCtx.InputReader(),
			OutputPath:       taskCtx.OutputWriter(),
			Task:             taskCtx.TaskReader(),
		}

		err := flytek8s.AddFlyteCustomizationsToContainer(ctx, templateParameters, resourceMode, &podSpec.Containers[index])
		if err != nil {
			return err
		}

		resReqs = append(resReqs, container.Resources)
	}
	// TODO all the edits here are super shoddy just for testing
	if hasPrimaryContainer && hasDefaultContainer {
		for index, container := range podSpec.Containers {
			if container.Name == primaryContainerName {
				err := mergo.Merge(primaryContainer, defaultContainer)
				if err != nil {
					return err
				}
				podSpec.Containers[index] = primaryContainer
			}
		}
	}

	if !hasPrimaryContainer {
		return errors.Errorf(errors.BadTaskSpecification, "invalid Sidecar task, primary container [%s] not defined", primaryContainerName)
	}

	flytek8s.UpdatePod(taskCtx.TaskExecutionMetadata(), resReqs, podSpec)
	return nil
}
