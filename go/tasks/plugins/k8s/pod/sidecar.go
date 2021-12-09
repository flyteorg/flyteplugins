package pod

import (
	"context"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/flyteorg/flyteplugins/go/tasks/errors"
	pluginsCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core/template"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/flytek8s"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/utils"

	v1 "k8s.io/api/core/v1"
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

func (sidecarPodBuilder) buildPodSpec(ctx context.Context, task *core.TaskTemplate, taskCtx pluginsCore.TaskExecutionContext) (*v1.PodSpec, error) {
	var podSpec *v1.PodSpec
	switch task.TaskTypeVersion {
	case 0:
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

		podSpec = sidecarJob.PodSpec
	case 1:
		err := utils.UnmarshalStructToObj(task.GetCustom(), podSpec)
		if err != nil {
			return nil, errors.Errorf(errors.BadTaskSpecification,
				"Unable to unmarshal task custom [%v], Err: [%v]", task.GetCustom(), err.Error())
		}
	default:
		if task.GetK8SPod() == nil || task.GetK8SPod().PodSpec == nil {
			return nil, errors.Errorf(errors.BadTaskSpecification,
				"Pod tasks with task type version > 1 should specify their target as a K8sPod with a defined pod spec")
		}

		err := utils.UnmarshalStructToObj(task.GetK8SPod().PodSpec, podSpec)
		if err != nil {
			return nil, errors.Errorf(errors.BadTaskSpecification,
				"Unable to unmarshal task custom [%v], Err: [%v]", task.GetCustom(), err.Error())
		}
	}

	// Set the restart policy to *not* inherit from the default so that a completed pod doesn't get caught in a
	// CrashLoopBackoff after the initial job completion.
	podSpec.RestartPolicy = v1.RestartPolicyNever

	return podSpec, nil
}

func getPrimaryContainerNameFromConfig(task *core.TaskTemplate) (string, error) {
	if len(task.GetConfig()) == 0 {
		return "", errors.Errorf(errors.BadTaskSpecification, 
			"invalid TaskSpecification, config needs to be non-empty and include missing [%s] key", primaryContainerKey)
	}

	primaryContainerName, ok := task.GetConfig()[primaryContainerKey]
	if !ok {
		return "", errors.Errorf(errors.BadTaskSpecification, 
			"invalid TaskSpecification, config missing [%s] key in [%v]", primaryContainerKey, task.GetConfig())
	}

	return primaryContainerName, nil
}

func (sidecarPodBuilder) updatePodMetadata(ctx context.Context, pod *v1.Pod, task *core.TaskTemplate, taskCtx pluginsCore.TaskExecutionContext) error {
	var primaryContainerName string
	switch task.TaskTypeVersion {
	case 0:
		sidecarJob := sidecarJob{}
		err := utils.UnmarshalStructToObj(task.GetCustom(), &sidecarJob)
		if err != nil {
			return errors.Errorf(errors.BadTaskSpecification, "invalid TaskSpecification [%v], Err: [%v]", task.GetCustom(), err.Error())
		}

		if sidecarJob.Annotations != nil {
			pod.Annotations = sidecarJob.Annotations
		}

		if sidecarJob.Labels != nil {
			pod.Labels = sidecarJob.Labels
		}

		primaryContainerName = sidecarJob.PrimaryContainerName
	case 1:
		containerName, err := getPrimaryContainerNameFromConfig(task)
		if err != nil {
			return err
		}

		primaryContainerName = containerName
	default:
		if task.GetK8SPod() == nil || task.GetK8SPod().Metadata != nil {
			if task.GetK8SPod().Metadata.Annotations != nil {
				pod.Annotations = task.GetK8SPod().Metadata.Annotations
			}

			if task.GetK8SPod().Metadata.Labels != nil {
				pod.Labels = task.GetK8SPod().Metadata.Labels
			}
		}

		containerName, err := getPrimaryContainerNameFromConfig(task)
		if err != nil {
			return err
		}

		primaryContainerName = containerName
	}

	pod.Annotations[primaryContainerKey] = primaryContainerName

	// validate pod
	if err := validateAndFinalizePodSpec(ctx, taskCtx, primaryContainerKey, &pod.Spec); err != nil {
		return err
	}

	return nil
}

// This method handles templatizing primary container input args, env variables and adds a GPU toleration to the pod
// spec if necessary.
func validateAndFinalizePodSpec(ctx context.Context, taskCtx pluginsCore.TaskExecutionContext, primaryContainerName string, podSpec *v1.PodSpec) error {
	var hasPrimaryContainer bool

	resReqs := make([]v1.ResourceRequirements, 0, len(podSpec.Containers))
	for index, container := range podSpec.Containers {
		var resourceMode = flytek8s.ResourceCustomizationModeEnsureExistingResourcesInRange
		if container.Name == primaryContainerName {
			hasPrimaryContainer = true
			resourceMode = flytek8s.ResourceCustomizationModeMergeExistingResources
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

	// TODO - update message
	if !hasPrimaryContainer {
		return errors.Errorf(errors.BadTaskSpecification, "invalid Sidecar task, primary container [%s] not defined", primaryContainerName)
	}

	flytek8s.UpdatePod(taskCtx.TaskExecutionMetadata(), resReqs, podSpec)
	return nil
}
