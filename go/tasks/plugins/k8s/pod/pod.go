package sidecar

import (
	"context"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/flyteorg/flyteplugins/go/tasks/errors"
	"github.com/flyteorg/flyteplugins/go/tasks/logs"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery"
	pluginsCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core/template"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/flytek8s"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/k8s"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/utils"

	k8sv1 "k8s.io/api/core/v1"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	containerTaskType   = "container"
	podTaskType         = "pod"
	primaryContainerKey = "primary_container_name"
	sidecarTaskType     = "sidecar"
)

type podSpecResource struct {
	podSpec              k8sv1.PodSpec
	primaryContainerName string
	annotations          map[string]string
	labels               map[string]string
}

func newPodSpecResource() podSpecResource {
	return podSpecResource{
		annotations: make(map[string]string),
		labels:      make(map[string]string),
	}
}

// Why, you might wonder do we recreate the generated go struct generated from the plugins.SidecarJob proto? Because
// although we unmarshal the task custom json, the PodSpec itself is not generated from a  proto definition,
// but a proper go struct defined in k8s libraries. Therefore we only unmarshal the sidecar as a json, rather than jsonpb.
type sidecarJob struct {
	PodSpec              *k8sv1.PodSpec
	PrimaryContainerName string
	Annotations          map[string]string
	Labels               map[string]string
}

type plugin struct{}

func (plugin) BuildIdentityResource(_ context.Context, _ pluginsCore.TaskExecutionMetadata) (
	client.Object, error) {
	return flytek8s.BuildIdentityPod(), nil
}

func (plugin) BuildResource(ctx context.Context, taskCtx pluginsCore.TaskExecutionContext) (client.Object, error) {
	// read TaskTemplate
	task, err := taskCtx.TaskReader().Read(ctx)
	if err != nil {
		return nil, errors.Errorf(errors.BadTaskSpecification,
			"TaskSpecification cannot be read, Err: [%v]", err.Error())
	}

	// initialize podSpecResource
	var podSpecResource podSpecResource
	switch task.Type {
	case sidecarTaskType:
		switch task.TaskTypeVersion {
		case 0:
			podSpecResource, err = buildResourceSidecarV0(task)
			if err != nil {
				return nil, err
			}
		case 1:
			podSpecResource, err = buildResourceSidecarV1(task)
			if err != nil {
				return nil, err
			}
		default:
			podSpecResource, err = buildResourceSidecarV2(task)
			if err != nil {
				return nil, err
			}
		}

		// Set the restart policy to *not* inherit from the default so that a completed pod doesn't get caught in a
		// CrashLoopBackoff after the initial job completion.
		podSpecResource.podSpec.RestartPolicy = k8sv1.RestartPolicyNever

		err := finalizePodSpec(ctx, taskCtx, &podSpecResource.podSpec, podSpecResource.primaryContainerName)
		if err != nil {
			return nil, err
		}
	default:
		podSpecResource, err = buildResourceContainer(ctx, taskCtx)
		if err != nil {
			return nil, err
		}
	}

	// build pod with spec
	pod := flytek8s.BuildPodWithSpec(&podSpecResource.podSpec)
	pod.Annotations = podSpecResource.annotations
	pod.Annotations[primaryContainerKey] = podSpecResource.primaryContainerName
	pod.Labels = podSpecResource.labels
	pod.Spec.ServiceAccountName = flytek8s.GetServiceAccountNameFromTaskExecutionMetadata(taskCtx.TaskExecutionMetadata())

	// validate pod
	if podSpecResource.primaryContainerName != "*" {
		hasPrimaryContainer := false
		for _, container := range pod.Spec.Containers {
			if container.Name == podSpecResource.primaryContainerName {
				hasPrimaryContainer = true
			}
		}

		// TODO - change this error - not a sidecar task
		if !hasPrimaryContainer {
			return nil, errors.Errorf(errors.BadTaskSpecification,
				"invalid Sidecar task, primary container [%s] not defined", podSpecResource.primaryContainerName)

		}
	}

	return pod, nil
}

func (plugin) GetTaskPhase(ctx context.Context, pluginContext k8s.PluginContext, r client.Object) (pluginsCore.PhaseInfo, error) {
	pod := r.(*k8sv1.Pod)

	transitionOccurredAt := flytek8s.GetLastTransitionOccurredAt(pod).Time
	info := pluginsCore.TaskInfo{
		OccurredAt: &transitionOccurredAt,
	}
	if pod.Status.Phase != k8sv1.PodPending && pod.Status.Phase != k8sv1.PodUnknown {
		taskLogs, err := logs.GetLogsForContainerInPod(ctx, pod, 0, " (User)")
		if err != nil {
			return pluginsCore.PhaseInfoUndefined, err
		}
		info.Logs = taskLogs
	}
	switch pod.Status.Phase {
	case k8sv1.PodSucceeded:
		return flytek8s.DemystifySuccess(pod.Status, info)
	case k8sv1.PodFailed:
		code, message := flytek8s.ConvertPodFailureToError(pod.Status)
		return pluginsCore.PhaseInfoRetryableFailure(code, message, &info), nil
	case k8sv1.PodPending:
		return flytek8s.DemystifyPending(pod.Status)
	case k8sv1.PodReasonUnschedulable:
		return pluginsCore.PhaseInfoQueued(transitionOccurredAt, pluginsCore.DefaultPhaseVersion, "pod unschedulable"), nil
	case k8sv1.PodUnknown:
		return pluginsCore.PhaseInfoUndefined, nil
	}

	// Otherwise, assume the pod is running.
	primaryContainerName, ok := r.GetAnnotations()[primaryContainerKey]
	if !ok {
		return pluginsCore.PhaseInfoUndefined, errors.Errorf(errors.BadTaskSpecification,
			"missing primary container annotation for pod")
	}

	// TODO - document
	if primaryContainerName == "*" {
		if len(info.Logs) > 0 {
			return pluginsCore.PhaseInfoRunning(pluginsCore.DefaultPhaseVersion+1, &info), nil
		}
		return pluginsCore.PhaseInfoRunning(pluginsCore.DefaultPhaseVersion, &info), nil
	}

	// TODO - document
	primaryContainerPhase := flytek8s.DeterminePrimaryContainerPhase(primaryContainerName, pod.Status.ContainerStatuses, &info)
	if primaryContainerPhase.Phase() == pluginsCore.PhaseRunning && len(info.Logs) > 0 {
		return pluginsCore.PhaseInfoRunning(pluginsCore.DefaultPhaseVersion+1, primaryContainerPhase.Info()), nil
	}
	return primaryContainerPhase, nil
}

func (plugin) GetProperties() k8s.PluginProperties {
	return k8s.PluginProperties{}
}

func buildResourceContainer(ctx context.Context, taskCtx pluginsCore.TaskExecutionContext) (podSpecResource, error) {
	podSpec, err := flytek8s.ToK8sPodSpec(ctx, taskCtx)
	if err != nil {
		return podSpecResource{}, err
	}

	res := newPodSpecResource()
	res.podSpec = *podSpec
	// TODO - flytek8s.ToK8sPodSpec fails if a container is not found, therefore we are ...
	//res.primaryContainerName = podSpec.Containers[0].Name
	res.primaryContainerName = "*"

	return res, nil
}

// Handles pod tasks when they are defined as Sidecar tasks and marshal the podspec using k8s proto.
func buildResourceSidecarV0(task *core.TaskTemplate) (podSpecResource, error) {
	res := newPodSpecResource()
	sidecarJob := sidecarJob{}
	err := utils.UnmarshalStructToObj(task.GetCustom(), &sidecarJob)
	if err != nil {
		return podSpecResource{}, errors.Errorf(errors.BadTaskSpecification,
			"invalid TaskSpecification [%v], Err: [%v]", task.GetCustom(), err.Error())
	}
	if sidecarJob.PodSpec == nil {
		return podSpecResource{}, errors.Errorf(errors.BadTaskSpecification,
			"invalid TaskSpecification, nil PodSpec [%v]", task.GetCustom())
	}
	res.podSpec = *sidecarJob.PodSpec
	res.primaryContainerName = sidecarJob.PrimaryContainerName
	if sidecarJob.Annotations != nil {
		res.annotations = sidecarJob.Annotations
	}

	if sidecarJob.Labels != nil {
		res.labels = sidecarJob.Labels
	}

	return res, nil
}

// Handles pod tasks that marshal the pod spec to the task custom.
func buildResourceSidecarV1(task *core.TaskTemplate) (podSpecResource, error) {
	res := newPodSpecResource()
	err := utils.UnmarshalStructToObj(task.GetCustom(), &res.podSpec)
	if err != nil {
		return podSpecResource{}, errors.Errorf(errors.BadTaskSpecification,
			"Unable to unmarshal task custom [%v], Err: [%v]", task.GetCustom(), err.Error())
	}
	res.primaryContainerName, err = getPrimaryContainerNameFromConfig(task)
	if err != nil {
		return podSpecResource{}, err
	}
	return res, nil
}

// Handles pod tasks that marshal the pod spec to the k8s_pod task target.
func buildResourceSidecarV2(task *core.TaskTemplate) (podSpecResource, error) {
	res := newPodSpecResource()
	if task.GetK8SPod() == nil || task.GetK8SPod().PodSpec == nil {
		return podSpecResource{}, errors.Errorf(errors.BadTaskSpecification,
			"Pod tasks with task type version > 1 should specify their target as a K8sPod with a defined pod spec")
	}
	err := utils.UnmarshalStructToObj(task.GetK8SPod().PodSpec, &res.podSpec)
	if err != nil {
		return podSpecResource{}, errors.Errorf(errors.BadTaskSpecification,
			"Unable to unmarshal task custom [%v], Err: [%v]", task.GetCustom(), err.Error())
	}
	res.primaryContainerName, err = getPrimaryContainerNameFromConfig(task)
	if err != nil {
		return podSpecResource{}, err
	}
	if task.GetK8SPod().Metadata != nil {
		if task.GetK8SPod().Metadata.Annotations != nil {
			res.annotations = task.GetK8SPod().Metadata.Annotations
		}
		if task.GetK8SPod().Metadata.Labels != nil {
			res.labels = task.GetK8SPod().Metadata.Labels
		}
	}
	return res, nil
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

// This method handles templatizing primary container input args, env variables and adds a GPU toleration to the pod
// spec if necessary.
func finalizePodSpec(ctx context.Context, taskCtx pluginsCore.TaskExecutionContext, podSpec *k8sv1.PodSpec, primaryContainerName string) error {
	resReqs := make([]k8sv1.ResourceRequirements, 0, len(podSpec.Containers))
	for index, container := range podSpec.Containers {
		var resourceMode = flytek8s.ResourceCustomizationModeEnsureExistingResourcesInRange
		if container.Name == primaryContainerName {
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
	flytek8s.UpdatePod(taskCtx.TaskExecutionMetadata(), resReqs, podSpec)
	return nil
}

func init() {
	pluginmachinery.PluginRegistry().RegisterK8sPlugin(
		k8s.PluginEntry{
			ID:                  podTaskType,
			RegisteredTaskTypes: []pluginsCore.TaskType{containerTaskType, sidecarTaskType},
			ResourceToWatch:     &k8sv1.Pod{},
			Plugin:              plugin{},
			IsDefault:           true,
			DefaultForTaskTypes: []pluginsCore.TaskType{containerTaskType, sidecarTaskType},
		})
}
