package pod

import (
	"context"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/flyteorg/flyteplugins/go/tasks/errors"
	"github.com/flyteorg/flyteplugins/go/tasks/logs"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery"
	pluginsCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/flytek8s"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/k8s"

	v1 "k8s.io/api/core/v1"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	containerTaskType   = "container"
	podTaskType         = "pod"
	primaryContainerKey = "primary_container_name"
	sidecarTaskType     = "sidecar"
)

type PodBuilder interface {
	BuildPodSpec(ctx context.Context, task *core.TaskTemplate, taskCtx pluginsCore.TaskExecutionContext) (*v1.PodSpec, error)
	UpdatePodMetadata(ctx context.Context, pod *v1.Pod, task *core.TaskTemplate, taskCtx pluginsCore.TaskExecutionContext) error
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

	// initialize PodBuilder
	var podBuilder PodBuilder
	switch task.Type {
	case sidecarTaskType:
		podBuilder = sidecarPodBuilder{}
	default:
		podBuilder = containerPodBuilder{}
	}

	// build pod
    podSpec, err := podBuilder.BuildPodSpec(ctx, task, taskCtx)
	if err != nil {
		return nil, err
	}

	podSpec.ServiceAccountName = flytek8s.GetServiceAccountNameFromTaskExecutionMetadata(taskCtx.TaskExecutionMetadata())
    pod := flytek8s.BuildPodWithSpec(podSpec)

	// update pod metadata
    if err = podBuilder.UpdatePodMetadata(ctx, pod, task, taskCtx); err != nil {
		return nil, err
    }

	return pod, nil
}

func (plugin) GetTaskPhase(ctx context.Context, pluginContext k8s.PluginContext, r client.Object) (pluginsCore.PhaseInfo, error) {
	pod := r.(*v1.Pod)

	transitionOccurredAt := flytek8s.GetLastTransitionOccurredAt(pod).Time
	info := pluginsCore.TaskInfo{
		OccurredAt: &transitionOccurredAt,
	}

	if pod.Status.Phase != v1.PodPending && pod.Status.Phase != v1.PodUnknown {
		taskLogs, err := logs.GetLogsForContainerInPod(ctx, pod, 0, " (User)")
		if err != nil {
			return pluginsCore.PhaseInfoUndefined, err
		}
		info.Logs = taskLogs
	}

	switch pod.Status.Phase {
	case v1.PodSucceeded:
		return flytek8s.DemystifySuccess(pod.Status, info)
	case v1.PodFailed:
		code, message := flytek8s.ConvertPodFailureToError(pod.Status)
		return pluginsCore.PhaseInfoRetryableFailure(code, message, &info), nil
	case v1.PodPending:
		return flytek8s.DemystifyPending(pod.Status)
	case v1.PodReasonUnschedulable:
		return pluginsCore.PhaseInfoQueued(transitionOccurredAt, pluginsCore.DefaultPhaseVersion, "pod unschedulable"), nil
	case v1.PodUnknown:
		return pluginsCore.PhaseInfoUndefined, nil
	}

	// TODO - document
	primaryContainerName, exists := r.GetAnnotations()[primaryContainerKey]
	if !exists {
		// TODO if the primary container does not exist then use the pod phase
		// i.e. all pods need to complete to mark this as succeeded
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

func init() {
	pluginmachinery.PluginRegistry().RegisterK8sPlugin(
		k8s.PluginEntry{
			ID:                  podTaskType,
			RegisteredTaskTypes: []pluginsCore.TaskType{containerTaskType, sidecarTaskType},
			ResourceToWatch:     &v1.Pod{},
			Plugin:              plugin{},
			IsDefault:           true,
			DefaultForTaskTypes: []pluginsCore.TaskType{containerTaskType, sidecarTaskType},
		})
}
