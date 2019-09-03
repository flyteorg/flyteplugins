package k8splugins

import (
	"context"
	"fmt"
	"github.com/lyft/flytestdlib/logger"

	pluginMachinery "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1"
	pluginsCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/k8s"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/utils"
	"github.com/lyft/flyteplugins/go/tasks/v1/errors"
	"github.com/lyft/flyteplugins/go/tasks/v1/flytek8s"

	k8sv1 "k8s.io/api/core/v1"
)

const (
	sidecarTaskType     = "sidecar"
	primaryContainerKey = "primary"
)

type sidecarResourceHandler struct{}

var allContainers = false

// This method handles templatizing primary container input args, env variables and adds a GPU toleration to the pod
// spec if necessary.
func validateAndFinalizePod(
	ctx context.Context, taskCtx pluginsCore.TaskExecutionContext, primaryContainerName string, podSpec *k8sv1.PodSpec) error {
	var hasPrimaryContainer bool

	finalizedContainers := make([]k8sv1.Container, len(podSpec.Containers))
	resReqs := make([]k8sv1.ResourceRequirements, 0, len(podSpec.Containers))
	for index, container := range podSpec.Containers {
		if container.Name == primaryContainerName {
			hasPrimaryContainer = true
		}
		err := flytek8s.AddFlyteModificationsForContainer(ctx, taskCtx, &container)
		if err != nil {
			return err
		}
		logger.Warnf(ctx, "appending resources [%+v]", container.Resources)
		resReqs = append(resReqs, container.Resources)
		finalizedContainers[index] = container
	}
	logger.Warnf(ctx, "resReqs [%+v]", resReqs)
	logger.Warnf(ctx, "podSpec.Containers [%+v]", podSpec.Containers)
	if !hasPrimaryContainer {
		return errors.Errorf(errors.BadTaskSpecification,
			"invalid Sidecar task, primary container [%s] not defined", primaryContainerName)

	}
	flytek8s.AddFlyteModificationsForPodSpec(taskCtx, finalizedContainers, resReqs, podSpec)
	return nil
}

func (sidecarResourceHandler) BuildResource(ctx context.Context, taskCtx pluginsCore.TaskExecutionContext) (k8s.Resource, error) {
	sidecarJob := plugins.SidecarJob{}
	task, err := taskCtx.TaskReader().Read(ctx)
	if err != nil {
		return nil, errors.Errorf(errors.BadTaskSpecification,
			"TaskSpecification cannot be read, Err: [%v]", err.Error())
	}
	err = utils.UnmarshalStruct(task.GetCustom(), &sidecarJob)
	if err != nil {
		return nil, errors.Errorf(errors.BadTaskSpecification,
			"invalid TaskSpecification [%v], Err: [%v]", task.GetCustom(), err.Error())
	}

	podSpec := sidecarJob.PodSpec
	err = validateAndFinalizePod(ctx, taskCtx, sidecarJob.PrimaryContainerName, podSpec)
	if err != nil {
		return nil, err
	}

	pod := flytek8s.BuildPodWithSpec(podSpec)
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string, 1)
	}

	pod.Annotations[primaryContainerKey] = sidecarJob.PrimaryContainerName

	return pod, nil
}

func (sidecarResourceHandler) BuildIdentityResource(_ context.Context, _ pluginsCore.TaskExecutionMetadata) (
	k8s.Resource, error) {
	return flytek8s.BuildIdentityPod(), nil
}

func determinePrimaryContainerPhase(primaryContainerName string, statuses []k8sv1.ContainerStatus, info *pluginsCore.TaskInfo) pluginsCore.PhaseInfo {
	for _, s := range statuses {
		if s.Name == primaryContainerName {
			if s.State.Waiting != nil || s.State.Running != nil {
				return pluginsCore.PhaseInfoRunning(pluginsCore.DefaultPhaseVersion, info)
			}

			if s.State.Terminated != nil {
				if s.State.Terminated.ExitCode != 0 {
					return pluginsCore.PhaseInfoRetryableFailure(
						s.State.Terminated.Reason, s.State.Terminated.Message, info)
				}
				return pluginsCore.PhaseInfoSuccess(info)
			}
		}
	}

	// If for some reason we can't find the primary container, always just return a permanent failure
	return pluginsCore.PhaseInfoFailure("PrimaryContainerMissing",
		fmt.Sprintf("Primary container [%s] not found in pod's container statuses", primaryContainerName), info)
}

func (sidecarResourceHandler) GetTaskPhase(ctx context.Context, pluginContext k8s.PluginContext, r k8s.Resource) (
	pluginsCore.PhaseInfo, error) {
	pod := r.(*k8sv1.Pod)
	phaseInfo, err := flytek8s.GetTaskPhaseFromPod(ctx, pod, flytek8s.AllContainers)
	if err != nil || phaseInfo.Phase() != pluginsCore.PhaseRunning {
		return phaseInfo, err
	}

	// If we made it here, the pod is running so we check the primary container status to determine the overall task status.
	primaryContainerName, ok := r.GetAnnotations()[primaryContainerKey]
	if !ok {
		return pluginsCore.PhaseInfoUndefined, errors.Errorf(errors.BadTaskSpecification,
			"missing primary container annotation for pod")
	}
	primaryContainerPhase := determinePrimaryContainerPhase(primaryContainerName, pod.Status.ContainerStatuses, phaseInfo.Info())
	if primaryContainerPhase.Phase() == pluginsCore.PhaseRunning && phaseInfo.Info() != nil && len(phaseInfo.Info().Logs) > 0 {
		return pluginsCore.PhaseInfoRunning(pluginsCore.DefaultPhaseVersion+1, primaryContainerPhase.Info()), nil
	}
	return primaryContainerPhase, nil
}

func init() {
	pluginMachinery.PluginRegistry().RegisterK8sPlugin(
		k8s.PluginEntry{
			ID:	sidecarTaskType,
			RegisteredTaskTypes: []pluginsCore.TaskType{sidecarTaskType},
			ResourceToWatch: &k8sv1.Pod{},
			Plugin: sidecarResourceHandler{},
			IsDefault: false,
		})
}
