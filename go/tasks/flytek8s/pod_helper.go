package flytek8s

import (
	"context"
	"fmt"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteplugins/go/tasks/logs"
	"github.com/lyft/flytestdlib/logger"
	"time"

	"k8s.io/api/core/v1"
	v12 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pluginsCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
)

const PodKind = "pod"
const OOMKilled = "OOMKilled"

// Add modifications required by the Flyte platform to an existing podSpec.
func AddFlyteModificationsForPodSpec(taskCtx pluginsCore.TaskExecutionContext, containers []v1.Container,
	resourceRequirements []v1.ResourceRequirements, podSpec *v1.PodSpec) {
	// We could specify Scheduler, Affinity, nodename etc
	podSpec.RestartPolicy = v1.RestartPolicyNever
	podSpec.Containers = containers
	finalizedResourceReqs := resourceRequirements
	if taskCtx.TaskExecutionMetadata().GetOverrides() != nil &&
		taskCtx.TaskExecutionMetadata().GetOverrides().GetResources() != nil {
		finalizedResourceReqs = append(finalizedResourceReqs,
			*taskCtx.TaskExecutionMetadata().GetOverrides().GetResources())
	}
	podSpec.Tolerations = GetTolerationsForResources(finalizedResourceReqs...)
	podSpec.ServiceAccountName = taskCtx.TaskExecutionMetadata().GetK8sServiceAccount()
}

func BuildPodWithSpec(podSpec *v1.PodSpec) *v1.Pod {
	pod := v1.Pod{
		TypeMeta: v12.TypeMeta{
			Kind:       PodKind,
			APIVersion: v1.SchemeGroupVersion.String(),
		},
		Spec: *podSpec,
	}

	return &pod
}

func BuildIdentityPod() *v1.Pod {
	return &v1.Pod{
		TypeMeta: v12.TypeMeta{
			Kind:       PodKind,
			APIVersion: v1.SchemeGroupVersion.String(),
		},
	}
}

// Important considerations.
// Pending Status in Pod could be for various reasons and sometimes could signal a problem
// Case I: Pending because the Image pull is failing and it is backing off
//         This could be transient. So we can actually rely on the failure reason.
//         The failure transitions from ErrImagePull -> ImagePullBackoff
// Case II: Not enough resources are available. This is tricky. It could be that the total number of
//          resources requested is beyond the capability of the system. for this we will rely on configuration
//          and hence input gates. We should not allow bad requests that request for large number of resource through.
//          In the case it makes through, we will fail after timeout
func DemystifyPending(status v1.PodStatus) (pluginsCore.PhaseInfo, error) {
	// Search over the difference conditions in the status object.  Note that the 'Pending' this function is
	// demystifying is the 'phase' of the pod status. This is different than the PodReady condition type also used below
	for _, c := range status.Conditions {
		switch c.Type {
		case v1.PodScheduled:
			if c.Status == v1.ConditionFalse {
				// Waiting to be scheduled. This usually refers to inability to acquire resources.
				return pluginsCore.PhaseInfoQueued(c.LastTransitionTime.Time, pluginsCore.DefaultPhaseVersion, fmt.Sprintf("%s:%s", c.Reason, c.Message)), nil
			}

		case v1.PodReasonUnschedulable:
			// We Ignore case in which we are unable to find resources on the cluster. This is because
			// - The resources may be not available at the moment, but may become available eventually
			//   The pod scheduler will keep on looking at this pod and trying to satisfy it.
			//
			//  Pod status looks like this:
			// 	message: '0/1 nodes are available: 1 Insufficient memory.'
			//  reason: Unschedulable
			// 	status: "False"
			// 	type: PodScheduled
			return pluginsCore.PhaseInfoQueued(c.LastTransitionTime.Time, pluginsCore.DefaultPhaseVersion, fmt.Sprintf("%s:%s", c.Reason, c.Message)), nil

		case v1.PodReady:
			if c.Status == v1.ConditionFalse {
				// This happens in the case the image is having some problems. In the following example, K8s is having
				// problems downloading an image. To ensure that, we will have to iterate over all the container statuses and
				// find if some container has imagepull failure
				// e.g.
				//     - lastProbeTime: null
				//      lastTransitionTime: 2018-12-18T00:57:30Z
				//      message: 'containers with unready status: [myapp-container]'
				//      reason: ContainersNotReady
				//      status: "False"
				//      type: Ready
				//
				// e.g. Container status
				//     - image: blah
				//      imageID: ""
				//      lastState: {}
				//      name: myapp-container
				//      ready: false
				//      restartCount: 0
				//      state:
				//        waiting:
				//          message: Back-off pulling image "blah"
				//          reason: ImagePullBackOff
				for _, containerStatus := range status.ContainerStatuses {
					if !containerStatus.Ready {
						if containerStatus.State.Waiting != nil {
							// There are a variety of reasons that can cause a pod to be in this waiting state.
							// Waiting state may be legitimate when the container is being downloaded, started or init containers are running
							reason := containerStatus.State.Waiting.Reason
							switch reason {
							case "ErrImagePull", "ContainerCreating", "PodInitializing":
								// But, there are only two "reasons" when a pod is successfully being created and hence it is in
								// waiting state
								// Refer to https://github.com/kubernetes/kubernetes/blob/master/pkg/kubelet/kubelet_pods.go
								// and look for the default waiting states
								// We also want to allow Image pulls to be retried, so ErrImagePull will be ignored
								// as it eventually enters into ImagePullBackOff
								// ErrImagePull -> Transitionary phase to ImagePullBackOff
								// ContainerCreating -> Image is being downloaded
								// PodInitializing -> Init containers are running
								return pluginsCore.PhaseInfoInitializing(c.LastTransitionTime.Time, pluginsCore.DefaultPhaseVersion, fmt.Sprintf("%s:%s", c.Reason, c.Message)), nil

							case "CreateContainerError":
								// This happens if for instance the command to the container is incorrect, ie doesn't run
								t := c.LastTransitionTime.Time
								return pluginsCore.PhaseInfoFailure(c.Reason, c.Message, &pluginsCore.TaskInfo{
									OccurredAt: &t,
								}), nil

							case "ImagePullBackOff":
								// TODO once we implement timeouts, this should probably be PhaseInitializing with version 1, so that user can see the reason
								fallthrough
							default:
								// Since we are not checking for all error states, we may end up perpetually
								// in the queued state returned at the bottom of this function, until the Pod is reaped
								// by K8s and we get elusive 'pod not found' errors
								// So be default if the container is not waiting with the PodInitializing/ContainerCreating
								// reasons, then we will assume a failure reason, and fail instantly
								t := c.LastTransitionTime.Time
								return pluginsCore.PhaseInfoRetryableFailure(c.Reason, c.Message, &pluginsCore.TaskInfo{
									OccurredAt: &t,
								}), nil
							}

						}
					}
				}
			}
		}
	}

	return pluginsCore.PhaseInfoQueued(time.Now(), pluginsCore.DefaultPhaseVersion, "Scheduling"), nil
}

// Cases have been observed where a pod will report success but a container has been killed due to OOM.
func DemystifySuccess(podStatus *v1.PodStatus, info pluginsCore.TaskInfo) (pluginsCore.PhaseInfo, error) {
	for _, container := range podStatus.ContainerStatuses {
		if container.State.Terminated != nil && container.State.Terminated.Reason == OOMKilled {
			return pluginsCore.PhaseInfoRetryableFailure(container.State.Terminated.Reason, container.State.Terminated.Message, &info), nil
		}
	}
	return pluginsCore.PhaseInfoSuccess(&info), nil
}

func ConvertPodFailureToError(status v1.PodStatus) (code, message string) {
	code = "UnknownError"
	message = "Container/Pod failed. No message received from kubernetes. Could be permissions?"
	if status.Reason != "" {
		code = status.Reason
	}
	if status.Message != "" {
		message = status.Message
	}
	return
}

func GetLastTransitionOccurredAt(pod *v1.Pod) v12.Time {
	var lastTransitionTime v12.Time
	containerStatuses := append(pod.Status.ContainerStatuses, pod.Status.InitContainerStatuses...)
	for _, containerStatus := range containerStatuses {
		if r := containerStatus.LastTerminationState.Running; r != nil {
			if r.StartedAt.Unix() > lastTransitionTime.Unix() {
				lastTransitionTime = r.StartedAt
			}
		} else if r := containerStatus.LastTerminationState.Terminated; r != nil {
			if r.FinishedAt.Unix() > lastTransitionTime.Unix() {
				lastTransitionTime = r.StartedAt
			}
		}
	}

	if lastTransitionTime.IsZero() {
		lastTransitionTime = v12.NewTime(time.Now())
	}

	return lastTransitionTime
}

type ContainerLogReportingMode int

const (
	UserOnly ContainerLogReportingMode = iota
	AllContainers
)

func getContainerLogs(ctx context.Context, pod *v1.Pod, reportingMode ContainerLogReportingMode) ([]*core.TaskLog, error) {
	if reportingMode == UserOnly {
		taskLogs, err := logs.GetLogsForContainerInPod(ctx, pod, 0, " (User)")
		if err != nil {
			return nil, err
		}
		return taskLogs, nil
	}
	if reportingMode == AllContainers {
		taskLogs := make([]*core.TaskLog, 0)
		for idx, container := range pod.Spec.Containers {
			containerLogs, err := logs.GetLogsForContainerInPod(ctx, pod, uint32(idx), fmt.Sprintf(" (%s)", container.Name))
			if err != nil {
				return nil, err
			}
			taskLogs = append(taskLogs, containerLogs...)
		}
		return taskLogs, nil
	}
	logger.Warnf(ctx, "Internal system error, unrecognized container log reporting mode [%+v]", reportingMode)
	return make([]*core.TaskLog, 0), nil
}

func GetTaskPhaseFromPod(ctx context.Context, pod *v1.Pod, reportingMode ContainerLogReportingMode) (pluginsCore.PhaseInfo, error) {
	t := GetLastTransitionOccurredAt(pod).Time
	info := pluginsCore.TaskInfo{
		OccurredAt: &t,
	}
	if pod.Status.Phase != v1.PodPending && pod.Status.Phase != v1.PodUnknown {
		taskLogs, err := getContainerLogs(ctx, pod, reportingMode)
		if err != nil {
			return pluginsCore.PhaseInfoUndefined, err
		}
		info.Logs = taskLogs
	}
	switch pod.Status.Phase {
	case v1.PodSucceeded:
		return DemystifySuccess(&pod.Status, info)
	case v1.PodFailed:
		code, message := ConvertPodFailureToError(pod.Status)
		return pluginsCore.PhaseInfoRetryableFailure(code, message, &info), nil
	case v1.PodReasonUnschedulable:
		fallthrough
	case v1.PodPending:
		return DemystifyPending(pod.Status)
	case v1.PodUnknown:
		return pluginsCore.PhaseInfoUndefined, nil
	}
	if len(info.Logs) > 0 {
		return pluginsCore.PhaseInfoRunning(pluginsCore.DefaultPhaseVersion+1, &info), nil
	}
	return pluginsCore.PhaseInfoRunning(pluginsCore.DefaultPhaseVersion, &info), nil
}