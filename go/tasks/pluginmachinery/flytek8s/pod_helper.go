package flytek8s

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/lyft/flytestdlib/logger"
	v1 "k8s.io/api/core/v1"
	v12 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pluginsCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/flytek8s/config"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
)

const PodKind = "pod"
const OOMKilled = "OOMKilled"
const Interrupted = "Interrupted"
const SIGKILL = 137

func ToK8sPodSpec(ctx context.Context, taskExecutionMetadata pluginsCore.TaskExecutionMetadata, taskReader pluginsCore.TaskReader,
	inputs io.InputReader, outputPaths io.OutputFilePaths) (*v1.PodSpec, error) {
	task, err := taskReader.Read(ctx)
	if err != nil {
		logger.Warnf(ctx, "failed to read task information when trying to construct Pod, err: %s", err.Error())
		return nil, err
	}
	c, err := ToK8sContainer(ctx, taskExecutionMetadata, task.GetContainer(), inputs, outputPaths)
	if err != nil {
		return nil, err
	}

	containers := []v1.Container{
		*c,
	}
	if taskExecutionMetadata.IsInterruptible() && len(config.GetK8sPluginConfig().InterruptibleNodeSelector) > 0 {
		return &v1.PodSpec{
			// We could specify Scheduler, Affinity, nodename etc
			RestartPolicy:      v1.RestartPolicyNever,
			Containers:         containers,
			Tolerations:        GetPodTolerations(taskExecutionMetadata.IsInterruptible(), c.Resources),
			ServiceAccountName: taskExecutionMetadata.GetK8sServiceAccount(),
			NodeSelector:       config.GetK8sPluginConfig().InterruptibleNodeSelector,
			SchedulerName:      config.GetK8sPluginConfig().SchedulerName,
		}, nil
	}
	return &v1.PodSpec{
		// We could specify Scheduler, Affinity, nodename etc
		RestartPolicy:      v1.RestartPolicyNever,
		Containers:         containers,
		Tolerations:        GetPodTolerations(taskExecutionMetadata.IsInterruptible(), c.Resources),
		ServiceAccountName: taskExecutionMetadata.GetK8sServiceAccount(),
		SchedulerName:      config.GetK8sPluginConfig().SchedulerName,
	}, nil

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
							finalReason := fmt.Sprintf("%s|%s", c.Reason, reason)
							finalMessage := fmt.Sprintf("%s|%s", c.Message, containerStatus.State.Waiting.Message)
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
								return pluginsCore.PhaseInfoInitializing(c.LastTransitionTime.Time, pluginsCore.DefaultPhaseVersion, fmt.Sprintf("[%s]: %s", finalReason, finalMessage),  &pluginsCore.TaskInfo{OccurredAt: &c.LastTransitionTime.Time}), nil

							case "CreateContainerError":
								// This happens if for instance the command to the container is incorrect, ie doesn't run
								t := c.LastTransitionTime.Time
								return pluginsCore.PhaseInfoFailure(finalReason, finalMessage, &pluginsCore.TaskInfo{
									OccurredAt: &t,
								}), nil

							case "ImagePullBackOff":
								t := c.LastTransitionTime.Time
								return pluginsCore.PhaseInfoRetryableFailure(finalReason, finalMessage, &pluginsCore.TaskInfo{
									OccurredAt: &t,
								}), nil
							default:
								// Since we are not checking for all error states, we may end up perpetually
								// in the queued state returned at the bottom of this function, until the Pod is reaped
								// by K8s and we get elusive 'pod not found' errors
								// So be default if the container is not waiting with the PodInitializing/ContainerCreating
								// reasons, then we will assume a failure reason, and fail instantly
								t := c.LastTransitionTime.Time
								return pluginsCore.PhaseInfoSystemRetryableFailure(finalReason, finalMessage, &pluginsCore.TaskInfo{
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

func DemystifySuccess(status v1.PodStatus, info pluginsCore.TaskInfo) (pluginsCore.PhaseInfo, error) {
	for _, status := range append(
		append(status.InitContainerStatuses, status.ContainerStatuses...), status.EphemeralContainerStatuses...) {
		if status.State.Terminated != nil && strings.Contains(status.State.Terminated.Reason, OOMKilled) {
			return pluginsCore.PhaseInfoRetryableFailure("OOMKilled",
				"Pod reported success despite being OOMKilled", &info), nil
		}
	}
	return pluginsCore.PhaseInfoSuccess(&info), nil
}

func ConvertPodFailureToError(status v1.PodStatus) (code, message string) {
	code = "UnknownError"
	message = "Container/Pod failed. No message received from kubernetes."
	if len(status.Reason) > 0 {
		code = status.Reason
	}

	if len(status.Message) > 0 {
		message = status.Message
	}

	for _, c := range append(
		append(status.InitContainerStatuses, status.ContainerStatuses...), status.EphemeralContainerStatuses...) {
		var containerState v1.ContainerState
		if c.LastTerminationState.Terminated != nil {
			containerState = c.LastTerminationState
		} else if c.State.Terminated != nil {
			containerState = c.State
		}
		if containerState.Terminated != nil {
			if strings.Contains(c.State.Terminated.Reason, OOMKilled) {
				code = OOMKilled
			} else if containerState.Terminated.ExitCode == SIGKILL {
				// in some setups, node termination sends SIGKILL to all the containers running on that node. Capturing and
				// tagging that correctly.
				code = Interrupted
			}

			message += fmt.Sprintf("\r\nContainer [%v] terminated with exit code (%v). Reason [%v]. Message: [%v].",
				c.Name,
				containerState.Terminated.ExitCode,
				containerState.Terminated.Reason,
				containerState.Terminated.Message)
		}
	}
	return code, message
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
