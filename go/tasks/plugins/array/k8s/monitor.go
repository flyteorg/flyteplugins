package k8s

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lyft/flyteplugins/go/tasks/errors"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/utils"

	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/storage"

	"github.com/lyft/flyteplugins/go/tasks/plugins/array"

	arrayCore "github.com/lyft/flyteplugins/go/tasks/plugins/array/core"

	"github.com/lyft/flytestdlib/bitarray"

	"github.com/lyft/flyteplugins/go/tasks/plugins/array/arraystatus"
	"github.com/lyft/flyteplugins/go/tasks/plugins/array/errorcollector"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sTypes "k8s.io/apimachinery/pkg/types"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/flytek8s"

	idlCore "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	errors2 "github.com/lyft/flytestdlib/errors"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/lyft/flyteplugins/go/tasks/logs"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
)

const (
	ErrCheckPodStatus errors2.ErrorCode = "CHECK_POD_FAILED"
)

func LaunchAndCheckSubTasksState(ctx context.Context, tCtx core.TaskExecutionContext, kubeClient core.KubeClient,
	config *Config, dataStore *storage.DataStore, outputPrefix, baseOutputDataSandbox storage.DataReference, currentState *arrayCore.State) (
	newState *arrayCore.State, logLinks []*idlCore.TaskLog, err error) {

	logLinks = make([]*idlCore.TaskLog, 0, 4)
	newState = currentState

	if int64(currentState.GetExecutionArraySize()) > config.MaxArrayJobSize {
		ee := fmt.Errorf("array size > max allowed. Requested [%v]. Allowed [%v]", currentState.GetExecutionArraySize(), config.MaxArrayJobSize)
		logger.Info(ctx, ee)
		currentState = currentState.SetPhase(arrayCore.PhasePermanentFailure, 0).SetReason(ee.Error())
		return currentState, logLinks, nil
	}

	msg := errorcollector.NewErrorMessageCollector()

	newArrayStatus := arraystatus.ArrayStatus{
		Summary:  arraystatus.ArraySummary{},
		Detailed: arrayCore.NewPhasesCompactArray(uint(currentState.GetExecutionArraySize())),
	}

	if len(currentState.GetArrayStatus().Detailed.GetItems()) == 0 {
		currentState.ArrayStatus = newArrayStatus
	}

	podTemplate, _, err := FlyteArrayJobToK8sPodTemplate(ctx, tCtx)
	if err != nil {
		return newState, logLinks, errors2.Wrapf(ErrBuildPodTemplate, err, "Failed to convert task template to a pod template for task")
	}

	var args []string
	if len(podTemplate.Spec.Containers) > 0 {
		args = append(podTemplate.Spec.Containers[0].Command, podTemplate.Spec.Containers[0].Args...)
		podTemplate.Spec.Containers[0].Command = []string{}
	}

	for childIdx, existingPhaseIdx := range currentState.GetArrayStatus().Detailed.GetItems() {
		existingPhase := core.Phases[existingPhaseIdx]
		indexStr := strconv.Itoa(childIdx)
		podName := formatSubTaskName(ctx, tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), indexStr)
		if existingPhase.IsTerminal() {
			// If we get here it means we have already "processed" this terminal phase since we will only persist
			// the phase after all processing is done (e.g. check outputs/errors file, record events... etc.).

			// Deallocate Resource
			err = tCtx.ResourceManager().ReleaseResource(ctx, core.ResourceNamespace(ResourcesPrimaryLabel), podName)
			if err != nil {
				logger.Errorf(ctx, "Error releasing allocation token [%s] in Finalize [%s]", podName, err)
				return newState, logLinks, errors2.Wrapf(ErrCheckPodStatus, err, "Error releasing allocation token.")
			}
			newArrayStatus.Summary.Inc(existingPhase)
			newArrayStatus.Detailed.SetItem(childIdx, bitarray.Item(existingPhase))

			// TODO: collect log links before doing this
			continue
		}

		pod := podTemplate.DeepCopy()
		pod.Name = podName
		pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env, corev1.EnvVar{
			Name:  FlyteK8sArrayIndexVarName,
			Value: indexStr,
		})

		pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env, arrayJobEnvVars...)

		pod.Spec.Containers[0].Args, err = utils.ReplaceTemplateCommandArgs(ctx, args, arrayJobInputReader{tCtx.InputReader()}, tCtx.OutputWriter())
		if err != nil {
			return newState, logLinks, errors2.Wrapf(ErrReplaceCmdTemplate, err, "Failed to replace cmd args")
		}

		pod = ApplyPodPolicies(ctx, config, pod)

		resourceNamespace := core.ResourceNamespace(pod.Namespace)
		resourceConstrainSpec := createResourceConstraintsSpec(ctx, tCtx, config, core.ResourceNamespace(ResourcesPrimaryLabel))
		allocationStatus, err := tCtx.ResourceManager().AllocateResource(ctx, resourceNamespace, podName, resourceConstrainSpec)
		if err != nil {
			logger.Errorf(ctx, "Resource manager failed for TaskExecId [%s] token [%s]. error %s",
				tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetID(), podName, err)
			return newState, logLinks, errors2.Wrapf(errors.ResourceManagerFailure, err, "Error requesting allocation token %s", podName)
		}
		if allocationStatus != core.AllocationStatusGranted {
			newArrayStatus.Detailed.SetItem(childIdx, bitarray.Item(core.PhaseWaitingForResources))
			newArrayStatus.Summary.Inc(core.PhaseWaitingForResources)
			continue
		}
		logger.Infof(ctx, "Allocation result for [%s] is [%s]", podName, allocationStatus)

		err = kubeClient.GetClient().Create(ctx, pod)
		if err != nil && !k8serrors.IsAlreadyExists(err) {
			if k8serrors.IsForbidden(err) {
				if strings.Contains(err.Error(), "exceeded quota") {
					// TODO: Quota errors are retried forever, it would be good to have support for backoff strategy.
					logger.Infof(ctx, "Failed to launch job, resource quota exceeded. Err: %v", err)
					newState = newState.SetPhase(arrayCore.PhaseWaitingForResources, 0).SetReason("Not enough resources to launch job.")
				} else {
					newState = newState.SetPhase(arrayCore.PhaseRetryableFailure, 0).SetReason("Failed to launch job.")
				}

				newState = newState.SetReason(err.Error())
				return newState, logLinks, nil
			}

			return newState, logLinks, errors2.Wrapf(ErrSubmitJob, err, "Failed to submit job")
		}

		phaseInfo, err := CheckPodStatus(ctx, kubeClient,
			k8sTypes.NamespacedName{
				Name:      podName,
				Namespace: tCtx.TaskExecutionMetadata().GetNamespace(),
			})
		if err != nil {
			return currentState, logLinks, errors2.Wrapf(ErrCheckPodStatus, err, "Failed to check pod status")
		}

		if phaseInfo.Info() != nil {
			logLinks = append(logLinks, phaseInfo.Info().Logs...)
		}

		if phaseInfo.Err() != nil {
			msg.Collect(childIdx, phaseInfo.Err().String())
		}

		actualPhase := phaseInfo.Phase()
		if phaseInfo.Phase().IsSuccess() {
			originalIdx := arrayCore.CalculateOriginalIndex(childIdx, currentState.GetIndexesToCache())
			actualPhase, err = array.CheckTaskOutput(ctx, dataStore, outputPrefix, baseOutputDataSandbox, childIdx, originalIdx)
			if err != nil {
				return nil, nil, err
			}
		}

		newArrayStatus.Detailed.SetItem(childIdx, bitarray.Item(actualPhase))
		newArrayStatus.Summary.Inc(actualPhase)

	}

	newState = newState.SetArrayStatus(newArrayStatus)

	// Check that the taskTemplate is valid
	taskTemplate, err := tCtx.TaskReader().Read(ctx)
	if err != nil {
		return currentState, logLinks, err
	} else if taskTemplate == nil {
		return currentState, logLinks, fmt.Errorf("required value not set, taskTemplate is nil")
	}

	phase := arrayCore.SummaryToPhase(ctx, currentState.GetOriginalMinSuccesses()-currentState.GetOriginalArraySize()+int64(currentState.GetExecutionArraySize()), newArrayStatus.Summary)
	if phase == arrayCore.PhaseWriteToDiscoveryThenFail {
		errorMsg := msg.Summary(GetConfig().MaxErrorStringLength)
		newState = newState.SetReason(errorMsg)
	}

	if phase == arrayCore.PhaseCheckingSubTaskExecutions {
		newPhaseVersion := uint32(0)

		// For now, the only changes to PhaseVersion and PreviousSummary occur for running array jobs.
		for phase, count := range newState.GetArrayStatus().Summary {
			newPhaseVersion += uint32(phase) * uint32(count)
		}

		newState = newState.SetPhase(phase, newPhaseVersion).SetReason("Task is still running.")
	} else {
		newState = newState.SetPhase(phase, core.DefaultPhaseVersion)
	}

	return newState, logLinks, nil
}

func CheckPodStatus(ctx context.Context, client core.KubeClient, name k8sTypes.NamespacedName) (
	info core.PhaseInfo, err error) {

	pod := &v1.Pod{
		TypeMeta: metaV1.TypeMeta{
			Kind:       PodKind,
			APIVersion: v1.SchemeGroupVersion.String(),
		},
	}

	err = client.GetClient().Get(ctx, name, pod)
	now := time.Now()

	if err != nil {
		if k8serrors.IsNotFound(err) {
			// If the object disappeared at this point, it means it was manually removed or garbage collected.
			// Mark it as a failure.
			return core.PhaseInfoFailed(core.PhaseRetryableFailure, &idlCore.ExecutionError{
				Code:    string(k8serrors.ReasonForError(err)),
				Message: err.Error(),
				Kind:    idlCore.ExecutionError_SYSTEM,
			}, &core.TaskInfo{
				OccurredAt: &now,
			}), nil
		}

		return info, err
	}

	t := flytek8s.GetLastTransitionOccurredAt(pod).Time
	taskInfo := core.TaskInfo{
		OccurredAt: &t,
	}

	if pod.Status.Phase != v1.PodPending && pod.Status.Phase != v1.PodUnknown {
		taskLogs, err := logs.GetLogsForContainerInPod(ctx, pod, 0, " (User)")
		if err != nil {
			return core.PhaseInfoUndefined, err
		}
		taskInfo.Logs = taskLogs
	}
	switch pod.Status.Phase {
	case v1.PodSucceeded:
		return flytek8s.DemystifySuccess(pod.Status, taskInfo)
	case v1.PodFailed:
		code, message := flytek8s.ConvertPodFailureToError(pod.Status)
		return core.PhaseInfoRetryableFailure(code, message, &taskInfo), nil
	case v1.PodPending:
		return flytek8s.DemystifyPending(pod.Status)
	case v1.PodUnknown:
		return core.PhaseInfoUndefined, nil
	}
	if len(taskInfo.Logs) > 0 {
		return core.PhaseInfoRunning(core.DefaultPhaseVersion+1, &taskInfo), nil
	}
	return core.PhaseInfoRunning(core.DefaultPhaseVersion, &taskInfo), nil

}

func createResourceConstraintsSpec(ctx context.Context, _ core.TaskExecutionContext, config *Config, primaryLabel core.ResourceNamespace) core.ResourceConstraintsSpec {
	constraintsSpec := core.ResourceConstraintsSpec{
		ProjectScopeResourceConstraint:   nil,
		NamespaceScopeResourceConstraint: nil,
	}

	if config.ResourcesConfig == (ResourceConfig{}) {
		logger.Infof(ctx, "No Resource config is found. Returning an empty resource constraints spec")
		return constraintsSpec
	}

	constraintsSpec.ProjectScopeResourceConstraint = &core.ResourceConstraint{Value: int64(1)}
	constraintsSpec.NamespaceScopeResourceConstraint = &core.ResourceConstraint{Value: int64(1)}

	logger.Infof(ctx, "Created a resource constraints spec: [%v]", constraintsSpec)
	return constraintsSpec
}
