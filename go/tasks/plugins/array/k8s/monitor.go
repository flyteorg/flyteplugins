package k8s

import (
	"context"
	"fmt"
	"strconv"
	"time"

	array2 "github.com/lyft/flyteplugins/go/tasks/plugins/array"
	arraystatus2 "github.com/lyft/flyteplugins/go/tasks/plugins/array/arraystatus"
	errorcollector2 "github.com/lyft/flyteplugins/go/tasks/plugins/array/errorcollector"
	bitarray "github.com/lyft/flytestdlib/bitarray"

	v1 "k8s.io/api/core/v1"
	v12 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types2 "k8s.io/apimachinery/pkg/types"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/flytek8s"

	core2 "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	errors2 "github.com/lyft/flytestdlib/errors"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/lyft/flyteplugins/go/tasks/logs"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
)

const (
	ErrCheckPodStatus errors2.ErrorCode = "CHECK_POD_FAILED"
)

func CheckSubTasksState(ctx context.Context, tCtx core.TaskExecutionContext, kubeClient core.KubeClient, cfg *Config, currentState *array2.State) (
	newState *array2.State, err error) {
	logLinks := make([]*core2.TaskLog, 0, 4)
	newState = currentState

	msg := errorcollector2.NewErrorMessageCollector()
	newArrayStatus := arraystatus2.ArrayStatus{
		Summary:  arraystatus2.ArraySummary{},
		Detailed: newStatusCompactArray(uint(currentState.GetExecutionArraySize())),
	}

	for childIdx, existingPhaseIdx := range currentState.GetArrayStatus().Detailed.GetItems() {
		existingPhase := core.Phases[existingPhaseIdx]
		if existingPhase.IsTerminal() {
			// If we get here it means we have already "processed" this terminal phase since we will only persist
			// the phase after all processing is done (e.g. check outputs/errors file, record events... etc.).
			newArrayStatus.Summary.Inc(existingPhase)

			// TODO: collect log links before doing this
			continue
		}

		phaseInfo, err := CheckPodStatus(ctx, kubeClient,
			types2.NamespacedName{
				Name:      formatSubTaskName(ctx, tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), strconv.Itoa(childIdx)),
				Namespace: tCtx.TaskExecutionMetadata().GetNamespace(),
			})
		if err != nil {
			return currentState, errors2.Wrapf(ErrCheckPodStatus, err, "Failed to check pod status")
		}

		if phaseInfo.Info() != nil {
			logLinks = append(logLinks, phaseInfo.Info().Logs...)
		}

		if phaseInfo.Err() != nil {
			msg.Collect(childIdx, phaseInfo.Err().String())
		}

		newArrayStatus.Detailed.SetItem(childIdx, bitarray.Item(phaseInfo.Phase()))
		newArrayStatus.Summary.Inc(phaseInfo.Phase())
	}

	newState = newState.SetArrayStatus(newArrayStatus)

	// Check that the taskTemplate is valid
	taskTemplate, err := tCtx.TaskReader().Read(ctx)
	if err != nil {
		return currentState, err
	} else if taskTemplate == nil {
		return currentState, fmt.Errorf("required value not set, taskTemplate is nil")
	}

	phase := array2.SummaryToPhase(ctx, currentState.OriginalMinSuccesses-currentState.OriginalArraySize+int64(currentState.ExecutionArraySize), newArrayStatus.Summary)
	if phase == array2.PhasePermanentFailure || phase == array2.PhaseRetryableFailure {
		errorMsg := msg.Summary(GetConfig().MaxErrorStringLength)
		newState = newState.SetReason(errorMsg)
	}

	if phase == array2.PhaseCheckingSubTaskExecutions {
		newPhaseVersion := uint32(0)
		if phase == array2.PhaseCheckingSubTaskExecutions {
			// For now, the only changes to PhaseVersion and PreviousSummary occur for running array jobs.
			for phase, count := range newState.GetArrayStatus().Summary {
				newPhaseVersion += uint32(phase) * uint32(count)
			}
		}

		newState = newState.SetPhase(phase, newPhaseVersion)
	} else {
		newState = newState.SetPhase(phase, core.DefaultPhaseVersion)
	}

	return newState, nil
}

func CheckPodStatus(ctx context.Context, client core.KubeClient, name types2.NamespacedName) (
	info core.PhaseInfo, err error) {

	pod := &v1.Pod{
		TypeMeta: v12.TypeMeta{
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
			return core.PhaseInfoFailed(core.PhaseRetryableFailure, &core2.ExecutionError{
				Code:    string(k8serrors.ReasonForError(err)),
				Message: err.Error(),
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
		return core.PhaseInfoSuccess(&taskInfo), nil
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
