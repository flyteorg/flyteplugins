package k8s

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"

	"github.com/lyft/flytedynamicjoboperator/pkg/apis/futures/v1alpha1"
	"github.com/lyft/flytedynamicjoboperator/pkg/controller/dynamicjob"
	"github.com/lyft/flyteidl/clients/go/events/errors"
	core2 "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteplugins.old/go/tasks/v1/events"
	"github.com/lyft/flyteplugins.old/go/tasks/v1/types"
	"github.com/lyft/flyteplugins/go/tasks/v1/array/arraystatus"
	"github.com/lyft/flyteplugins/go/tasks/v1/array/bitarray"
	"github.com/lyft/flyteplugins/go/tasks/v1/array/errorcollector"
	"github.com/lyft/flyteplugins/go/tasks/v1/logs"
	errors2 "github.com/lyft/flytestdlib/errors"
	"github.com/lyft/flytestdlib/logger"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/core"
	"github.com/lyft/flyteplugins/go/tasks/v1/array"
)

const (
	ErrCheckPodStatus errors2.ErrorCode = "CHECK_POD_FAILED"
)

func CheckSubTasksState(ctx context.Context, tCtx core.TaskExecutionContext, cfg *Config, currentState *array.State) (
	newState *array.State, err error) {
	logs := make([]*core2.TaskLog, 0, 4)

	msg := errorcollector.NewErrorMessageCollector()
	newArrayStatus := arraystatus.ArrayStatus{
		Summary:  arraystatus.ArraySummary{},
		Detailed: newStatusCompactArray(uint(currentState.GetActualArraySize())),
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

		phase, podEventInfo, errMsg, err := CheckPodStatus(ctx, formatSubTaskName(ctx, tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), strconv.Itoa(childIdx)))
		if err != nil {
			return currentState, errors2.Wrapf(ErrCheckPodStatus, err, "Failed to check pod status")
		}

		if podEventInfo != nil {
			logs = append(logs, podEventInfo.Logs...)
		}

		// If we have already reached a terminal state before, don't attempt to override it (in case of long running
		// jobs, pods that finished early might disappear causing the phase to attempt to be set to failure)
		if existingPhase.IsSuccess() {
			// This's meant to avoid any failure message to be passed on in case the original attempt has succeeded.
			errMsg = ""
		}

		if len(errMsg) > 0 {
			msg.Collect(childIdx, errMsg)
		}

		newArrayStatus.Detailed.SetItem(childIdx, bitarray.Item(phase))
		newArrayStatus.Summary.Inc(phase)
	}

	// Check that the taskTemplate is valid
	taskTemplate, err := tCtx.TaskReader().Read(ctx)
	if err != nil {
		return currentState, err
	} else if taskTemplate == nil {
		return currentState, fmt.Errorf("required value not set, taskTemplate is nil")
	}

	var arrayJob *plugins.ArrayJob
	if taskTemplate.GetCustom() != nil {
		arrayJob, err = array.ToArrayJob(taskTemplate.GetCustom())
		if err != nil {
			return currentState, err
		}
	}

	phase := SummaryToTaskPhase(ctx, arrayJob, newArrayStatus.Summary)
	if phase.IsFailure() {
		errorMsg := msg.Summary(cfg.MaxErrorStringLength)
		newState = currentState.SetReason(errorMsg)
		newState = newState.SetPhase(phase)
	}

	var phaseVersionChanged bool
	if phase == types.TaskPhaseRunning {
		// For now, the only changes to PhaseVersion and PreviousSummary occur for running array jobs.
		for phase, count := range customState.ArrayStatus.Summary {
			previousCount := previousSummary[phase]
			if previousCount != count {
				phaseVersionChanged = true
				break
			}
		}
	}

	if phaseVersionChanged {
		taskStatus.PhaseVersion = taskCtx.GetPhaseVersion() + 1
	}

	now := time.Now()
	if err := e.eventRecorder.RecordTaskEvent(ctx,
		events.CreateEvent(taskCtx, taskStatus, &events.TaskEventInfo{OccurredAt: &now, Logs: logs})); err != nil {

		logger.Infof(ctx, "Failed to write task event. Error: %v", err)
		return taskStatus.WithState(marshaled), err
	}

	return taskStatus.WithState(marshaled), nil
}

func CheckPodStatus(ctx context.Context, taskCtx types.TaskContext) (
	phase types.TaskPhase, info *events.TaskEventInfo, msg string, err error) {

	pod := &corev1.Pod{
		TypeMeta: v12.TypeMeta{
			Kind:       v1alpha1.KindK8sPod,
			APIVersion: corev1.SchemeGroupVersion.String(),
		},
	}

	err = e.getResource(ctx, types2.NamespacedName{
		Name:      taskCtx.GetTaskExecutionID().GetGeneratedName(),
		Namespace: taskCtx.GetNamespace(),
	}, pod)

	if err != nil {
		if k8serrors.IsNotFound(err) {
			// If the object disappeared at this point, it means it was manually removed or garbage collected.
			// Mark it as a failure.
			return types.TaskPhasePermanentFailure, nil, "marked as failed because it was not found.", nil
		}

		return phase, nil, "", err
	}

	podPhase := JobStatusToPhase(pod)

	if PhaseManager.IsTerminalPhase(podPhase.String()) {
		// TODO: Do we want to let the error document override the pod phase (if the pod has already failed) as we do here?
		podPhase, msg, err = e.CheckTaskInstanceStatus(ctx, podPhase, taskCtx.GetDataDir())

		if err != nil {
			return podPhase, nil, msg, err
		}
	}

	taskIdentifier := taskCtx.GetTaskExecutionID().GetID().TaskId
	taskName := "User"
	if taskIdentifier != nil {
		taskName = taskIdentifier.Name
	}

	taskLogs, err := logs.GetLogsForContainerInPod(ctx, pod, 0, fmt.Sprintf(" (%v)", taskName))
	if err != nil {
		return podPhase, nil, "", err
	}

	// TODO: Admin doesn't support sending events for individual pods yet..
	//newTaskStatus := types.TaskStatus{Phase: podPhase, Err: fmt.Errorf(msg)}
	//if err := e.eventRecorder.RecordTaskEvent(ctx,
	//	events.CreateEvent(taskCtx, newTaskStatus, &events.TaskEventInfo{OccurredAt: &now, Logs: taskLogs})); err != nil {
	//
	//	logger.Infof(ctx, "Failed to write task event. Error: %v", err)
	//	return podPhase, msg, err
	//}

	now := time.Now()
	return podPhase, &events.TaskEventInfo{OccurredAt: &now, Logs: taskLogs}, msg, err
}

func (e Executor) CheckTaskStatus(ctx context.Context, taskCtx types.TaskContext, task *core.TaskTemplate) (types.TaskStatus, error) {
	if taskCtx.GetPhase().IsTerminal() {
		return types.TaskStatus{
			Phase: taskCtx.GetPhase(),
		}.WithState(taskCtx.GetCustomState()), nil
	}

	customState := arraystatus.CustomState{}
	err := customState.MergeFrom(ctx, taskCtx.GetCustomState())
	if err != nil {
		return types.TaskStatusUndefined, err
	}

	logs := make([]*core.TaskLog, 0, 4)

	if customState.ArrayStatus != nil {
		msg := errorcollector.NewErrorMessageCollector()
		newSummary := arraystatus.ArraySummary{}
		previousSummary := arraystatus.CopyArraySummaryFrom(customState.ArrayStatus.Summary)

		for childIdx, existingPhaseIdx := range customState.ArrayStatus.Detailed.GetItems() {
			existingPhase := PhaseManager.FromPhaseIndex(int(existingPhaseIdx))
			if PhaseManager.IsTerminalPhase(existingPhase) {
				// If we get here it means we have already "processed" this terminal phase since we will only persist
				// the phase after all processing is done (e.g. check outputs/errors file, record events... etc.).
				newSummary.Inc(ToTaskPhase(existingPhase))
				continue
			}

			subNodeName := strconv.Itoa(childIdx)
			subCtx, err := dynamicjob.CreateSubContext(ctx, taskCtx, e.dataStore, subNodeName, subNodeName, types.TaskPhaseUnknown, taskCtx.GetCustomState())
			if err != nil {
				return types.TaskStatusUndefined, errors.WrapError(err, errors.NewUnknownError("Failed to build task context."))
			}

			phase, podEventInfo, errMsg, err := e.CheckPodStatus(ctx, subCtx)
			if err != nil {
				return types.TaskStatusUndefined, errors.WrapError(err, errors.NewUnknownError("Failed to check pod status"))
			}

			if podEventInfo != nil {
				logs = append(logs, podEventInfo.Logs...)
			}

			// If we have already reached a terminal state before, don't attempt to override it (in case of long running
			// jobs, pods that finished early might disappear causing the phase to attempt to be set to failure)
			existingPhaseValue := PhaseManager.FromPhaseIndex(int(existingPhaseIdx))
			if PhaseManager.IsTerminalPhase(existingPhaseValue) {
				phase = ToTaskPhase(existingPhaseValue)

				if PhaseManager.IsSuccessPhase(phase.String()) {
					// This's meant to avoid any failure message to be passed on in case the original attempt has succeeded.
					errMsg = ""
				}
			}

			if len(errMsg) > 0 {
				msg.Collect(childIdx, errMsg)
			}

			customState.ArrayStatus.Detailed.SetItem(childIdx, bitarray.Item(PhaseManager.GetPhaseIndex(phase.String())))
			newSummary.Inc(phase)
		}

		customState.ArrayStatus.Summary = newSummary
		marshaled, err := customState.AsMap(ctx)
		if err != nil {
			return types.TaskStatusUndefined, err
		}

		phase := SummaryToTaskPhase(ctx, customState.ArrayProperties, newSummary)
		taskStatus := types.TaskStatus{Phase: phase}
		if phase.IsRetryableFailure() || phase.IsPermanentFailure() {
			errorMsg := msg.Summary(maxErrorStringLength)
			taskStatus = types.TaskStatus{Phase: phase, Err: fmt.Errorf(errorMsg)}
		}

		var phaseVersionChanged bool
		if phase == types.TaskPhaseRunning {
			// For now, the only changes to PhaseVersion and PreviousSummary occur for running array jobs.
			for phase, count := range customState.ArrayStatus.Summary {
				previousCount := previousSummary[phase]
				if previousCount != count {
					phaseVersionChanged = true
					break
				}
			}
		}

		if phaseVersionChanged {
			taskStatus.PhaseVersion = taskCtx.GetPhaseVersion() + 1
		}

		now := time.Now()
		if err := e.eventRecorder.RecordTaskEvent(ctx,
			events.CreateEvent(taskCtx, taskStatus, &events.TaskEventInfo{OccurredAt: &now, Logs: logs})); err != nil {

			logger.Infof(ctx, "Failed to write task event. Error: %v", err)
			return taskStatus.WithState(marshaled), err
		}

		return taskStatus.WithState(marshaled), nil
	}

	phase, podEventInfo, errMsg, err := e.CheckPodStatus(ctx, taskCtx)
	if err != nil {
		return types.TaskStatusUndefined, errors.WrapError(err, errors.NewUnknownError("Failed to check pod status"))
	}

	var taskStatus types.TaskStatus
	if phase.IsRetryableFailure() || phase.IsPermanentFailure() {
		taskStatus = types.TaskStatus{Phase: phase, Err: fmt.Errorf(errMsg)}
	} else {
		taskStatus = types.TaskStatus{Phase: phase}
	}

	if err := e.eventRecorder.RecordTaskEvent(ctx, events.CreateEvent(taskCtx, taskStatus, podEventInfo)); err != nil {
		logger.Infof(ctx, "Failed to write task event. Error: %v", err)
		return taskStatus.WithState(taskCtx.GetCustomState()), err
	}

	return taskStatus.WithState(taskCtx.GetCustomState()), nil
}
