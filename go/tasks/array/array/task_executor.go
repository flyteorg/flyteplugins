/*
 * Copyright (c) 2018 Lyft. All rights reserved.
 */

package array

import (
	"context"
	"fmt"
	"github.com/lyft/flyteplugins/go/tasks/array/k8s"
	"strconv"
	"time"

	"github.com/lyft/flyteplugins/go/tasks/logs"
	v12 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/lyft/flytedynamicjoboperator/pkg/apis/futures/v1alpha1"

	"github.com/lyft/flyteplugins/go/tasks/flytek8s"
	"github.com/lyft/flyteplugins/go/tasks/v1/events"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/lyft/flytedynamicjoboperator/pkg/controller/dynamicjob"

	"github.com/lyft/flyteplugins/go/tasks/v1/utils"

	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/runtime/inject"
	"sigs.k8s.io/controller-runtime/pkg/source"

	source2 "github.com/lyft/flytedynamicjoboperator/pkg/controller/source"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"
	v1 "github.com/lyft/flyteplugins/go/tasks/v1"
	"github.com/lyft/flyteplugins/go/tasks/v1/types"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/storage"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	types2 "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/lyft/flytedynamicjoboperator/errors"
	"github.com/lyft/flytedynamicjoboperator/pkg/apis/futures/v1alpha1/arraystatus"
	"github.com/lyft/flytedynamicjoboperator/pkg/internal"
	"github.com/lyft/flytedynamicjoboperator/pkg/internal/bitarray"
	"github.com/lyft/flytedynamicjoboperator/pkg/internal/errorcollector"
)

const (
	arrayTaskType        = "container_array"
	executorName         = "K8S-ARRAY"
	maxErrorStringLength = 100
)

var nilLiteral = &core.Literal{
	Value: &core.Literal_Scalar{
		Scalar: &core.Scalar{
			Value: &core.Scalar_NoneType{
				NoneType: &core.Void{},
			},
		},
	},
}

type envVarName = string

var PhaseManager, _ = internal.NewPhaseManager(
	[]string{
		types.TaskPhaseQueued.String(),
		types.TaskPhaseRunning.String(),
		types.TaskPhaseSucceeded.String(),
		types.TaskPhasePermanentFailure.String(),
		types.TaskPhaseRetryableFailure.String(),
	},
	[]string{types.TaskPhaseSucceeded.String()},
	[]string{types.TaskPhasePermanentFailure.String(), types.TaskPhaseRetryableFailure.String()})

const (
	JobIndexVarName           envVarName = "BATCH_JOB_ARRAY_INDEX_VAR_NAME"
	FlyteK8sArrayIndexVarName envVarName = "FLYTE_K8S_ARRAY_INDEX"
)

var arrayJobEnvVars = []corev1.EnvVar{
	{
		Name:  JobIndexVarName,
		Value: FlyteK8sArrayIndexVarName,
	},
}

type Executor struct {
	eventRecorder types.EventRecorder
	dataStore     *storage.DataStore
	runtimeClient client.Client
	cache         cache.Cache
}

func (e *Executor) InjectClient(c client.Client) error {
	e.runtimeClient = c
	return nil
}

func (e *Executor) InjectCache(c cache.Cache) error {
	e.cache = c
	return nil
}

func (Executor) GetID() types.TaskExecutorName {
	return executorName
}

func (Executor) GetProperties() types.ExecutorProperties {
	return types.ExecutorProperties{}
}

func (e *Executor) Initialize(ctx context.Context, params types.ExecutorInitializationParameters) error {

	e.eventRecorder = params.EventRecorder
	e.dataStore = params.DataStore

	src := &source.Kind{Type: &corev1.Pod{}}
	if _, err := inject.CacheInto(e.cache, src); err != nil {
		return err
	}

	return src.Start(
		source2.NewEnqueueOwnerAdapter(params.EnqueueOwner),
		workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()))
}

func (e Executor) StartTask(ctx context.Context, taskCtx types.TaskContext, task *core.TaskTemplate, inputs *core.LiteralMap) (types.TaskStatus, error) {
	if task == nil {
		logger.Warnf(ctx, "task is nil, skipping.")
		return types.TaskStatusSucceeded, nil
	}

	podTemplate, arrayProps, err := FlyteArrayJobToK8sPod(ctx, taskCtx, task, inputs)
	if err != nil {
		logger.Errorf(ctx, "Failed to build batch job input: %v", err)
		return types.TaskStatusUndefined, errors.WrapError(err, errors.NewUnknownError("Failed to build batch job input"))
	}

	customState, err := e.startAllJobs(ctx, taskCtx, podTemplate, arrayProps)
	if err != nil {
		logger.Errorf(ctx, "Failed to start all jobs: %v", err)
		return types.TaskStatusUndefined, errors.WrapError(err, errors.NewUnknownError("Failed to start all jobs"))
	}

	return types.TaskStatusRunning.WithState(customState), nil
}

func (e Executor) ApplyPodPolicies(ctx context.Context, pod *corev1.Pod) *corev1.Pod {
	c := k8s.GetConfig()
	if len(c.DefaultScheduler) > 0 {
		pod.Spec.SchedulerName = c.DefaultScheduler
	}

	return pod
}

func (e Executor) startAllJobs(ctx context.Context, taskCtx types.TaskContext,
	podTemplate corev1.Pod, arrayProps *plugins.ArrayJob) (state types.CustomState, err error) {

	var command []string
	if len(podTemplate.Spec.Containers) > 0 {
		command = append(podTemplate.Spec.Containers[0].Command, podTemplate.Spec.Containers[0].Args...)
		podTemplate.Spec.Containers[0].Args = []string{}
	}

	args := utils.CommandLineTemplateArgs{
		Input:        taskCtx.GetDataDir().String(),
		OutputPrefix: taskCtx.GetDataDir().String(),
	}

	size := int64(1)
	if arrayProps != nil {
		size = arrayProps.Size
	}

	// TODO: Respect slots param
	for i := int64(0); i < size; i++ {
		pod := podTemplate.DeepCopy()
		indexStr := strconv.Itoa(int(i))
		if size > 1 {
			subNodeName := strconv.FormatInt(i, 10)
			subCtx, err := dynamicjob.CreateSubContext(ctx, taskCtx, e.dataStore, subNodeName, subNodeName,
				types.TaskPhaseUnknown, taskCtx.GetCustomState())
			if err != nil {
				return nil, errors.WrapError(err, errors.NewUnknownError("Failed to create sub context for task [%v] with index [%v]", taskCtx.GetTaskExecutionID(), i))
			}

			pod.Name = subCtx.GetTaskExecutionID().GetGeneratedName()
			pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env, corev1.EnvVar{
				Name:  FlyteK8sArrayIndexVarName,
				Value: indexStr,
			})

			pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env, arrayJobEnvVars...)
		} else {
			pod.Name = taskCtx.GetTaskExecutionID().GetGeneratedName()
		}

		pod.Spec.Containers[0].Command, err = utils.ReplaceTemplateCommandArgs(ctx, command, args)
		if err != nil {
			return nil, errors.WrapError(err, errors.NewUnknownError("Failed to replace cmd args"))
		}

		pod = e.ApplyPodPolicies(ctx, pod)

		err = e.runtimeClient.Create(ctx, pod)
		if err != nil && !k8serrors.IsAlreadyExists(err) {
			return nil, errors.WrapError(err, errors.NewUnknownError("Failed to submit job"))
		}
	}

	logger.Infof(ctx, "Successfully submitted Job [%v]", taskCtx.GetTaskExecutionID().GetGeneratedName())

	var arrayStatus *arraystatus.ArrayStatus
	if size > 1 {
		arrayStatus = &arraystatus.ArrayStatus{
			Summary:  arraystatus.ArraySummary{},
			Detailed: newStatusCompactArray(uint(size)),
		}
	}

	var arrayJob *arraystatus.ArrayJob
	if arrayProps != nil {
		arrayJob = &arraystatus.ArrayJob{ArrayJob: *arrayProps}
	}

	return arraystatus.CustomState{
		ArrayStatus:     arrayStatus,
		ArrayProperties: arrayJob,
	}.AsMap(ctx)
}

func (e Executor) CheckTaskInstanceStatus(ctx context.Context, podPhase types.TaskPhase,
	dataDir storage.DataReference) (phase types.TaskPhase, msg string, err error) {
	// Check for an error document. If not found, we fallback to the pod's reported phase for error information.
	// Note: We don't check for an outputs.pb because it is not required that one is produced if no outputs are referenced.
	err = e.ExtractError(ctx, dataDir)
	if err != nil {
		return types.TaskPhasePermanentFailure, err.Error(), nil
	}
	return podPhase, "", nil
}

func (e Executor) CheckPodStatus(ctx context.Context, taskCtx types.TaskContext) (
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

func (e Executor) getResource(ctx context.Context, key types2.NamespacedName, obj runtime.Object) error {
	err := e.cache.Get(ctx, key, obj)
	if err != nil && flytek8s.IsK8sObjectNotExists(err) {
		return e.runtimeClient.Get(ctx, key, obj)
	}

	return nil
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

func (Executor) KillTask(ctx context.Context, taskCtx types.TaskContext, reason string) error {
	return nil
}

func (e Executor) ResolveOutputs(ctx context.Context, taskCtx types.TaskContext, outputVariables ...types.VarName) (
	values map[types.VarName]*core.Literal, err error) {
	customStatus := &arraystatus.CustomState{}
	err = customStatus.MergeFrom(ctx, taskCtx.GetCustomState())
	if err != nil {
		return nil, err
	}

	values = map[types.VarName]*core.Literal{}
	for _, varName := range outputVariables {
		index, actualVar, err := internal.ParseVarName(varName)
		if err != nil {
			return nil, err
		}

		var outputsPath storage.DataReference
		if index == nil {
			outputsPath, err = dynamicjob.GetOutputsPath(ctx, e.dataStore, taskCtx.GetDataDir())
		} else {
			subTaskPhase := ToTaskPhase(PhaseManager.FromPhaseIndex(int(customStatus.ArrayStatus.Detailed.GetItem(*index))))
			if subTaskPhase.IsSuccess() {
				prefix, err := dynamicjob.GetArrayOutputsPrefixPath(ctx, e.dataStore, taskCtx.GetDataDir(), strconv.Itoa(*index))
				if err != nil {
					return nil, err
				}

				outputsPath, err = dynamicjob.GetOutputsPath(ctx, e.dataStore, prefix)
				if err != nil {
					return nil, err
				}
			} else {
				values[varName] = nilLiteral
				continue
			}
		}

		if err != nil {
			return nil, err
		}

		l := &core.LiteralMap{}
		err = e.dataStore.ReadProtobuf(ctx, outputsPath, l)

		// If we are in this method, an output is expected for the task when it completes successfully.  Therefore,
		// in any case where we fail to read this protobuf, it is an error!
		if err != nil {
			return nil, err
		}

		values[varName] = l.Literals[actualVar]
	}

	return values, nil
}

func (e Executor) ExtractError(ctx context.Context, dataDir storage.DataReference) error {
	errDoc, err := dynamicjob.ReadErrorsFile(ctx, e.dataStore, dataDir)
	if storage.IsNotFound(err) || (err == nil && errDoc.Error == nil) {
		return nil
	} else if err != nil {
		return err
	}
	return errors.Error{
		Code:    errors.ErrorCode(errDoc.Error.Code),
		Message: errDoc.Error.Message,
	}
}

func LoadPlugin(_ context.Context) error {
	return v1.RegisterForTaskTypes(&Executor{}, arrayTaskType)
}

func init() {
	v1.RegisterLoader(LoadPlugin)
}

func newStatusCompactArray(count uint) bitarray.CompactArray {
	a, err := bitarray.NewCompactArray(count, bitarray.Item(PhaseManager.GetCount()-1))
	if err != nil {
		return bitarray.CompactArray{}
	}

	return a
}

// Converts k8s batch Job status to v1alpha1 Phase.
func JobStatusToPhase(j *corev1.Pod) types.TaskPhase {
	switch j.Status.Phase {
	case corev1.PodFailed:
		return types.TaskPhasePermanentFailure
	case corev1.PodSucceeded:
		return types.TaskPhaseSucceeded
	default:
		return types.TaskPhaseRunning
	}
}

func SummaryToTaskPhase(ctx context.Context, arrayJobProps *arraystatus.ArrayJob, summary arraystatus.ArraySummary) types.TaskPhase {
	minSuccesses := int64(1)
	if arrayJobProps != nil {
		minSuccesses = arrayJobProps.MinSuccesses
	}

	totalCount := int64(0)
	totalSuccesses := int64(0)
	totalFailures := int64(0)
	totalRunning := int64(0)
	for phase, count := range summary {
		totalCount += count
		if phase.IsTerminal() {
			if phase.IsSuccess() {
				totalSuccesses += count
			} else {
				// TODO: Split out retryable failures to be retried without doing the entire array task.
				// TODO: Other option: array tasks are only retriable as a full set and to get single task retriability
				// TODO: dynamic_task must be updated to not auto-combine to array tasks.  For scale reasons, it is
				// TODO: preferable to auto-combine to array tasks for now.
				totalFailures += count
			}
		} else {
			totalRunning += count
		}
	}

	if totalCount < minSuccesses {
		logger.Infof(ctx, "Array failed because totalCount[%v] < minSuccesses[%v]", totalCount, minSuccesses)
		return types.TaskPhasePermanentFailure
	}

	// No chance to reach the required success numbers.
	if totalRunning+totalSuccesses < minSuccesses {
		logger.Infof(ctx, "Array failed early because totalRunning[%v] + totalSuccesses[%v] < minSuccesses[%v]",
			totalRunning, totalSuccesses, minSuccesses)
		return types.TaskPhasePermanentFailure
	}

	if totalSuccesses >= minSuccesses && totalRunning == 0 {
		logger.Infof(ctx, "Array succeeded because totalSuccesses[%v] >= minSuccesses[%v]", totalSuccesses, minSuccesses)
		return types.TaskPhaseSucceeded
	}

	logger.Debugf(ctx, "Array is still running [Successes: %v, Failures: %v, Total: %v, MinSuccesses: %v]",
		totalSuccesses, totalFailures, totalCount, minSuccesses)
	return types.TaskPhaseRunning
}

func ToTaskPhase(phaseValue internal.PhaseValue) types.TaskPhase {
	switch phaseValue {
	case types.TaskPhaseQueued.String():
		return types.TaskPhaseQueued
	case types.TaskPhaseRunning.String():
		return types.TaskPhaseRunning
	case types.TaskPhaseSucceeded.String():
		return types.TaskPhaseSucceeded
	case types.TaskPhasePermanentFailure.String():
		return types.TaskPhasePermanentFailure
	case types.TaskPhaseRetryableFailure.String():
		return types.TaskPhaseRetryableFailure
	}

	return types.TaskPhaseUndefined
}
