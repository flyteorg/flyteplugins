/*
 * Copyright (c) 2018 Lyft. All rights reserved.
 */

package awsbatch

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"

	"github.com/lyft/flytedynamicjoboperator/pkg/apis/futures/v1alpha1/arraystatus"
	"github.com/lyft/flytedynamicjoboperator/pkg/internal/errorcollector"
	"github.com/lyft/flyteplugins/go/tasks/v1/events"

	"github.com/lyft/flytedynamicjoboperator/pkg/aws"
	"github.com/lyft/flytedynamicjoboperator/pkg/catalog"

	"github.com/aws/aws-sdk-go/service/batch"
	"github.com/lyft/flytedynamicjoboperator/errors"
	"github.com/lyft/flytedynamicjoboperator/pkg/aws/batch/definition"
	ctrlConfig "github.com/lyft/flytedynamicjoboperator/pkg/controller/config"
	controller "github.com/lyft/flytedynamicjoboperator/pkg/controller/dynamicjob"
	"github.com/lyft/flytedynamicjoboperator/pkg/internal"
	"github.com/lyft/flytedynamicjoboperator/pkg/internal/bitarray"
	"github.com/lyft/flytedynamicjoboperator/pkg/resource/flowcontrol"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	v1 "github.com/lyft/flyteplugins/go/tasks/v1"
	"github.com/lyft/flyteplugins/go/tasks/v1/types"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/storage"
)

const (
	arrayTaskType       = "container_array"
	arrayJobIDFormatter = "%v:%v"
	executorName        = "AWS_ARRAY"
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

type Executor struct {
	recorder        types.EventRecorder
	dataStore       *storage.DataStore
	jobsStore       Store
	ownerKind       string
	client          Client
	definitionCache definition.Cache
	catalogCache    catalog.Cache
}

func LoadPlugin(_ context.Context) error {
	return v1.RegisterForTaskTypes(&Executor{
		definitionCache: definition.NewCache(),
	}, arrayTaskType)
}

func init() {
	v1.RegisterLoader(LoadPlugin)
}

func newStatusCompactArray(count uint) bitarray.CompactArray {
	// TODO: This is fragile, we should introduce a TaskPhaseCount as the last element in the enum
	a, err := bitarray.NewCompactArray(count, bitarray.Item(types.TaskPhaseUnknown))
	if err != nil {
		return bitarray.CompactArray{}
	}

	return a
}

func (e *Executor) SetClient(c Client) {
	e.client = c
}

func (e *Executor) GetClient() Client {
	return e.client
}

func (e Executor) GetJobsStore() Store {
	return e.jobsStore
}

func (e *Executor) SetJobsStore(s Store) {
	e.jobsStore = s
}

func (e Executor) GetProperties() types.ExecutorProperties {
	return types.ExecutorProperties{
		RequiresFinalizer:       true,
		DisableNodeLevelCaching: true,
	}
}

func subtaskIsCached(customState *arraystatus.CustomState, subtaskIndex uint) bool {
	if customState.ArrayCachedStatus != nil && customState.ArrayCachedStatus.CachedArrayJobs != nil {
		return customState.ArrayCachedStatus.CachedArrayJobs.IsSet(subtaskIndex)
	}

	return false
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
			outputsPath, err = controller.GetOutputsPath(ctx, e.dataStore, taskCtx.GetDataDir())

			// If we are in this method, an output is expected for the task when it completes successfully.  Therefore,
			// in any case where we fail to read this protobuf, it is an error!
			if err != nil {
				return nil, err
			}

			l := &core.LiteralMap{}
			err = e.dataStore.ReadProtobuf(ctx, outputsPath, l)
			if err != nil {
				return nil, err
			}
			values[varName] = l.Literals[actualVar]
		} else {
			subTaskPhase := types.TaskPhaseSucceeded
			if customStatus.ArrayStatus != nil && !subtaskIsCached(customStatus, uint(*index)) {
				subTaskPhase = types.TaskPhase(customStatus.ArrayStatus.Detailed.GetItem(*index))
			}

			if subTaskPhase.IsSuccess() {
				// If we are in this method, an output is expected for the task when it completes successfully.
				// Therefore, in any case where we fail to read this protobuf, it is an error!
				outputsPath, err = e.dataStore.ConstructReference(ctx, taskCtx.GetDataDir(), strconv.Itoa(*index), controller.OutputsSuffix)
				if err != nil {
					return nil, err
				}

				l := &core.LiteralMap{}
				err = e.dataStore.ReadProtobuf(ctx, outputsPath, l)
				if err != nil {
					return nil, err
				}
				values[varName] = l.Literals[actualVar]
			} else {
				values[varName] = nilLiteral
			}
		}
	}

	return values, nil
}

func (e Executor) ExtractError(ctx context.Context, taskCtx types.TaskContext, childIdx int) error {
	childOutput, err := e.dataStore.ConstructReference(ctx, taskCtx.GetDataDir(), strconv.Itoa(childIdx))
	if err != nil {
		return err
	}

	errDoc, err := controller.ReadErrorsFile(ctx, e.dataStore, childOutput)
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

func (e Executor) getIndexLookupArray(ctx context.Context, customStatus *arraystatus.CustomState, dataDir storage.DataReference) (*catalog.IndexLookupArray, error) {
	cachedStatus := customStatus.ArrayCachedStatus
	if cachedStatus != nil && customStatus.ArrayStatus != nil &&
		customStatus.ArrayStatus.Detailed.ItemsCount != 0 {
		indexLookupArray, err := e.catalogCache.GetIndexLookupArray(ctx, e.dataStore, dataDir)
		if err != nil {
			logger.Errorf(ctx, "Unable to get indexlookup.pb %v", err)
			return nil, err
		}

		if len(indexLookupArray.Literals) != int(customStatus.ArrayStatus.Detailed.ItemsCount) {
			logger.Errorf(ctx, "Invalid index mapping array, lookup size %v does not match arrayjob size submitted, err %v",
				len(indexLookupArray.Literals), customStatus.ArrayStatus.Detailed.ItemsCount, err)
			return nil, fmt.Errorf("Index mapping array len(%v) does not match array job size (%v)",
				len(indexLookupArray.Literals), customStatus.ArrayStatus.Detailed.ItemsCount)
		}

		return indexLookupArray, nil
	}

	return nil, nil
}

func appendTaskLogIfPossible(j Job, suffix string, logs *[]*core.TaskLog) {
	if len(LogStream) > 0 {
		*logs = append(*logs, &core.TaskLog{Uri: LogStream, Name: fmt.Sprintf("Aws Batch%v", suffix)})
	}
}

// Construct the statuses of the original job with all items (cached and non cached)
func constructOriginalDetails(customStatus *arraystatus.CustomState, indexLookup *catalog.IndexLookupArray) (bitarray.CompactArray, error) {
	originalSize := customStatus.ArrayCachedStatus.NumCached + customStatus.ArrayStatus.Detailed.ItemsCount
	originalStatuses := newStatusCompactArray(originalSize)

	// construct map to see which original index corresponds to batch job index
	originalIdxToBatchJobIdx := make(map[uint]int, customStatus.ArrayStatus.Detailed.ItemsCount)
	for index, literal := range indexLookup.Literals {
		originalIdxToBatchJobIdx[uint(literal.GetScalar().GetPrimitive().GetInteger())] = index
	}
	for i := uint(0); i < originalSize; i++ {
		if customStatus.ArrayCachedStatus.CachedArrayJobs.IsSet(i) {
			originalStatuses.SetItem(int(i), uint64(types.TaskPhaseSucceeded))
		} else {
			batchIdx, ok := originalIdxToBatchJobIdx[i]
			if !ok {
				return originalStatuses, fmt.Errorf("Indexlookup does not have the index of the original subtask %v", i)
			}

			originalStatuses.SetItem(int(i), customStatus.ArrayStatus.Detailed.GetItem(batchIdx))
		}
	}

	return originalStatuses, nil
}

func (e Executor) CheckTaskStatus(ctx context.Context, taskCtx types.TaskContext, task *core.TaskTemplate) (status types.TaskStatus, err error) {
	pluginsCustom := taskCtx.GetCustomState()
	customStatus := &arraystatus.CustomState{}
	err = customStatus.MergeFrom(ctx, pluginsCustom)
	if err != nil {
		return types.TaskStatusUndefined, err
	}

	job, found, err := Get(ctx, taskCtx.GetTaskExecutionID().GetGeneratedName())
	if err != nil {
		return types.TaskStatusUndefined, err
	}

	if !found {
		// We might be recovering from a restart, this "slowly" rebuilds the jobId cache for monitoring...
		status := JobStatus{}
		if customStatus.ArrayStatus != nil {
			count := int64(0)
			for _, val := range customStatus.ArrayStatus.Summary {
				count += val
			}

			ArrayJob = ArrayJobSummary{}
			ArrayJob[batch.JobStatusSubmitted] = count
		}
		if err := e.ensureBatchJobMonitored(ctx, Job{
			Name:   taskCtx.GetTaskExecutionID().GetGeneratedName(),
			ID:     customStatus.JobID,
			Status: status,
			Owner:  taskCtx.GetOwnerID(),
		}); err != nil {
			return types.TaskStatusUndefined, err
		}

		outStatus := types.TaskStatusRunning.WithState(pluginsCustom)
		return outStatus, nil
	}

	logs := make([]*core.TaskLog, 0, 1)

	appendTaskLogIfPossible(job, "", &logs)

	var previousSummary arraystatus.ArraySummary
	var newCustomState types.CustomState
	var newTaskStatus *types.TaskStatus
	newJobPhase := batch.JobStatusSubmitted
	if ArrayJob != nil {
		msg := errorcollector.NewErrorMessageCollector()

		if customStatus.ArrayStatus == nil {
			count := 0
			// Convert summary to use taskStatus
			for _, val := range ArrayJob {
				count += int(val)
			}

			customStatus.ArrayStatus = &arraystatus.ArrayStatus{
				Summary:  arraystatus.ArraySummary{},
				Detailed: newStatusCompactArray(uint(count)),
			}
		}

		previousSummary = arraystatus.CopyArraySummaryFrom(customStatus.ArrayStatus.Summary)
		customStatus.ArrayStatus.Summary = arraystatus.ArraySummary{}

		indexLookupArray, err := e.getIndexLookupArray(ctx, customStatus, taskCtx.GetDataDir())
		if err != nil {
			return types.TaskStatusPermanentFailure(err), nil
		}

		for childIdx, existingPhaseInt := range customStatus.ArrayStatus.Detailed.GetItems() {
			subJob, err := e.findBatchJob(ctx, ID, taskCtx.GetTaskExecutionID().GetGeneratedName(), childIdx)
			if err != nil {
				return types.TaskStatusUndefined, err
			}

			originalIndex := childIdx
			if indexLookupArray != nil {
				originalIndex = int(indexLookupArray.Literals[childIdx].GetScalar().GetPrimitive().GetInteger())
			}

			var phase types.TaskPhase
			existingPhase := types.TaskPhase(existingPhaseInt)
			if existingPhase.IsTerminal() {
				// No need to re-evaluate.
				phase = existingPhase
			} else {
				// The summary is already known given this sub-jobs status, so we need just set the detailed information
				phase = ToTaskPhase(Phase)
				if phase.IsTerminal() {
					// TODO: If the pod has failed, do we want to allow reading the error document to override the error as it does?
					// If an errors.pb file exists, we should mark this task as an error. Note: We do not care if an
					// outputs.pb exists because it isn't required if no outputs are referenced.
					if err := e.ExtractError(ctx, taskCtx, childIdx); err != nil {
						// TODO: If error is recoverable from the error document, we should mark it as such here.
						// We are changing the status from what is defined in the summary by the array task, thus we must
						// decrement and increment the summary.
						phase = types.TaskPhasePermanentFailure
						msg.Collect(childIdx, err.Error())
					} else if phase.IsPermanentFailure() {
						msg.Collect(childIdx, "Job failed")
					}

					// Cache subtask output to Catalog if phase completed successfully
					if customStatus.ArrayCachedStatus != nil && phase.IsSuccess() {
						taskExecID := taskCtx.GetTaskExecutionID().GetID()
						taskTemplate := &core.TaskTemplate{
							Id: customStatus.ArrayCachedStatus.TaskIdentifier,
							Metadata: &core.TaskMetadata{
								DiscoveryVersion: customStatus.ArrayCachedStatus.DiscoverableVersion,
							},
							Interface: task.Interface,
						}
						err = e.catalogCache.CacheSubtaskExecution(ctx, e.dataStore, taskCtx.GetDataDir(),
							originalIndex, taskExecID, taskTemplate)
						if err != nil {
							// non-critical failure if we fail to cache the subtask, log and continue
							logger.Warningf(ctx, "Unable to cache subtask %v, err %v", taskExecID, err)
						}
					}
				}
			}

			logger.Debugf(ctx, "Setting subtask idx[%v] to phase [%v]", childIdx, phase)
			customStatus.ArrayStatus.Detailed.SetItem(childIdx, uint64(phase))
			customStatus.ArrayStatus.Summary.Inc(phase)
			appendTaskLogIfPossible(subJob, fmt.Sprintf(" #%v (%v)", originalIndex, phase.String()), &logs)
			// TODO: [Events] If phase is not equals to existing phase, emit event
		}

		failedCount := int64(0)
		succeededCount := int64(0)
		runningCount := int64(0)
		totalCount := int64(0)
		queuedCount := int64(0)
		terminalCount := int64(0)
		for phase, count := range customStatus.ArrayStatus.Summary {
			if phase.IsTerminal() {
				if phase.IsSuccess() {
					succeededCount += count
				} else {
					// TODO: Retryable failures?
					failedCount += count
				}
				terminalCount += count
			} else if phase == types.TaskPhaseQueued {
				queuedCount += count
			} else {
				runningCount += count
			}

			totalCount += count
		}

		// No chance to reach the required success numbers. So terminate early.
		if runningCount+queuedCount+succeededCount < customStatus.ArrayProperties.MinSuccesses {
			errString := fmt.Sprintf(
				"Array failed early because totalRunning[%v] + totalSuccesses[%v] + totalQueued[%v] < minSuccesses[%v]. FailedCount[%v], TotalCount[%v]. Summary: %v",
				runningCount,
				succeededCount,
				queuedCount,
				customStatus.ArrayProperties.MinSuccesses,
				failedCount,
				totalCount,
				msg.Summary(aws.GetConfig().MaxErrorStringLength),
			)

			logger.Infof(ctx, errString)
			newTaskStatus = &types.TaskStatus{Phase: types.TaskPhasePermanentFailure, Err: fmt.Errorf(errString)}
			newJobPhase = batch.JobStatusFailed
		} else if terminalCount == totalCount {
			// If we were going to fail, we would have done so above when checking runningCount+queuedCount+succeededCount
			newJobPhase = batch.JobStatusSucceeded
		} else if runningCount > 0 || failedCount > 0 || succeededCount > 0 {
			newJobPhase = batch.JobStatusRunning
		} else if queuedCount > 0 {
			newJobPhase = batch.JobStatusPending
		} else {
			newJobPhase = batch.JobStatusSubmitted
		}

		if ToTaskPhase(newJobPhase).IsTerminal() {
			// If there are cached items, let's make sure the arrayStatus contains info for all items (cached and noncached)
			// ResolvingOutputs will expect the customState.ArrayStatus.Details to have details of all the items
			if customStatus.ArrayCachedStatus != nil {
				customStatus.ArrayStatus.Detailed, err = constructOriginalDetails(customStatus, indexLookupArray)
				if err != nil {
					return types.TaskStatusUndefined, err
				}
				customStatus.ArrayStatus.Summary[types.TaskPhaseSucceeded] += int64(customStatus.ArrayCachedStatus.NumCached)
			}
		}

		marshaled, err := customStatus.AsMap(ctx)
		if err != nil {
			return types.TaskStatusUndefined, err
		}
		newCustomState = marshaled
	} else {
		// Note: If subJobFound is false, subJob is set to the default struct which will appear in the submitted phase.
		taskName := taskCtx.GetTaskExecutionID().GetGeneratedName()
		subJob, subJobFound, err := Get(ctx, taskName)
		if err != nil {
			logger.Errorf(ctx, "Failed to get job [%v] from job store. Error: %v", taskCtx.GetTaskExecutionID(), err)
		}

		if !subJobFound {
			// TODO: Accumulate errors.
			subJob = Job{
				Name:   taskName,
				ID:     ID,
				Status: JobStatus{},
			}

			err = e.ensureBatchJobMonitored(ctx, subJob)

			if err != nil {
				logger.Errorf(ctx, "Failed to monitor job [%v] [%v]. Error: %v", taskCtx, ID, err)
			}
		}

		// If array is of size 1, we don't construct an array status.
		newPhase := ToTaskPhase(Phase)
		statusMsg := Message
		newTaskStatus = new(types.TaskStatus)
		if newPhase.IsTerminal() {
			// TODO: If the pod has failed, do we want to allow reading the error document to override the error as it does?
			// If an errors.pb file exists, we should mark this task as an error. Note: We do not care if an
			// outputs.pb exists because it isn't required if no outputs are referenced.
			err := e.ExtractError(ctx, taskCtx, 0)

			if err != nil {
				// TODO: If error is recoverable from the error document, we should mark it as such here.
				// We are changing the status from what is defined in the summary by the array task, thus we must
				// decrement and increment the summary.
				statusMsg = err.Error()
			}

			if err != nil || newPhase.IsPermanentFailure() {
				newPhase = types.TaskPhasePermanentFailure
				newJobPhase = batch.JobStatusFailed
			} else {
				newJobPhase = Phase
			}

			*newTaskStatus = ToTaskStatus(newJobPhase, statusMsg)
		} else {
			*newTaskStatus = ToTaskStatus(Phase, statusMsg)
		}
	}

	if newTaskStatus == nil {
		newTaskStatus = new(types.TaskStatus)
		*newTaskStatus = ToTaskStatus(newJobPhase, Message)
	}

	newTaskStatus.State = newCustomState

	var phaseVersionChanged bool
	if newTaskStatus.Phase == types.TaskPhaseRunning && customStatus.ArrayStatus != nil {
		// For now, the only changes to PhaseVersion and PreviousSummary occur for running array jobs.
		for phase, count := range customStatus.ArrayStatus.Summary {
			previousCount := previousSummary[phase]
			if previousCount != count {
				phaseVersionChanged = true
				break
			}
		}
	}

	if phaseVersionChanged {
		newTaskStatus.PhaseVersion = taskCtx.GetPhaseVersion() + 1
	}

	if phaseVersionChanged || newTaskStatus.Phase != taskCtx.GetPhase() {
		now := time.Now()

		// TODO: [Events] Get logs
		if err := e.recorder.RecordTaskEvent(ctx,
			events.CreateEvent(taskCtx, *newTaskStatus, &events.TaskEventInfo{OccurredAt: &now, Logs: logs})); err != nil {

			logger.Infof(ctx, "Failed to write task event. Error: %v", err)
			return types.TaskStatusUndefined, err
		}
	}

	return *newTaskStatus, nil
}

func (Executor) GetID() types.TaskExecutorName {
	return executorName
}

func (e *Executor) Initialize(ctx context.Context, params types.ExecutorInitializationParameters) error {
	e.recorder = params.EventRecorder
	e.dataStore = params.DataStore
	e.ownerKind = params.OwnerKind

	jobsStore := NewInMemoryStore(aws.GetConfig().JobStoreCacheSize, params.MetricsScope.NewSubScope("jobs_store"))
	batchClient := NewBatchClient(aws.GetClient(),
		jobsStore,
		flowcontrol.NewRateLimiter("Get",
			float64(aws.GetConfig().BatchGet.Rate),
			aws.GetConfig().BatchGet.Burst),
		flowcontrol.NewRateLimiter("Default",
			float64(aws.GetConfig().BatchDefault.Rate),
			aws.GetConfig().BatchDefault.Burst))

	e.client = batchClient
	e.jobsStore = jobsStore

	controllerCfg := ctrlConfig.GetConfig()

	e.catalogCache = catalog.NewCatalogCache(params.CatalogClient)
	logger.Infof(ctx, "Watching Batch Jobs with SyncPeriod: %v", controllerCfg.ResyncPeriod)
	runWatcher(ctx, params.EnqueueOwner, controllerCfg.ResyncPeriod.Duration)

	return nil
}

func (e Executor) ensureJobDefinition(ctx context.Context, name, containerImage, role string) (
	jobDefinition definition.JobDefinitionArn, err error) {

	cacheKey := definition.NewCacheKey(role, containerImage)
	if existingArn, found := e.definitionCache.Get(cacheKey); found {
		logger.Infof(ctx, "Found an existing job definition for Image [%v] and Role [%v]. Arn [%v]",
			containerImage, role, existingArn)

		return existingArn, nil
	}

	arn, err := RegisterJobDefinition(ctx, name, containerImage, role)
	if err != nil {
		return "", err
	}

	err = e.definitionCache.Put(cacheKey, arn)
	if err != nil {
		logger.Warnf(ctx, "Failed to store job definition arn in cache. Will continue with the registered arn [%v]. Error: %v",
			arn, err)
	}

	return arn, nil
}

func (e Executor) findBatchJob(ctx context.Context, parentJobID, name string, index int) (Job, error) {
	subJobName := fmt.Sprintf(arrayJobIDFormatter, name, index)
	// Note: If subJobFound is false, subJob is set to the default struct which will appear in the submitted phase.
	subJob, subJobFound, err := Get(ctx, subJobName)
	if err != nil {
		logger.Errorf(ctx, "Failed to get job [%v] from job store. Error: %v", subJobName, err)
	}

	subJobID := fmt.Sprintf(arrayJobIDFormatter, parentJobID, index)
	if !subJobFound {
		// TODO: Accumulate errors.
		subJob = Job{
			Name:   subJobName,
			ID:     subJobID,
			Status: JobStatus{},
		}

		err = e.ensureBatchJobMonitored(ctx, subJob)

		if err != nil {
			logger.Errorf(ctx, "Failed to monitor job [%v] [%v]. Error: %v", subJobName, subJobID, err)
		}
	}

	return subJob, err
}

func (e Executor) ensureBatchJobMonitored(ctx context.Context, job Job) error {
	err := AddOrUpdate(ctx, job)
	if err != nil {
		logger.Errorf(ctx, "failed to get job [%v] from jobs store. Error: %v", String(), err)
		return errors.WrapError(err, errors.NewUnknownError("failed to get job [%v] from jobs store.", String()))
	}

	if ArrayJob != nil {
		count := 0
		for _, val := range ArrayJob {
			count += int(val)
		}

		for i := 0; i < count; i++ {
			childJob := Job{
				ID:     fmt.Sprintf(arrayJobIDFormatter, ID, i),
				Name:   fmt.Sprintf(arrayJobIDFormatter, Name, i),
				Status: JobStatus{},
				Owner:  Owner,
			}

			err = e.ensureBatchJobMonitored(ctx, childJob)

			if err != nil {
				logger.Errorf(ctx, "failed to monitor child job [%v]. Error: %v", String(), err)
			}
		}
	}

	return nil
}

func validateArrayJob(_ context.Context, job *plugins.ArrayJob) error {
	if allowedSize := aws.GetConfig().MaxBatchJobSize; job.Size > allowedSize {
		return fmt.Errorf("job size [%v] exceeds the allowed array size [%v]", job.Size, allowedSize)
	}

	return nil
}

func (e Executor) StartTask(ctx context.Context, taskCtx types.TaskContext, task *core.TaskTemplate, inputs *core.LiteralMap) (status types.TaskStatus, err error) {

	if task == nil {
		logger.Warnf(ctx, "task is nil, skipping.")
		return types.TaskStatusSucceeded, nil
	}

	customState := arraystatus.CustomState{}

	// Check if there are any previously cached tasks. If there are we will only submit an ArrayJob for the
	// non-cached tasks. The ArrayJob is now a different size, and each task will get a new index location
	// which is different than their original location. To find the original index we construct an indexLookup array.
	// The subtask can find it's original index value in indexLookup[AWS_BATCH_JOB_ARRAY_INDEX]
	numCachedJobs := int64(0)
	if task.Metadata != nil && task.Metadata.Discoverable {
		// Start with the originally specified ArrayJob
		arrayJob, err := toArrayJob(task.GetCustom())
		if err != nil {
			return types.TaskStatusPermanentFailure(err), nil
		}

		cacheCtx, cancelCacheLookup := context.WithTimeout(ctx, aws.GetConfig().CatalogCacheTimeout.Duration)
		defer cancelCacheLookup()
		catalogCacheStatus, err := e.catalogCache.HandleCachedSubtasks(cacheCtx, arrayJob, taskCtx, task, e.dataStore)
		if err != nil {
			logger.Warningf(ctx, "Failed checking Catalog cache, skipping cache for task %v,  err %v", task.Id.Name, err)
		} else {
			numCachedJobs = int64(catalogCacheStatus.NumCached)
			arrayCachedStatus := &arraystatus.ArrayCachedStatus{
				TaskIdentifier:      task.Id,
				DiscoverableVersion: task.Metadata.DiscoveryVersion,
				CachedArrayJobs:     catalogCacheStatus.CachedSubTaskSet,
				NumCached:           uint(numCachedJobs),
			}
			customState.ArrayCachedStatus = arrayCachedStatus
			logger.Infof(ctx, "Found %d of %v subtasks are cached", numCachedJobs, arrayJob.Size)

			// early out if ALL jobs are cached since we do not have to kick off any ArrayJobs
			if numCachedJobs == arrayJob.Size {
				customStateMap, err := customState.AsMap(ctx)
				if err != nil {
					return types.TaskStatusUndefined, err
				}
				logger.Infof(ctx, "All %d subtasks are cached, skipping execution: %v", numCachedJobs, task.Id.Name)
				return types.TaskStatusSucceeded.WithState(customStateMap), nil
			}
		}
	}

	containerImage := getContainerImage(ctx, task)
	if len(containerImage) == 0 {
		logger.Infof(ctx, "Future task doesn't have an image specified. Failing.")
		return types.TaskStatusUndefined, errors.NewRequiredValueNotSet("image")
	}

	role := getRole(ctx, taskCtx.GetAnnotations())
	jobDefinition, err := e.ensureJobDefinition(ctx, definition.GetJobDefinitionSafeName(internal.ContainerImageRepository(containerImage)), containerImage, role)
	if err != nil {
		logger.Errorf(ctx, "Failed to get/create job definition: %v", err)
		return types.TaskStatusUndefined, errors.WrapError(err, errors.NewUnknownError("Failed to get/create job definition"))
	}

	arrayJob, batchInput, err := ArrayJobToBatchInput(ctx, taskCtx, jobDefinition, task, numCachedJobs)
	if err != nil {
		logger.Errorf(ctx, "Failed to build batch job input: %v", err)
		return types.TaskStatusPermanentFailure(err), nil
	}

	if arrayJob != nil {
		err := validateArrayJob(ctx, arrayJob)
		if err != nil {
			return types.TaskStatusPermanentFailure(err), nil
		}
	}

	logger.Infof(ctx, "Submitting job with Name: %v, Size: %v", *batchInput.JobName, arrayJob.Size)

	job, err := SubmitJob(ctx, batchInput)
	if err != nil {
		logger.Errorf(ctx, "Failed to submit batch job: %v", err)
		return types.TaskStatusUndefined, errors.WrapError(err, errors.NewUnknownError("Failed to submit batch job"))
	}

	if arrayJob != nil && arrayJob.Size > 1 {
		ArrayJob = ArrayJobSummary{}
		ArrayJob[batch.JobStatusSubmitted] = *batchInput.ArrayProperties.Size
	}

	// Set owner id used to re-enqueue owner object.
	Owner = taskCtx.GetOwnerID()

	if err = e.ensureBatchJobMonitored(ctx, job); err != nil {
		return types.TaskStatusUndefined, errors.WrapError(err, errors.NewUnknownError("Failed to monitor batch job"))
	}

	logger.Infof(ctx, "Successfully submitted Job [%v]", String())

	// TODO: Figure out how to add finalizers
	var arrayStatus *arraystatus.ArrayStatus
	if arrayJob.Size > 1 {
		arrayStatus = &arraystatus.ArrayStatus{
			Summary:  arraystatus.ArraySummary{},
			Detailed: newStatusCompactArray(uint(arrayJob.Size)),
		}
	}

	var arrayJobWrapper *arraystatus.ArrayJob
	if arrayJob != nil {
		arrayJobWrapper = &arraystatus.ArrayJob{
			ArrayJob: *arrayJob,
		}
	}

	customState.ArrayProperties = arrayJobWrapper
	customState.ArrayStatus = arrayStatus
	customState.JobID = ID

	customStateMap, err := customState.AsMap(ctx)

	if err != nil {
		logger.Errorf(ctx, "Failed to convert custom state to map with err %v", err)
		return types.TaskStatusPermanentFailure(err), nil
	}

	// @kumare/@katrogan
	//&v1alpha1.DynamicJobStatus{
	//	TaskCategory: task.Category.String(),
	//	TaskType:     task.Type,
	//	ExecutionID:  job.ID,
	//	Name:         job.Name,
	//	CustomState:  customState,
	//}
	return types.TaskStatusQueued.WithState(customStateMap), nil
}

func (e Executor) KillTask(ctx context.Context, taskCtx types.TaskContext, reason string) error {
	job, found, err := Get(ctx, taskCtx.GetTaskExecutionID().GetGeneratedName())
	if err == nil && found {
		return TerminateJob(ctx, ID, reason)
	}

	return err
}
