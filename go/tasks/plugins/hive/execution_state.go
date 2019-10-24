package hive

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/lyft/flytestdlib/cache"

	idlCore "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/utils"
	"github.com/lyft/flyteplugins/go/tasks/plugins/hive/config"

	"github.com/lyft/flyteplugins/go/tasks/errors"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/plugins/hive/client"
	"github.com/lyft/flytestdlib/logger"
)

type ExecutionPhase int

const (
	PhaseNotStarted ExecutionPhase = iota
	PhaseQueued                    // resource manager token gotten
	PhaseSubmitted                 // Sent off to Qubole

	PhaseQuerySucceeded
	PhaseQueryFailed
)

const (
	QuboleResourceNamespace core.ResourceNamespace = "qubole"
)

func (p ExecutionPhase) String() string {
	switch p {
	case PhaseNotStarted:
		return "PhaseNotStarted"
	case PhaseQueued:
		return "PhaseQueued"
	case PhaseSubmitted:
		return "PhaseSubmitted"
	case PhaseQuerySucceeded:
		return "PhaseQuerySucceeded"
	case PhaseQueryFailed:
		return "PhaseQueryFailed"
	}
	return "Bad Qubole execution phase"
}

type ExecutionState struct {
	Phase ExecutionPhase

	// This will store the command ID from Qubole
	CommandId string `json:"command_id,omitempty"`

	// This number keeps track of the number of failures within the sync function. Without this, what happens in
	// the sync function is entirely opaque. Note that this field is completely orthogonal to Flyte system/node/task
	// level retries, just errors from hitting the Qubole API, inside the sync loop
	SyncFailureCount int `json:"sync_failure_count,omitempty"`

	// In kicking off the Qubole command, this is the number of failures
	CreationFailureCount int `json:"creation_failure_count,omitempty"`
}

// This is the main state iteration
func HandleExecutionState(ctx context.Context, tCtx core.TaskExecutionContext, currentState ExecutionState, quboleClient client.QuboleClient,
	executionsCache cache.AutoRefresh, cfg *config.Config) (ExecutionState, error) {

	var transformError error
	var newState ExecutionState

	switch currentState.Phase {
	case PhaseNotStarted:
		newState, transformError = GetAllocationToken(ctx, tCtx)

	case PhaseQueued:
		newState, transformError = KickOffQuery(ctx, tCtx, currentState, quboleClient, executionsCache, cfg)

	case PhaseSubmitted:
		newState, transformError = MonitorQuery(ctx, tCtx, currentState, executionsCache)

	case PhaseQuerySucceeded:
		newState = currentState
		transformError = nil

	case PhaseQueryFailed:
		newState = currentState
		transformError = nil
	}

	return newState, transformError
}

func MapExecutionStateToPhaseInfo(state ExecutionState, quboleClient client.QuboleClient) core.PhaseInfo {
	var phaseInfo core.PhaseInfo
	t := time.Now()

	switch state.Phase {
	case PhaseNotStarted:
		phaseInfo = core.PhaseInfoNotReady(t, core.DefaultPhaseVersion, "Haven't received allocation token")
	case PhaseQueued:
		// TODO: Turn into config
		if state.CreationFailureCount > 5 {
			phaseInfo = core.PhaseInfoRetryableFailure("QuboleFailure", "Too many creation attempts", nil)
		} else {
			phaseInfo = core.PhaseInfoQueued(t, uint32(state.CreationFailureCount), "Waiting for Qubole launch")
		}
	case PhaseSubmitted:
		phaseInfo = core.PhaseInfoRunning(core.DefaultPhaseVersion, ConstructTaskInfo(state, quboleClient))

	case PhaseQuerySucceeded:
		phaseInfo = core.PhaseInfoSuccess(ConstructTaskInfo(state, quboleClient))

	case PhaseQueryFailed:
		phaseInfo = core.PhaseInfoFailure(errors.DownstreamSystemError, "Query failed", ConstructTaskInfo(state, quboleClient))
	}

	return phaseInfo
}

func ConstructTaskLog(e ExecutionState, quboleClient client.QuboleClient) *idlCore.TaskLog {
	l, err := quboleClient.GetLogLinkPath(context.TODO(), e.CommandId)
	uri := ""
	if err == nil {
		uri = l.String()
	} else {
		logger.Errorf(context.TODO(), "failed to create log link, error: %s", err)
	}
	return &idlCore.TaskLog{
		Name:          fmt.Sprintf("Status: %s [%s]", e.Phase, e.CommandId),
		MessageFormat: idlCore.TaskLog_UNKNOWN,
		Uri:           uri,
	}
}

func ConstructTaskInfo(e ExecutionState, quboleClient client.QuboleClient) *core.TaskInfo {
	logs := make([]*idlCore.TaskLog, 0, 1)
	t := time.Now()
	if e.CommandId != "" {
		logs = append(logs, ConstructTaskLog(e, quboleClient))
		return &core.TaskInfo{
			Logs:       logs,
			OccurredAt: &t,
		}
	}
	return nil
}

func GetAllocationToken(ctx context.Context, tCtx core.TaskExecutionContext) (ExecutionState, error) {
	newState := ExecutionState{}
	uniqueId := tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()
	allocationStatus, err := tCtx.ResourceManager().AllocateResource(ctx, QuboleResourceNamespace, uniqueId)
	if err != nil {
		logger.Errorf(ctx, "Resource manager failed for TaskExecId [%s] token [%s]. error %s",
			tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetID(), uniqueId, err)
		return newState, errors.Wrapf(errors.ResourceManagerFailure, err, "Error requesting allocation token %s", uniqueId)
	}
	logger.Infof(ctx, "Allocation result for [%s] is [%s]", uniqueId, allocationStatus)

	if allocationStatus == core.AllocationStatusGranted {
		newState.Phase = PhaseQueued
	} else if allocationStatus == core.AllocationStatusExhausted {
		newState.Phase = PhaseNotStarted
	} else if allocationStatus == core.AllocationStatusNamespaceQuotaExceeded {
		newState.Phase = PhaseNotStarted
	} else {
		return newState, errors.Errorf(errors.ResourceManagerFailure, "Got bad allocation result [%s] for token [%s]",
			allocationStatus, uniqueId)
	}

	return newState, nil
}

// This function is the link between the output written by the SDK, and the execution side. It extracts the query
// out of the task template.
func GetQueryInfo(ctx context.Context, tCtx core.TaskExecutionContext) (
	query string, cluster string, tags []string, timeoutSec uint32, err error) {

	taskTemplate, err := tCtx.TaskReader().Read(ctx)
	if err != nil {
		return "", "", []string{}, 0, err
	}

	hiveJob := plugins.QuboleHiveJob{}
	err = utils.UnmarshalStruct(taskTemplate.GetCustom(), &hiveJob)
	if err != nil {
		return "", "", []string{}, 0, err
	}

	query = hiveJob.Query.GetQuery()
	cluster = hiveJob.ClusterLabel
	tags = hiveJob.Tags
	timeoutSec = hiveJob.Query.TimeoutSec

	return
}

func KickOffQuery(ctx context.Context, tCtx core.TaskExecutionContext, currentState ExecutionState, quboleClient client.QuboleClient,
	cache cache.AutoRefresh, cfg *config.Config) (ExecutionState, error) {

	uniqueId := tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()
	apiKey, err := tCtx.SecretManager().Get(ctx, cfg.TokenKey)
	if err != nil {
		return currentState, errors.Wrapf(errors.RuntimeFailure, err, "Failed to read token from secrets manager")
	}

	query, cluster, tags, timeoutSec, err := GetQueryInfo(ctx, tCtx)
	if err != nil {
		return currentState, err
	}

	cmdDetails, err := quboleClient.ExecuteHiveCommand(ctx, query, timeoutSec,
		cluster, apiKey, tags)
	if err != nil {
		// If we failed, we'll keep the NotStarted state
		currentState.CreationFailureCount = currentState.CreationFailureCount + 1
		logger.Warnf(ctx, "Error creating Qubole query for %s, failure counts %d. Error: %s", uniqueId, currentState.CreationFailureCount, err)
	} else {
		// If we succeed, then store the command id returned from Qubole, and update our state. Also, add to the
		// AutoRefreshCache so we start getting updates.
		commandId := strconv.FormatInt(cmdDetails.ID, 10)
		logger.Infof(ctx, "Created Qubole ID [%s] for token %s", commandId, uniqueId)
		currentState.CommandId = commandId
		currentState.Phase = PhaseSubmitted
		executionStateCacheItem := ExecutionStateCacheItem{
			ExecutionState: currentState,
			Id:             uniqueId,
		}

		// The first time we put it in the cache, we know it won't have succeeded so we don't need to look at it
		_, err := cache.GetOrCreate(uniqueId, executionStateCacheItem)
		if err != nil {
			// This means that our cache has fundamentally broken... return a system error
			logger.Errorf(ctx, "Cache failed to GetOrCreate for execution [%s] cache key [%s], owner [%s]. Error %s",
				tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetID(), uniqueId,
				tCtx.TaskExecutionMetadata().GetOwnerReference(), err)
			return currentState, err
		}
	}

	return currentState, nil
}

func MonitorQuery(ctx context.Context, tCtx core.TaskExecutionContext, currentState ExecutionState, cache cache.AutoRefresh) (
	ExecutionState, error) {

	uniqueId := tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()
	executionStateCacheItem := ExecutionStateCacheItem{
		ExecutionState: currentState,
		Id:             uniqueId,
	}

	cachedItem, err := cache.GetOrCreate(uniqueId, executionStateCacheItem)
	if err != nil {
		// This means that our cache has fundamentally broken... return a system error
		logger.Errorf(ctx, "Cache is broken on execution [%s] cache key [%s], owner [%s]. Error %s",
			tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetID(), uniqueId,
			tCtx.TaskExecutionMetadata().GetOwnerReference(), err)
		return currentState, errors.Wrapf(errors.CacheFailed, err, "Error when GetOrCreate while monitoring")
	}

	cachedExecutionState, ok := cachedItem.(ExecutionStateCacheItem)
	if !ok {
		logger.Errorf(ctx, "Error casting cache object into ExecutionState")
		return currentState, errors.Errorf(errors.CacheFailed, "Failed to cast [%v]", cachedItem)
	}

	// TODO: Add a couple of debug lines here - did it change or did it not?

	// If there were updates made to the state, we'll have picked them up automatically. Nothing more to do.
	return cachedExecutionState.ExecutionState, nil
}

func Abort(ctx context.Context, tCtx core.TaskExecutionContext, currentState ExecutionState, qubole client.QuboleClient, apiKey string) error {
	// Cancel Qubole query if non-terminal state
	if !InTerminalState(currentState) && currentState.CommandId != "" {
		err := qubole.KillCommand(ctx, currentState.CommandId, apiKey)
		if err != nil {
			logger.Errorf(ctx, "Error terminating Qubole command in Finalize [%s]", err)
			return err
		}
	}
	return nil
}

func Finalize(ctx context.Context, tCtx core.TaskExecutionContext, _ ExecutionState) error {
	// Release allocation token
	uniqueId := tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()
	err := tCtx.ResourceManager().ReleaseResource(ctx, QuboleResourceNamespace, uniqueId)
	if err != nil {
		logger.Errorf(ctx, "Error releasing allocation token [%s] in Finalize [%s]", uniqueId, err)
		return err
	}
	return nil
}

func InTerminalState(e ExecutionState) bool {
	return e.Phase == PhaseQuerySucceeded || e.Phase == PhaseQueryFailed
}

func IsNotYetSubmitted(e ExecutionState) bool {
	if e.Phase == PhaseNotStarted || e.Phase == PhaseQueued {
		return true
	}
	return false
}
