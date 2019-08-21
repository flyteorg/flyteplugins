package qubole_single

import (
	"context"
	"fmt"
	idlCore "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"
	"github.com/lyft/flyteplugins/go/tasks/v1/qubole_single/config"
	"github.com/lyft/flyteplugins/go/tasks/v1/utils"
	"github.com/lyft/flytestdlib/contextutils"
	"github.com/lyft/flytestdlib/promutils/labeled"
	"strconv"

	"github.com/lyft/flyteplugins/go/tasks/v1/errors"
	"github.com/lyft/flyteplugins/go/tasks/v1/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/v1/qubole_single/client"
	"github.com/lyft/flytestdlib/logger"
	utils2 "github.com/lyft/flytestdlib/utils"
	"time"
)

// This is the name of this plugin effectively. In Flyte plugin configuration, use this string to enable this plugin.
const quboleHiveExecutorId = "qubole_collection-hive-executor"

// NB: This is a different string than the old one. The old one will now only be used for multiple query Hive tasks.
const hiveTaskType = "hive-task" // This needs to match the type defined in Flytekit constants.py

type QuboleHiveExecutor struct {
	id              string
	secretsManager  SecretsManager
	metrics         QuboleHiveExecutorMetrics
	quboleClient    client.QuboleClient
	executionsCache utils2.AutoRefreshCache
}

func (q QuboleHiveExecutor) GetID() string {
	return q.id
}

func (q QuboleHiveExecutor) Handle(ctx context.Context, tCtx core.TaskExecutionContext) (core.Transition, error) {
	incomingState := ExecutionState{}

	// We assume here that the first time this function is called, the custom state we get back is
	// the zero-value of our struct. todo
	if _, err := tCtx.PluginStateReader().Get(&incomingState); err != nil {
		logger.Errorf(ctx, "Plugin %s failed to unmarshal custom state when handling [%s] [%s]",
			q.id, tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), err)
		return core.UnknownTransition, errors.Wrapf(errors.CustomStateFailure, err,
			"Failed to unmarshal custom state in Handle")
	}

	// Do what needs to be done, and give this function everything it needs to do its job properly
	outgoingState, transformError := incomingState.Handle(ctx, tCtx, q.quboleClient, q.secretsManager, q.executionsCache)

	// Return if there was an error
	if transformError != nil {
		return core.UnknownTransition, transformError
	}

	// If no error, then infer the new Phase from the various states
	phaseInfo := MapExecutionStateToPhaseInfo(outgoingState)

	// todo: write new state

	return core.DoTransitionType(core.TransitionTypeBestEffort, phaseInfo), nil
}

func (q QuboleHiveExecutor) Abort(ctx context.Context, tCtx core.TaskExecutionContext) error {
	// Shouldn't have to do anything, everything we have to do will be taken care of in Finalize
	return nil
}

func (q QuboleHiveExecutor) Finalize(ctx context.Context, tCtx core.TaskExecutionContext) error {
	incomingState := ExecutionState{}
	if _, err := tCtx.PluginStateReader().Get(&incomingState); err != nil {
		logger.Errorf(ctx, "Plugin %s failed to unmarshal custom state in Finalize [%s] Err [%s]",
			q.id, tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), err)
		return errors.Wrapf(errors.CustomStateFailure, err, "Failed to unmarshal custom state in Finalize")
	}

	return incomingState.Finalize(ctx, tCtx, q.quboleClient, q.secretsManager)

}

func (q *QuboleHiveExecutor) Setup(ctx context.Context, iCtx core.SetupContext) error {

	q.quboleClient = client.NewQuboleClient()
	q.secretsManager = NewSecretsManager()
	_, err := q.secretsManager.GetToken()
	if err != nil {
		logger.Errorf(ctx, "Failed to read secret in QuboleHiveExecutor Setup")
		return err
	}
	q.metrics = getQuboleHiveExecutorMetrics(iCtx.MetricsScope())

	executionsAutoRefreshCache, err := NewQuboleHiveExecutionsCache(ctx, q.quboleClient, q.secretsManager,
		config.GetQuboleConfig().LruCacheSize, iCtx.MetricsScope().NewSubScope(hiveTaskType))
	if err != nil {
		logger.Errorf(ctx, "Failed to create AutoRefreshCache in QuboleHiveExecutor Setup")
		return err
	}
	q.executionsCache = executionsAutoRefreshCache
	q.executionsCache.Start(ctx)

	return nil

}

func NewQuboleHiveExecutor() QuboleHiveExecutor {
	return QuboleHiveExecutor{
		id: quboleHiveExecutorId,
	}
}

type ExecutionPhase int

const (
	PhaseNotStarted ExecutionPhase = iota
	PhaseQueued      // resource manager token gotten
	PhaseSubmitted   // Sent off to Qubole

	// todo: remove
	PhaseQuerySucceeded
	PhaseQueryFailed

	// todo: remove
	// This is bad and shouldn't happen. It's here to encapsulate unknown statuses returned from the Qubole API itself
	PhaseUnknown
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
	case PhaseUnknown:
		return "PhaseUnknown"
	}
	return "Bad Qubole execution phase"
}

type ExecutionState struct {
	Phase ExecutionPhase

	// This ID is the cache key and so will need to be unique across all objects in the cache (it will probably be
	// unique across all of Flyte) and needs to be deterministic.
	// This will also be used as the allocation token for now.
	// todo: remove
	UniqueAllocationTokenId string `json:"unique_work_cache_key"`

	// This will store the command ID from Qubole
	CommandId string `json:"command_id,omitempty"`

	// This number keeps track of the number of failures within the sync function. Without this, what happens in
	// the sync function is entirely opaque. Note that this field is completely orthogonal to Flyte system/node/task
	// level retries, just errors from hitting the Qubole API, inside the sync loop
	SyncQuboleApiFailures int `json:"sync_qubole_api_failures,omitempty"`

	// In kicking off the Qubole command, this is the number of
	QuboleApiCreationFailures int `json:"qubole_api_creation_failures,omitempty"`
}

// This is the main state iteration
// todo: remove receiver
func (e ExecutionState) Handle(ctx context.Context, tCtx core.TaskExecutionContext, quboleClient client.QuboleClient,
	secretsManager SecretsManager, executionsCache utils2.AutoRefreshCache) (ExecutionState, error) {

	var transformError error
	var outgoingState ExecutionState

	switch e.Phase {
	case PhaseNotStarted:
		outgoingState, transformError = e.GetAllocationToken(ctx, tCtx)

	case PhaseQueued:
		outgoingState, transformError = e.KickOffQuery(ctx, tCtx, quboleClient, secretsManager, executionsCache)

	case PhaseSubmitted:
		outgoingState, transformError = e.MonitorQuery(ctx, tCtx, executionsCache)

	// todo: Think about returning transition instead of the new state

	case PhaseQuerySucceeded:
		outgoingState = e.Copy()
		transformError = nil

	case PhaseQueryFailed:
		outgoingState = e.Copy()
		transformError = nil
	}

	return outgoingState, transformError
}

func MapExecutionStateToPhaseInfo(state ExecutionState) core.PhaseInfo {
	var phaseInfo core.PhaseInfo
	t := time.Now()

	switch state.Phase {
	case PhaseNotStarted:
		phaseInfo = core.PhaseInfoNotReady(t, core.DefaultPhaseVersion, "Haven't received allocation token")
	case PhaseQueued:
		// TODO: Turn into config
		if state.QuboleApiCreationFailures > 5 {
			phaseInfo = core.PhaseInfoFailure("QuboleFailure", "Too many creation attempts", nil)
		} else {
			phaseInfo = core.PhaseInfoQueued(t, uint8(state.QuboleApiCreationFailures), "Waiting for Qubole launch")
		}
	case PhaseSubmitted:
		phaseInfo = core.PhaseInfoRunning(core.DefaultPhaseVersion, state.ConstructTaskInfo())

	case PhaseQuerySucceeded:
		phaseInfo = core.PhaseInfoSuccess(state.ConstructTaskInfo())

	case PhaseQueryFailed:
		phaseInfo = core.PhaseInfoFailure(errors.DownstreamSystemError, "Query failed", state.ConstructTaskInfo())
	}

	return phaseInfo
}

func (e ExecutionState) ConstructTaskLog() *idlCore.TaskLog {
	return &idlCore.TaskLog{
		Name:          fmt.Sprintf("Status: %s [%s]", e.Phase, e.CommandId),
		MessageFormat: idlCore.TaskLog_UNKNOWN,
		Uri:           fmt.Sprintf(client.QuboleLogLinkFormat, e.CommandId),
	}
}

func (e ExecutionState) ConstructTaskInfo() *core.TaskInfo {
	logs := make([]*idlCore.TaskLog, 0, 1)
	if e.CommandId != "" {
		logs = append(logs, e.ConstructTaskLog())
		return &core.TaskInfo{
			Logs: logs,
		}
	}

	return nil
}

func (e ExecutionState) GetAllocationToken(ctx context.Context, tCtx core.TaskExecutionContext) (ExecutionState, error) {
	newState := ExecutionState{}
	uniqueAllocationTokenId := tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()
	allocationStatus, err := tCtx.ResourceManager().AllocateResource(ctx, tCtx.TaskExecutionMetadata().GetNamespace(), uniqueAllocationTokenId)
	if err != nil {
		logger.Errorf(ctx, "Resource manager failed for TaskExecId [%s] token [%s]",
			tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetID(), uniqueAllocationTokenId)
		return newState, errors.Wrapf(errors.ResourceManagerFailure, err, "Error requesting allocation token %s", uniqueAllocationTokenId)
	}
	logger.Infof(ctx, "Allocation result for [%s] is [%s]", uniqueAllocationTokenId, allocationStatus)

	if allocationStatus == core.AllocationStatusGranted {
		newState.Phase = PhaseQueued
	} else if allocationStatus == core.AllocationStatusExhausted {
		newState.Phase = PhaseNotStarted
	} else if allocationStatus == core.AllocationStatusNamespaceQuotaExceeded {
		newState.Phase = PhaseNotStarted
	} else {
		return newState, errors.Errorf(errors.ResourceManagerFailure, "Got bad allocation result [%s] for token [%s]",
			allocationStatus, uniqueAllocationTokenId)
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

func (e ExecutionState) KickOffQuery(ctx context.Context, tCtx core.TaskExecutionContext, quboleClient client.QuboleClient,
	secretsManager SecretsManager, cache utils2.AutoRefreshCache) (ExecutionState, error) {

	outgoingState := e.Copy()

	apiKey, err := secretsManager.GetToken()
	if err != nil {
		return outgoingState, errors.Wrapf(errors.RuntimeFailure, err, "Failed to read token from secrets manager")
	}

	query, cluster, tags, timeoutSec, err := GetQueryInfo(ctx, tCtx)
	if err != nil {
		return outgoingState, err
	}

	cmdDetails, err := quboleClient.ExecuteHiveCommand(ctx, query, timeoutSec,
		cluster, apiKey, tags)
	if err != nil {
		// If we failed, we'll keep the NotStarted state
		logger.Warnf(ctx, "Error creating Qubole query for %s", e.UniqueAllocationTokenId)
		outgoingState.QuboleApiCreationFailures = outgoingState.QuboleApiCreationFailures + 1
	} else {
		// If we succeed, then store the command id returned from Qubole, and update our state. Also, add to the
		// AutoRefreshCache so we start getting updates.
		commandId := strconv.FormatInt(cmdDetails.ID, 10)
		logger.Infof(ctx, "Created Qubole ID [%s] for token %s", commandId, e.UniqueAllocationTokenId)
		outgoingState.CommandId = commandId
		outgoingState.Phase = PhaseSubmitted
		_, err := cache.GetOrCreate(outgoingState)
		if err != nil {
			// This means that our cache has fundamentally broken... return a system error
			logger.Errorf(ctx, "Cache failed to GetOrCreate for execution [%s] cache key [%s], owner [%s]",
				tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetID(), outgoingState.UniqueAllocationTokenId,
				tCtx.TaskExecutionMetadata().GetOwnerReference())
			return outgoingState, err
		}
	}

	return outgoingState, nil
}

func (e ExecutionState) MonitorQuery(ctx context.Context, tCtx core.TaskExecutionContext, cache utils2.AutoRefreshCache) (
	ExecutionState, error) {

	incomingCopy := e.Copy()

	cachedItem, err := cache.GetOrCreate(e)
	if err != nil {
		// This means that our cache has fundamentally broken... return a system error
		logger.Errorf(ctx, "Cache is broken on execution [%s] cache key [%s], owner [%s]",
			tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetID(), e.UniqueAllocationTokenId,
			tCtx.TaskExecutionMetadata().GetOwnerReference())
		return incomingCopy, errors.Wrapf(errors.CacheFailed, err, "Error when GetOrCreate while monitoring")
	}

	cachedExecutionState, ok := cachedItem.(ExecutionState)
	if !ok {
		logger.Errorf(ctx, "Error casting cache object into ExecutionState")
		return incomingCopy, errors.Errorf(errors.CacheFailed, "Failed to cast [%v]", cachedItem)
	}

	// TODO: Add a couple of debug lines here - did it change or did it not?

	// If there were updates made to the state, we'll have picked them up automatically. Nothing more to do.
	return cachedExecutionState, nil
}

func (e ExecutionState) Finalize(ctx context.Context, tCtx core.TaskExecutionContext, qubole client.QuboleClient,
	manager SecretsManager) error {
	// Cancel Qubole query if non-terminal state
	if !e.InTerminalState() && e.CommandId != "" {
		key, err := manager.GetToken()
		if err != nil {
			logger.Errorf(ctx, "Error reading token in Finalize [%s]", err)
			return err
		}
		err = qubole.KillCommand(ctx, e.CommandId, key)
		if err != nil {
			// Should this return an error?  If we do, then we won't deallocate from FRM.
			logger.Errorf(ctx, "Error terminating Qubole command in Finalize [%s]", err)
		}
	}
	// Release allocation token
	err := tCtx.ResourceManager().ReleaseResource(ctx, tCtx.TaskExecutionMetadata().GetNamespace(), e.UniqueAllocationTokenId)
	if err != nil {
		logger.Errorf(ctx, "Error releasing allocation token [%s] in Finalize [%s]", e.UniqueAllocationTokenId, err)
		return err
	}
	return nil
}

// This function is here to comply with the AutoRefreshCache interface
func (e ExecutionState) ID() string {
	return e.UniqueAllocationTokenId
}

func (e ExecutionState) Copy() ExecutionState {
	return ExecutionState(e)
}

func (e ExecutionState) InTerminalState() bool {
	return e.Phase == PhaseQuerySucceeded || e.Phase == PhaseQueryFailed
}

func init() {
	labeled.SetMetricKeys(contextutils.ProjectKey, contextutils.DomainKey, contextutils.WorkflowIDKey, contextutils.TaskIDKey)
}
