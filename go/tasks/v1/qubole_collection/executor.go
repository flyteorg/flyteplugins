package qubole_collection

import (
	"context"
	"fmt"
	idlCore "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteplugins/go/tasks/v1/errors"
	"github.com/lyft/flyteplugins/go/tasks/v1/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/v1/qubole_single"
	"github.com/lyft/flyteplugins/go/tasks/v1/qubole_single/client"
	"github.com/lyft/flyteplugins/go/tasks/v1/qubole_single/config"
	"github.com/lyft/flytestdlib/contextutils"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/promutils/labeled"
	utils2 "github.com/lyft/flytestdlib/utils"
)

// This is the name of this plugin effectively. In Flyte plugin configuration, use this string to enable this plugin.
const quboleHiveExecutorId = "qubole_collection-hive-collection-executor"

// NB: This is the original string. It's different than the single-qubole_collection task type string.
const hiveTaskType = "hive" // This needs to match the type defined in Flytekit constants.py

type QuboleCollectionHiveExecutor struct {
	id              string
	secretsManager  SecretsManager
	metrics         HiveExecutorMetrics
	quboleClient    client.QuboleClient
	executionsCache utils2.AutoRefreshCache
}

func (q QuboleCollectionHiveExecutor) GetID() string {
	return q.id
}

type CollectionExecutionPhase int

const (
	PhaseDoingEverything CollectionExecutionPhase = iota
	PhaseEverythingLaunched
	PhaseAllQueriesTerminated
)

func (q QuboleCollectionHiveExecutor) Handle(ctx context.Context, tCtx core.TaskExecutionContext) (core.Transition, error) {
	incomingState := CollectionExecutionState{}

	// We assume here that the chicken-and-egg problem has been solved for us. If this is the first time this function
	// is called, the custom state we get back is the zero-value of our struct.
	if _, err := tCtx.PluginStateReader().Get(&incomingState); err != nil {
		logger.Errorf(ctx, "Plugin %s failed to unmarshal custom state when handling [%s] [%s]",
			q.id, tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), err)
		return core.UnknownTransition, errors.Wrapf(errors.CustomStateFailure, err,
			"Failed to unmarshal custom state in Handle")
	}

	var transformError error
	var outgoingState CollectionExecutionState

	switch incomingState.Phase {
	case PhaseDoingEverything:
		outgoingState, transformError = incomingState.DoEverything(ctx, tCtx, q.quboleClient, q.secretsManager, q.executionsCache)

	// TODO: This is an optimization - in cases where we've already launched all the queries, we don't have to do read the input
	//       file to get all the queries. In cases where we have lots of queries, this will save a bit of time.
	case PhaseEverythingLaunched:
		outgoingState, transformError = incomingState.DoEverything(ctx, tCtx, q.quboleClient, q.secretsManager, q.executionsCache)

	case PhaseAllQueriesTerminated:
		outgoingState = incomingState.Copy()
		transformError = nil
	}

	// Return if there was an error
	if transformError != nil {
		return core.UnknownTransition, transformError
	}

	// If no error, then infer the new Phase from the various states
	phaseInfo := MapCollectionExecutionToPhaseInfo(outgoingState)

	return core.DoTransitionType(core.TransitionTypeBestEffort, phaseInfo), nil
}

func (q QuboleCollectionHiveExecutor) Abort(ctx context.Context, tCtx core.TaskExecutionContext) error {
	// Shouldn't have to do anything, everything we have to do will be taken care of in Finalize
	return nil
}

func (q QuboleCollectionHiveExecutor) Finalize(ctx context.Context, tCtx core.TaskExecutionContext) error {
	incomingState := ExecutionState{}
	if _, err := tCtx.PluginStateReader().Get(&incomingState); err != nil {
		logger.Errorf(ctx, "Plugin %s failed to unmarshal custom state in Finalize [%s] Err [%s]",
			q.id, tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), err)
		return errors.Wrapf(errors.CustomStateFailure, err, "Failed to unmarshal custom state in Finalize")
	}

	return incomingState.Finalize(ctx, tCtx, q.quboleClient, q.secretsManager)

}

func (q *QuboleCollectionHiveExecutor) Setup(ctx context.Context, iCtx core.SetupContext) error {

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

func NewQuboleCollectionHiveExecutor() QuboleCollectionHiveExecutor {
	return QuboleCollectionHiveExecutor{
		id: quboleHiveExecutorId,
	}
}

type CollectionExecutionState struct {
	Phase CollectionExecutionPhase

	states []qubole_single.ExecutionState
}

// This function does everything - launches, gets allocation tokens, does everything.
func (c CollectionExecutionState) DoEverything(ctx context.Context, tCtx core.TaskExecutionContext, quboleClient client.QuboleClient,
	secretsManager SecretsManager, cache utils2.AutoRefreshCache) (CollectionExecutionState, error) {

	newStates := make([]qubole_single.ExecutionState, len(c.states))

	for _, x := range c.states {
		// Handle each little thing
		newState, err := x.Handle(ctx, tCtx, quboleClient, secretsManager, cache)
		if err != nil {
			return c.Copy(), err
		}
		newStates = append(newStates, newState)
	}

	// Each of the little ExecutionState phases have now been updated.  It's time to update the larger state
	newExecutionPhase := DetermineCollectionPhaseFrom(newStates)
	return CollectionExecutionState{
		Phase:  newExecutionPhase,
		states: newStates,
	}, nil
}

func DetermineCollectionPhaseFrom(states []qubole_single.ExecutionState) CollectionExecutionPhase {
	for _, x := range states {
		// If any are Queued or NotStarted, then we continue to do everything
		if x.Phase < qubole_single.PhaseSubmitted {
			return PhaseDoingEverything
		}
	}

	for _, x := range states {
		// If any are Queued or NotStarted, then we continue to do everything
		if !x.InTerminalState() {
			return PhaseEverythingLaunched
		}
	}

	return PhaseAllQueriesTerminated
}

func MapCollectionExecutionToPhaseInfo(state CollectionExecutionState) core.PhaseInfo {
	var phaseInfo core.PhaseInfo

	taskInfo := state.ConstructTaskInfo()

	switch state.Phase {
	case PhaseDoingEverything, PhaseEverythingLaunched:
		if taskInfo != nil {
			phaseInfo = core.PhaseInfoRunning(uint8(len(phaseInfo.Info().Logs)), taskInfo)
		} else {
			phaseInfo = core.PhaseInfoRunning(core.DefaultPhaseVersion, phaseInfo)
		}

	case PhaseAllQueriesTerminated:
		notSucceeded := state.countNotSucceeded()
		if notSucceeded == 0 {
			phaseInfo = core.PhaseInfoSuccess(taskInfo)
		} else {
			phaseInfo = core.PhaseInfoRetryableFailure(errors.DownstreamSystemError,
				fmt.Sprintf("Errors in Qubole queries [%d/%d] queries failed", notSucceeded, len(state.states)), taskInfo)
		}

	}

	return phaseInfo
}

func (c CollectionExecutionState) countNotSucceeded() int {
	var notSuccess = 0
	for _, x := range c.states {
		if x.Phase != qubole_single.PhaseQuerySucceeded {
			notSuccess++
		}
	}
	return notSuccess
}

func (c CollectionExecutionState) Copy() CollectionExecutionState {
	newStates := make([]qubole_single.ExecutionState, len(c.states))
	for _, x := range c.states {
		newStates = append(newStates, x.Copy())
	}
	return CollectionExecutionState{Phase: c.Phase, states: newStates}
}

func (c CollectionExecutionState) ConstructTaskInfo() *core.TaskInfo {
	logs := make([]*idlCore.TaskLog, 0, 1)
	for _, x := range c.states {
		if x.CommandId != "" {
			logs = append(logs, x.ConstructTaskLog())
		}
	}

	if len(logs) > 0 {
		return &core.TaskInfo{
			Logs: logs,
		}
	}
	return nil
}

func init() {
	labeled.SetMetricKeys(contextutils.ProjectKey, contextutils.DomainKey, contextutils.WorkflowIDKey, contextutils.TaskIDKey)
}
