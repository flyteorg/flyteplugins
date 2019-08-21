package qubole_collection

import (
	"context"
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
const quboleHiveExecutorId = "qubole-hive-collection-executor"

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

func (q QuboleCollectionHiveExecutor) Handle(ctx context.Context, tCtx core.TaskExecutionContext) (core.Transition, error) {
	currentState := CollectionExecutionState{}

	// We assume here that the first time this function is called, the custom state we get back is whatever we passed in,
	// namely the zero-value of our struct.
	if _, err := tCtx.PluginStateReader().Get(&currentState); err != nil {
		logger.Errorf(ctx, "Plugin %s failed to unmarshal custom state when handling [%s] [%s]",
			q.id, tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), err)
		return core.UnknownTransition, errors.Wrapf(errors.CorruptedPluginState, err,
			"Failed to unmarshal custom state in Handle")
	}

	var transformError error
	var newState CollectionExecutionState

	switch currentState.Phase {
	case PhaseInitializing:
		newState, transformError = InitializeStates(ctx, tCtx)
	case PhaseAttemptAllQueries:
		newState, transformError = DoEverything(ctx, tCtx, currentState, q.quboleClient, q.secretsManager, q.executionsCache)

	// TODO: This is an optimization - in cases where we've already launched all the queries, we don't have to do read the input
	//       file to get all the queries. In cases where we have lots of queries, this will save a bit of time.
	case PhaseAllQueriesLaunched:
		newState, transformError = DoEverything(ctx, tCtx, currentState, q.quboleClient, q.secretsManager, q.executionsCache)

	case PhaseAllQueriesTerminated:
		newState = currentState
		transformError = nil
	}

	// Return if there was an error
	if transformError != nil {
		return core.UnknownTransition, transformError
	}

	// If no error, then infer the new Phase from the various states
	phaseInfo := MapCollectionExecutionToPhaseInfo(newState)

	return core.DoTransitionType(core.TransitionTypeBestEffort, phaseInfo), nil
}

func (q QuboleCollectionHiveExecutor) Abort(ctx context.Context, tCtx core.TaskExecutionContext) error {
	incomingState := CollectionExecutionState{}
	if _, err := tCtx.PluginStateReader().Get(&incomingState); err != nil {
		logger.Errorf(ctx, "Plugin %s failed to unmarshal custom state in Finalize [%s] Err [%s]",
			q.id, tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), err)
		return errors.Wrapf(errors.CorruptedPluginState, err, "Failed to unmarshal custom state in Finalize")
	}

	return Abort(ctx, tCtx, incomingState, q.quboleClient, q.secretsManager)
}

func (q QuboleCollectionHiveExecutor) Finalize(ctx context.Context, tCtx core.TaskExecutionContext) error {
	incomingState := CollectionExecutionState{}
	if _, err := tCtx.PluginStateReader().Get(&incomingState); err != nil {
		logger.Errorf(ctx, "Plugin %s failed to unmarshal custom state in Finalize [%s] Err [%s]",
			q.id, tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), err)
		return errors.Wrapf(errors.CorruptedPluginState, err, "Failed to unmarshal custom state in Finalize")
	}

	return Finalize(ctx, tCtx, incomingState)

}

func (q *QuboleCollectionHiveExecutor) Setup(ctx context.Context, iCtx core.SetupContext) error {
	q.quboleClient = client.NewQuboleClient()
	q.secretsManager = NewSecretsManager()
	_, err := q.secretsManager.GetToken()
	if err != nil {
		logger.Errorf(ctx, "Failed to read secret in QuboleHiveExecutor Setup")
		return err
	}

	executionsAutoRefreshCache, err := qubole_single.NewQuboleHiveExecutionsCache(ctx, q.quboleClient, q.secretsManager,
		config.GetQuboleConfig().LruCacheSize, iCtx.MetricsScope().NewSubScope(hiveTaskType))
	if err != nil {
		logger.Errorf(ctx, "Failed to create AutoRefreshCache in QuboleCollectionHiveExecutor Setup")
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

func init() {
	labeled.SetMetricKeys(contextutils.ProjectKey, contextutils.DomainKey, contextutils.WorkflowIDKey, contextutils.TaskIDKey)
}
