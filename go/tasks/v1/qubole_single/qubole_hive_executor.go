package qubole_single

import (
	"context"
	"github.com/lyft/flyteplugins/go/tasks/v1/errors"
	"github.com/lyft/flyteplugins/go/tasks/v1/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/v1/qubole_single/client"
	"github.com/lyft/flyteplugins/go/tasks/v1/qubole_single/config"
	"github.com/lyft/flytestdlib/contextutils"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/promutils/labeled"
	utils2 "github.com/lyft/flytestdlib/utils"
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

	// We assume here that the first time this function is called, the custom state we get back is whatever we passed in,
	// namely the zero-value of our struct.
	if _, err := tCtx.PluginStateReader().Get(&incomingState); err != nil {
		logger.Errorf(ctx, "Plugin %s failed to unmarshal custom state when handling [%s] [%s]",
			q.id, tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), err)
		return core.UnknownTransition, errors.Wrapf(errors.CorruptedPluginState, err,
			"Failed to unmarshal custom state in Handle")
	}

	// Do what needs to be done, and give this function everything it needs to do its job properly
	// TODO: Play around with making this return a transition directly. How will that pattern affect the multi-Qubole plugin
	outgoingState, transformError := HandleExecutionState(ctx, tCtx, incomingState, q.quboleClient, q.secretsManager, q.executionsCache)

	// Return if there was an error
	if transformError != nil {
		return core.UnknownTransition, transformError
	}

	// If no error, then infer the new Phase from the various states
	phaseInfo := MapExecutionStateToPhaseInfo(outgoingState)

	if err := tCtx.PluginStateWriter().Put(0, outgoingState); err != nil {
		return core.UnknownTransition, err
	}

	return core.DoTransitionType(core.TransitionTypeBestEffort, phaseInfo), nil
}

func (q QuboleHiveExecutor) Abort(ctx context.Context, tCtx core.TaskExecutionContext) error {
	incomingState := ExecutionState{}
	if _, err := tCtx.PluginStateReader().Get(&incomingState); err != nil {
		logger.Errorf(ctx, "Plugin %s failed to unmarshal custom state in Finalize [%s] Err [%s]",
			q.id, tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), err)
		return errors.Wrapf(errors.CorruptedPluginState, err, "Failed to unmarshal custom state in Finalize")
	}

	return Abort(ctx, tCtx, incomingState, q.quboleClient, q.secretsManager)
}

func (q QuboleHiveExecutor) Finalize(ctx context.Context, tCtx core.TaskExecutionContext) error {
	incomingState := ExecutionState{}
	if _, err := tCtx.PluginStateReader().Get(&incomingState); err != nil {
		logger.Errorf(ctx, "Plugin %s failed to unmarshal custom state in Finalize [%s] Err [%s]",
			q.id, tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), err)
		return errors.Wrapf(errors.CorruptedPluginState, err, "Failed to unmarshal custom state in Finalize")
	}

	return Finalize(ctx, tCtx, incomingState, q.quboleClient, q.secretsManager)
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

func init() {
	labeled.SetMetricKeys(contextutils.ProjectKey, contextutils.DomainKey, contextutils.WorkflowIDKey, contextutils.TaskIDKey)
}
