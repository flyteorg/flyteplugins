package hive

import (
	"context"
	"github.com/lyft/flyteplugins/go/tasks/errors"
	pluginMachinery "github.com/lyft/flyteplugins/go/tasks/pluginmachinery"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/plugins/hive/client"
	"github.com/lyft/flyteplugins/go/tasks/plugins/hive/config"
	"github.com/lyft/flytestdlib/logger"
	utils2 "github.com/lyft/flytestdlib/utils"
)

// This is the name of this plugin effectively. In Flyte plugin configuration, use this string to enable this plugin.
const quboleHiveExecutorId = "qubole-hive-executor"

// Version of the custom state this plugin stores.  Useful for backwards compatibility if you one day need to update
// the structure of the stored state
const pluginStateVersion = 0

const hiveTaskType = "hive" // This needs to match the type defined in Flytekit constants.py

type QuboleHiveExecutor struct {
	id              string
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
	outgoingState, transformError := HandleExecutionState(ctx, tCtx, incomingState, q.quboleClient, q.executionsCache)

	// Return if there was an error
	if transformError != nil {
		return core.UnknownTransition, transformError
	}

	// If no error, then infer the new Phase from the various states
	phaseInfo := MapExecutionStateToPhaseInfo(outgoingState)

	if err := tCtx.PluginStateWriter().Put(pluginStateVersion, outgoingState); err != nil {
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

	return Abort(ctx, tCtx, incomingState, q.quboleClient)
}

func (q QuboleHiveExecutor) Finalize(ctx context.Context, tCtx core.TaskExecutionContext) error {
	incomingState := ExecutionState{}
	if _, err := tCtx.PluginStateReader().Get(&incomingState); err != nil {
		logger.Errorf(ctx, "Plugin %s failed to unmarshal custom state in Finalize [%s] Err [%s]",
			q.id, tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), err)
		return errors.Wrapf(errors.CorruptedPluginState, err, "Failed to unmarshal custom state in Finalize")
	}

	return Finalize(ctx, tCtx, incomingState)
}

func (q QuboleHiveExecutor) GetProperties() core.PluginProperties {
	return core.PluginProperties{}
}

func QuboleHiveExecutorLoader(ctx context.Context, iCtx core.SetupContext) (core.Plugin, error) {
	q := NewQuboleHiveExecutor()
	q.quboleClient = client.NewQuboleClient()
	q.metrics = getQuboleHiveExecutorMetrics(iCtx.MetricsScope())

	executionsAutoRefreshCache, err := NewQuboleHiveExecutionsCache(ctx, q.quboleClient, iCtx.SecretManager(),
		config.GetQuboleConfig(), iCtx.MetricsScope().NewSubScope(hiveTaskType))
	if err != nil {
		logger.Errorf(ctx, "Failed to create AutoRefreshCache in QuboleHiveExecutor Setup")
		return q, err
	}
	q.executionsCache = executionsAutoRefreshCache
	q.executionsCache.Start(ctx)

	return q, nil
}

// type PluginLoader func(ctx context.Context, iCtx SetupContext) (Plugin, error)
func NewQuboleHiveExecutor() QuboleHiveExecutor {
	return QuboleHiveExecutor{
		id: quboleHiveExecutorId,
	}
}

func init() {
	pluginMachinery.PluginRegistry().RegisterCorePlugin(
		core.PluginEntry{
			ID:                  quboleHiveExecutorId,
			RegisteredTaskTypes: []core.TaskType{hiveTaskType},
			LoadPlugin:          QuboleHiveExecutorLoader,
			IsDefault:           false,
		})
}
