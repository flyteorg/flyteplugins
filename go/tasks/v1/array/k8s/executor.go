package k8s

import (
	"context"

	pluginMachinery "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/workqueue"
	k8sarray "github.com/lyft/flyteplugins/go/tasks/v1/array"
	"github.com/lyft/flyteplugins/go/tasks/v1/errors"
)

const executorName = "k8s-array-executor"
const arrayTaskType = "container_array"
const pluginStateVersion = 0

type Executor struct {
	catalogReader workqueue.IndexedWorkQueue
	catalogWriter workqueue.IndexedWorkQueue
	kubeClient    core.KubeClient
}

func NewExecutor() Executor {
	return Executor{}
}

func (e Executor) GetID() string {
	return executorName
}

func (Executor) GetProperties() core.PluginProperties {
	return core.PluginProperties{}
}

func (e Executor) Handle(ctx context.Context, tCtx core.TaskExecutionContext) (core.Transition, error) {
	pluginConfig := GetConfig()

	pluginState := &k8sarray.State{}
	if _, err := tCtx.PluginStateReader().Get(pluginState); err != nil {
		return core.UnknownTransition, errors.Wrapf(errors.CorruptedPluginState, err, "Failed to read unmarshal custom state")
	}

	var nextState *k8sarray.State
	var err error

	switch pluginState.GetPhase() {
	case k8sarray.PhaseStart:
		nextState, err = k8sarray.DetermineDiscoverability(ctx, tCtx, pluginState, e.catalogReader)

	case k8sarray.PhaseLaunch:
		nextState, err = LaunchSubTasks(ctx, tCtx, e.kubeClient, pluginConfig, pluginState)

	case k8sarray.PhaseCheckingSubTaskExecutions:
		nextState, err = CheckSubTasksState(ctx, tCtx, e.kubeClient, pluginConfig, pluginState)

	case k8sarray.PhaseWriteToDiscovery:
		nextState, err = k8sarray.WriteToDiscovery(ctx, tCtx, e.catalogWriter, pluginState)

	default:
		nextState = pluginState
		err = nil
	}
	if err != nil {
		return core.UnknownTransition, err
	}

	if err := tCtx.PluginStateWriter().Put(pluginStateVersion, nextState); err != nil {
		return core.UnknownTransition, err
	}

	// Determine transition information from the state
	phaseInfo := k8sarray.MapArrayStateToPluginPhase(ctx, *nextState)
	return core.DoTransitionType(core.TransitionTypeBestEffort, phaseInfo), nil
}

func (Executor) Abort(ctx context.Context, tCtx core.TaskExecutionContext) error {
	panic("implement me")
}

func (Executor) Finalize(ctx context.Context, tCtx core.TaskExecutionContext) error {
	panic("implement me")
}

func init() {
	pluginMachinery.PluginRegistry().RegisterCorePlugin(
		core.PluginEntry{
			ID:                  executorName,
			RegisteredTaskTypes: []core.TaskType{arrayTaskType},
			LoadPlugin:          GetNewExecutorPlugin,
			IsDefault:           false,
		})
}

func GetNewExecutorPlugin(ctx context.Context, iCtx core.SetupContext) (core.Plugin, error) {
	return NewExecutor(), nil
}
