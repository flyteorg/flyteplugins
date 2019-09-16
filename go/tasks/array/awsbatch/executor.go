package awsbatch

import (
	"context"

	"github.com/lyft/flyteplugins/go/tasks/array"
	"github.com/lyft/flyteplugins/go/tasks/array/awsbatch/config"
	"github.com/lyft/flyteplugins/go/tasks/errors"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
)

const (
	executorName              = "aws-array"
	defaultPluginStateVersion = 0
)

type Executor2 struct {
	batchClient Client
}

func (e Executor2) GetID() string {
	return executorName
}

func (e Executor2) GetProperties() core.PluginProperties {
	return core.PluginProperties{}
}

func (e Executor2) Handle(ctx context.Context, tCtx core.TaskExecutionContext) (core.Transition, error) {
	pluginConfig := config.GetConfig()

	pluginState := &array.State{}
	if _, err := tCtx.PluginStateReader().Get(pluginState); err != nil {
		return core.UnknownTransition, errors.Wrapf(errors.CorruptedPluginState, err, "Failed to read unmarshal custom state")
	}

	var nextState *array.State
	var err error

	switch p, _ := pluginState.GetPhase(); p {
	case array.PhaseStart:
		nextState, err = array.DetermineDiscoverability(ctx, tCtx, pluginState)

	case array.PhaseLaunch:
		nextState, err = LaunchSubTasks(ctx, tCtx, e.batchClient, pluginConfig, pluginState)

	case array.PhaseCheckingSubTaskExecutions:
		nextState, err = CheckSubTasksState(ctx, tCtx, e.batchClient, pluginConfig, pluginState)

	case array.PhaseWriteToDiscovery:
		nextState, err = array.WriteToDiscovery(ctx, tCtx, pluginState)

	default:
		nextState = pluginState
		err = nil
	}
	if err != nil {
		return core.UnknownTransition, err
	}

	if err := tCtx.PluginStateWriter().Put(defaultPluginStateVersion, nextState); err != nil {
		return core.UnknownTransition, err
	}

	// Determine transition information from the state
	phaseInfo := array.MapArrayStateToPluginPhase(ctx, *nextState)
	return core.DoTransitionType(core.TransitionTypeBestEffort, phaseInfo), nil
}

func (e Executor2) Abort(ctx context.Context, tCtx core.TaskExecutionContext) error {
	panic("implement me")
}

func (e Executor2) Finalize(ctx context.Context, tCtx core.TaskExecutionContext) error {
	panic("implement me")
}
