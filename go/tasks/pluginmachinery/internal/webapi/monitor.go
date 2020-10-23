package webapi

import (
	"context"

	"github.com/lyft/flyteplugins/go/tasks/errors"
	"github.com/lyft/flytestdlib/cache"
	"github.com/lyft/flytestdlib/logger"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
)

func monitor(ctx context.Context, tCtx core.TaskExecutionContext, cache cache.AutoRefresh, state *State) (
	newState *State, phaseInfo core.PhaseInfo, err error) {
	incomingState := State{}
	if _, err := tCtx.PluginStateReader().Get(&incomingState); err != nil {
		logger.Errorf(ctx, "Failed to unmarshal custom state when handling [%s]. Error: %v",
			tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), err)

		return nil, core.PhaseInfo{},
			errors.Wrapf(errors.CorruptedPluginState, err,
				"Failed to unmarshal custom state in Handle")
	}

	cacheItem := CacheItem{
		State: *state,
	}

	item, err := cache.GetOrCreate(
		tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), cacheItem)
	if err != nil {
		return nil, core.PhaseInfo{}, err
	}

	cacheItem, ok := item.(CacheItem)
	if !ok {
		logger.Errorf(ctx, "Error casting cache object into ExecutionState")
		return nil, core.PhaseInfo{}, errors.Errorf(
			errors.CacheFailed, "Failed to cast [%v]", cacheItem)
	}

	// If there were updates made to the state, we'll have picked them up automatically. Nothing more to do.
	return &cacheItem.State, cacheItem.LatestPhaseInfo, nil
}
