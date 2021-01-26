package webapi

import (
	"context"
	"time"

	"github.com/lyft/flytestdlib/cache"
	"github.com/lyft/flytestdlib/logger"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/webapi"
)

func launch(ctx context.Context, p webapi.AsyncPlugin, tCtx core.TaskExecutionContext, cache cache.AutoRefresh,
	state *State) (newState *State, phaseInfo core.PhaseInfo, err error) {
	rMeta, r, err := p.Create(ctx, tCtx)
	if err != nil {
		logger.Errorf(ctx, "Failed to create resource. Error: %v", err)
		return nil, core.PhaseInfo{}, err
	}

	// If we succeed, then store the created resource name, and update our state. Also, add to the
	// AutoRefreshCache so we start getting updates.
	logger.Infof(ctx, "Created Resource Name [%s] and Meta [%v]", tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), rMeta)
	if r != nil {
		phase, err := p.Status(ctx, newPluginContext(rMeta, r, "", tCtx))
		if err != nil {
			logger.Errorf(ctx, "Failed to check resource status. Error: %v", err)
			return nil, core.PhaseInfo{}, err
		}

		if phase.Phase().IsTerminal() {
			logger.Infof(ctx, "Resource has already terminated ID:[%s], Phase:[%s]",
				tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), phase.Phase())
			return state, phase, nil
		}
	}

	state.ResourceMeta = rMeta
	state.Phase = PhaseResourcesCreated
	cacheItem := CacheItem{
		State: *state,
	}

	// The first time we put it in the cache, we know it won't have succeeded so we don't need to look at it
	_, err = cache.GetOrCreate(tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), cacheItem)
	if err != nil {
		logger.Errorf(ctx, "Failed to add item to cache. Error: %v", err)
		return nil, core.PhaseInfo{}, err
	}

	return state, core.PhaseInfoQueued(time.Now(), 2, "launched"), nil
}
