package remote

import (
	"context"
	"time"

	"github.com/lyft/flytestdlib/cache"
	"github.com/lyft/flytestdlib/logger"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/remote"
)

func launch(ctx context.Context, p remote.Plugin, tCtx core.TaskExecutionContext, cache cache.AutoRefresh,
	state *State) (newState *State, phaseInfo core.PhaseInfo, err error) {
	r, err := p.Create(ctx, tCtx)
	if err != nil {
		logger.Errorf(ctx, "Failed to create resource. Error: %v", err)
		return nil, core.PhaseInfo{}, err
	}

	// If we succeed, then store the created resource name, and update our state. Also, add to the
	// AutoRefreshCache so we start getting updates.
	logger.Infof(ctx, "Created Resource Name [%s]", r.Name)
	cacheItem := CacheItem{
		State:      *state,
		Identifier: r.Name,
	}

	// The first time we put it in the cache, we know it won't have succeeded so we don't need to look at it
	_, err = cache.GetOrCreate(r.Name, cacheItem)
	if err != nil {
		logger.Errorf(ctx, "Failed to add item to cache. Error: %v", err)
		return nil, core.PhaseInfo{}, err
	}

	return state, core.PhaseInfoQueued(time.Now(), 1, "launched"), nil
}
