package remote

import (
	"context"

	"github.com/lyft/flytestdlib/cache"

	"github.com/lyft/flyteplugins/go/tasks/errors"
	"github.com/lyft/flytestdlib/logger"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/remote"
)

const (
	pluginStateVersion = 1
)

type CorePlugin struct {
	id    string
	p     remote.Plugin
	cache cache.AutoRefresh
}

func (c CorePlugin) GetID() string {
	return c.id
}

func (c CorePlugin) GetProperties() core.PluginProperties {
	return core.PluginProperties{}
}

func (c CorePlugin) Handle(ctx context.Context, tCtx core.TaskExecutionContext) (core.Transition, error) {
	incomingState := State{}

	// We assume here that the first time this function is called, the custom state we get back is whatever we passed in,
	// namely the zero-value of our struct.
	if _, err := tCtx.PluginStateReader().Get(&incomingState); err != nil {
		logger.Errorf(ctx, "Plugin %s failed to unmarshal custom state when handling [%s]. Error: %v",
			c.GetID(), tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), err)

		return core.UnknownTransition, errors.Wrapf(errors.CorruptedPluginState, err,
			"Failed to unmarshal custom state in Handle")
	}

	var nextState *State
	var phaseInfo core.PhaseInfo
	var err error
	switch incomingState.Phase {
	case PhaseNotStarted:
		nextState, phaseInfo, err = allocateToken(ctx, c.p, tCtx, &incomingState)
	case PhaseAllocationTokenAcquired:
		nextState, phaseInfo, err = launch(ctx, c.p, tCtx, c.cache, &incomingState)
	case PhaseResourcesCreated:
		nextState, phaseInfo, err = monitor(ctx, c.p, tCtx, c.cache, &incomingState)
	}

	if err != nil {
		return core.UnknownTransition, err
	}

	if err := tCtx.PluginStateWriter().Put(pluginStateVersion, nextState); err != nil {
		return core.UnknownTransition, err
	}

	return core.DoTransitionType(core.TransitionTypeBarrier, phaseInfo), nil
}

func (c CorePlugin) Abort(ctx context.Context, tCtx core.TaskExecutionContext) error {
	incomingState := State{}
	if _, err := tCtx.PluginStateReader().Get(&incomingState); err != nil {
		logger.Errorf(ctx, "Plugin %s failed to unmarshal custom state when handling [%s]. Error: %v",
			c.GetID(), tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), err)

		return errors.Wrapf(errors.CorruptedPluginState, err,
			"Failed to unmarshal custom state in Handle")
	}

	logger.Infof(ctx, "Attempting to abort resource [%v].", incomingState.ResourceKey.Name)

	err := c.p.Delete(ctx, incomingState.ResourceKey)
	if err != nil {
		logger.Errorf(ctx, "Failed to abort some resources [%v]. Error: %v", incomingState.ResourceKey.Name, err)
		return err
	}

	return nil
}

func (c CorePlugin) Finalize(ctx context.Context, tCtx core.TaskExecutionContext) error {
	incomingState := State{}
	if _, err := tCtx.PluginStateReader().Get(&incomingState); err != nil {
		logger.Errorf(ctx, "Plugin %s failed to unmarshal custom state when handling [%s]. Error: %v",
			c.GetID(), tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), err)

		return errors.Wrapf(errors.CorruptedPluginState, err,
			"Failed to unmarshal custom state in Handle")
	}

	logger.Infof(ctx, "Attempting to finalize resource [%v].", incomingState.ResourceKey.Name)
	return releaseToken(ctx, c.p, tCtx, &incomingState)
}

func CreateRemotePlugin(pluginEntry remote.PluginEntry) core.PluginEntry {
	return core.PluginEntry{
		ID:                  pluginEntry.ID,
		RegisteredTaskTypes: pluginEntry.SupportedTaskTypes,
		LoadPlugin: func(ctx context.Context, iCtx core.SetupContext) (
			core.Plugin, error) {
			p, err := pluginEntry.PluginLoader(ctx, iCtx)
			if err != nil {
				return nil, err
			}

			if quotas := p.GetPluginProperties().ResourceQuotas; len(quotas) > 0 {
				for ns, quota := range quotas {
					err := iCtx.ResourceRegistrar().RegisterResourceQuota(ctx, ns, quota)
					if err != nil {
						return nil, err
					}
				}
			}

			resourceCache, err := NewResourceCache(ctx, pluginEntry.ID, p, p.GetPluginProperties().Caching,
				iCtx.MetricsScope().NewSubScope("cache"))

			if err != nil {
				return nil, err
			}

			return CorePlugin{
				id:    pluginEntry.ID,
				p:     p,
				cache: resourceCache,
			}, nil
		},
	}
}
