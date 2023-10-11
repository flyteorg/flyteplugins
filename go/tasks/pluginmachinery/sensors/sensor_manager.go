package sensors

import (
	"context"
	pluginsCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/utils"
)

// Sensor Manager implements the core.Plugin interface and provides the state management for building Sensors using a
// simplified interface
type SensorManager struct {
	ID          pluginsCore.TaskType
	plugin      Plugin
	rateLimiter utils.RateLimiter
}

func (s SensorManager) GetID() string {
	return s.ID
}

func (s SensorManager) GetProperties() pluginsCore.PluginProperties {
	return pluginsCore.PluginProperties{}
}

// Handle provides the necessary boilerplate to create simple Sensor Plugins
func (s SensorManager) Handle(ctx context.Context, tCtx pluginsCore.TaskExecutionContext) (pluginsCore.Transition, error) {
	if err := s.rateLimiter.Wait(ctx); err != nil {
		logger.Errorf(ctx, "Failed to wait on rateLimiter for sensor %s, error: %s", s.ID, err)
		return pluginsCore.Transition{}, err
	}
	p, err := s.plugin.Poke(ctx, tCtx)
	if err != nil {
		logger.Errorf(ctx, "Received error from sensor %s, err: %s", s.ID, err)
		return pluginsCore.Transition{}, err
	}
	if p.Phase == PhaseFailure {
		return pluginsCore.DoTransition(pluginsCore.PhaseInfoFailure("SensorFailed", p.Reason, nil)), nil
	}
	if p.Phase == PhaseReady {
		return pluginsCore.DoTransition(pluginsCore.PhaseInfoSuccess(nil)), nil
	}
	return pluginsCore.DoTransition(pluginsCore.PhaseInfoRunning(0, nil)), nil
}

func (s SensorManager) Abort(_ context.Context, _ pluginsCore.TaskExecutionContext) error {
	return nil
}

func (s SensorManager) Finalize(_ context.Context, _ pluginsCore.TaskExecutionContext) error {
	return nil
}

// Creates a new SensorManager for the given PluginEntry. The Plugin.Initialize method is also invoked during this
// construction
func NewSensorManager(ctx context.Context, iCtx pluginsCore.SetupContext, entry PluginEntry) (*SensorManager, error) {
	if err := entry.Plugin.Initialize(ctx, iCtx); err != nil {
		logger.Errorf(ctx, "Failed to initialize plugin %s, err: %s", entry.ID, err)
		return nil, err
	}
	name := entry.ID
	return &SensorManager{
		ID:          entry.ID,
		rateLimiter: utils.NewRateLimiter(name, entry.Properties.MaxRate, entry.Properties.BurstRate),
		plugin:      entry.Plugin,
	}, nil
}
