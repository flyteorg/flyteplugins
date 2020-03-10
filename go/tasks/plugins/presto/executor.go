package presto

import (
	"context"
	"github.com/lyft/flyteplugins/go/tasks/plugins/command"
	"github.com/lyft/flyteplugins/go/tasks/plugins/presto/client"

	"github.com/lyft/flytestdlib/cache"

	"github.com/lyft/flyteplugins/go/tasks/errors"
	pluginMachinery "github.com/lyft/flyteplugins/go/tasks/pluginmachinery"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/plugins/presto/config"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/promutils"
)

// This is the name of this plugin effectively. In Flyte plugin configuration, use this string to enable this plugin.
const prestoExecutorId = "presto-executor"

// Version of the custom state this plugin stores.  Useful for backwards compatibility if you one day need to update
// the structure of the stored state
const pluginStateVersion = 0

const prestoTaskType = "presto" // This needs to match the type defined in Flytekit constants.py

type PrestoExecutor struct {
	id              string
	metrics         PrestoExecutorMetrics
	prestoClient    command.CommandClient
	executionsCache cache.AutoRefresh
	cfg             *config.Config
}

func (p PrestoExecutor) GetID() string {
	return p.id
}

func (p PrestoExecutor) Handle(ctx context.Context, tCtx core.TaskExecutionContext) (core.Transition, error) {
	incomingState := ExecutionState{}

	// We assume here that the first time this function is called, the custom state we get back is whatever we passed in,
	// namely the zero-value of our struct.
	if _, err := tCtx.PluginStateReader().Get(&incomingState); err != nil {
		logger.Errorf(ctx, "Plugin %s failed to unmarshal custom state when handling [%s] [%s]",
			p.id, tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), err)
		return core.UnknownTransition, errors.Wrapf(errors.CorruptedPluginState, err,
			"Failed to unmarshal custom state in Handle")
	}

	// Do what needs to be done, and give this function everything it needs to do its job properly
	outgoingState, transformError := HandleExecutionState(ctx, tCtx, incomingState, p.prestoClient, p.executionsCache, p.metrics)

	// Return if there was an error
	if transformError != nil {
		return core.UnknownTransition, transformError
	}

	// If no error, then infer the new Phase from the various states
	phaseInfo := MapExecutionStateToPhaseInfo(outgoingState)

	if err := tCtx.PluginStateWriter().Put(pluginStateVersion, outgoingState); err != nil {
		return core.UnknownTransition, err
	}

	return core.DoTransitionType(core.TransitionTypeBarrier, phaseInfo), nil
}

func (p PrestoExecutor) Abort(ctx context.Context, tCtx core.TaskExecutionContext) error {
	incomingState := ExecutionState{}
	if _, err := tCtx.PluginStateReader().Get(&incomingState); err != nil {
		logger.Errorf(ctx, "Plugin %s failed to unmarshal custom state in Finalize [%s] Err [%s]",
			p.id, tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), err)
		return errors.Wrapf(errors.CorruptedPluginState, err, "Failed to unmarshal custom state in Finalize")
	}

	return Abort(ctx, incomingState, p.prestoClient)
}

func (p PrestoExecutor) Finalize(ctx context.Context, tCtx core.TaskExecutionContext) error {
	incomingState := ExecutionState{}
	if _, err := tCtx.PluginStateReader().Get(&incomingState); err != nil {
		logger.Errorf(ctx, "Plugin %s failed to unmarshal custom state in Finalize [%s] Err [%s]",
			p.id, tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), err)
		return errors.Wrapf(errors.CorruptedPluginState, err, "Failed to unmarshal custom state in Finalize")
	}

	return Finalize(ctx, tCtx, incomingState)
}

func (p PrestoExecutor) GetProperties() core.PluginProperties {
	return core.PluginProperties{}
}

func PrestoExecutorLoader(ctx context.Context, iCtx core.SetupContext) (core.Plugin, error) {
	cfg := config.GetPrestoConfig()
	return InitializePrestoExecutor(ctx, iCtx, cfg, BuildResourceConfig(cfg), client.NewPrestoClient(cfg))
}

func BuildResourceConfig(cfg *config.Config) map[string]int {
	resourceConfig := make(map[string]int, len(cfg.RoutingGroupConfigs))

	for _, routingGroupCfg := range cfg.RoutingGroupConfigs {
		resourceConfig[routingGroupCfg.Name] = routingGroupCfg.Limit
	}
	return resourceConfig
}

func InitializePrestoExecutor(
	ctx context.Context,
	iCtx core.SetupContext,
	cfg *config.Config,
	resourceConfig map[string]int,
	prestoClient command.CommandClient) (core.Plugin, error) {
	logger.Infof(ctx, "Initializing a Presto executor with a resource config [%v]", resourceConfig)
	q, err := NewPrestoExecutor(ctx, cfg, prestoClient, iCtx.MetricsScope())
	if err != nil {
		logger.Errorf(ctx, "Failed to create a new PrestoExecutor due to error: [%v]", err)
		return nil, err
	}

	for routingGroupName, routingGroupLimit := range resourceConfig {
		logger.Infof(ctx, "Registering resource quota for cluster [%v]", routingGroupName)
		if err := iCtx.ResourceRegistrar().RegisterResourceQuota(ctx, core.ResourceNamespace(routingGroupName), routingGroupLimit); err != nil {
			logger.Errorf(ctx, "Resource quota registration for [%v] failed due to error [%v]", routingGroupName, err)
			return nil, err
		}
	}

	return q, nil
}

func NewPrestoExecutor(
	ctx context.Context,
	cfg *config.Config,
	prestoClient command.CommandClient,
	scope promutils.Scope) (PrestoExecutor, error) {
	executionsAutoRefreshCache, err := NewPrestoExecutionsCache(ctx, prestoClient, cfg, scope.NewSubScope(prestoTaskType))
	if err != nil {
		logger.Errorf(ctx, "Failed to create AutoRefreshCache in PrestoExecutor Setup. Error: %v", err)
		return PrestoExecutor{}, err
	}

	err = executionsAutoRefreshCache.Start(ctx)
	if err != nil {
		logger.Errorf(ctx, "Failed to start AutoRefreshCache. Error: %v", err)
	}

	return PrestoExecutor{
		id:              prestoExecutorId,
		cfg:             cfg,
		metrics:         getPrestoExecutorMetrics(scope),
		prestoClient:    prestoClient,
		executionsCache: executionsAutoRefreshCache,
	}, nil
}

func init() {
	pluginMachinery.PluginRegistry().RegisterCorePlugin(
		core.PluginEntry{
			ID:                  prestoExecutorId,
			RegisteredTaskTypes: []core.TaskType{prestoTaskType},
			LoadPlugin:          PrestoExecutorLoader,
			IsDefault:           false,
		})
}
