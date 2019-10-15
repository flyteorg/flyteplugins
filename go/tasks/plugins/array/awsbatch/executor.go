package awsbatch

import (
	"context"

	arrayCore "github.com/lyft/flyteplugins/go/tasks/plugins/array/core"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery"
	"github.com/lyft/flyteplugins/go/tasks/plugins/array/awsbatch/definition"

	"github.com/lyft/flyteplugins/go/tasks/plugins/array"
	batchConfig "github.com/lyft/flyteplugins/go/tasks/plugins/array/awsbatch/config"

	"github.com/lyft/flytestdlib/logger"

	"github.com/lyft/flyteplugins/go/tasks/aws"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/utils"

	"github.com/lyft/flyteplugins/go/tasks/errors"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
)

const (
	executorName              = "aws_array"
	defaultPluginStateVersion = 0
	arrayTaskType             = "container_array"
)

type Executor struct {
	jobStore           *JobStore
	jobDefinitionCache definition.Cache

	outputAssembler array.OutputAssembler
	errorAssembler  array.OutputAssembler
}

func (e Executor) GetID() string {
	return executorName
}

func (e Executor) GetProperties() core.PluginProperties {
	return core.PluginProperties{}
}

func (e Executor) Handle(ctx context.Context, tCtx core.TaskExecutionContext) (core.Transition, error) {
	pluginConfig := batchConfig.GetConfig()

	pluginState := &State{}
	if _, err := tCtx.PluginStateReader().Get(pluginState); err != nil {
		return core.UnknownTransition, errors.Wrapf(errors.CorruptedPluginState, err, "Failed to read unmarshal custom state")
	}

	if pluginState.State == nil {
		pluginState.State = &arrayCore.State{}
	}

	var err error

	switch p, _ := pluginState.GetPhase(); p {
	case arrayCore.PhaseStart:
		pluginState.State, err = array.DetermineDiscoverability(ctx, tCtx, pluginState.State)

	case arrayCore.PhasePreLaunch:
		pluginState, err = EnsureJobDefinition(ctx, tCtx, pluginConfig, e.jobStore.Client, e.jobDefinitionCache, pluginState)

	case arrayCore.PhaseLaunch:
		pluginState, err = LaunchSubTasks(ctx, tCtx, e.jobStore, pluginConfig, pluginState)

	case arrayCore.PhaseCheckingSubTaskExecutions:
		pluginState, err = CheckSubTasksState(ctx, tCtx.TaskExecutionMetadata(), e.jobStore, pluginConfig, pluginState)

	case arrayCore.PhaseAssembleFinalOutput:
		pluginState.State, err = array.AssembleFinalOutputs(ctx, e.outputAssembler, tCtx, pluginState.State)

	case arrayCore.PhaseWriteToDiscovery:
		pluginState.State, err = array.WriteToDiscovery(ctx, tCtx, pluginState.State)

	case arrayCore.PhaseAssembleFinalError:
		pluginState.State, err = array.AssembleFinalOutputs(ctx, e.errorAssembler, tCtx, pluginState.State)

	default:
		err = nil
	}

	if err != nil {
		return core.UnknownTransition, err
	}

	if err := tCtx.PluginStateWriter().Put(defaultPluginStateVersion, pluginState); err != nil {
		return core.UnknownTransition, err
	}

	// Determine transition information from the state
	phaseInfo := arrayCore.MapArrayStateToPluginPhase(ctx, pluginState.State)
	return core.DoTransitionType(core.TransitionTypeBarrier, phaseInfo), nil
}

func (e Executor) Abort(ctx context.Context, tCtx core.TaskExecutionContext) error {
	//TODO: implement
	return nil
}

func (e Executor) Finalize(ctx context.Context, tCtx core.TaskExecutionContext) error {
	//TODO: implement
	return nil
}

func NewExecutor(ctx context.Context, awsClient aws.Client, cfg *batchConfig.Config,
	enqueueOwner core.EnqueueOwner, scope promutils.Scope) (Executor, error) {

	getRateLimiter := utils.NewRateLimiter("getRateLimiter", float64(cfg.GetRateLimiter.Rate),
		cfg.GetRateLimiter.Burst)
	defaultRateLimiter := utils.NewRateLimiter("defaultRateLimiter", float64(cfg.DefaultRateLimiter.Rate),
		cfg.DefaultRateLimiter.Burst)
	batchClient := NewBatchClient(awsClient, getRateLimiter, defaultRateLimiter)
	jobStore, err := NewJobStore(ctx, batchClient, cfg.ResyncPeriod.Duration, cfg.JobStoreConfig, EventHandler{
		Updated: func(ctx context.Context, event Event) {
			err := enqueueOwner(event.NewJob.OwnerReference)
			if err != nil {
				logger.Warnf(ctx, "Failed to enqueue owner [%v] of job [%v]. Error: %v", event.NewJob.OwnerReference, event.NewJob.ID)
			}
		},
	}, scope)

	if err != nil {
		return Executor{}, err
	}

	outputAssembler, err := array.NewOutputAssembler(cfg.OutputAssembler, scope.NewSubScope("output"))
	if err != nil {
		return Executor{}, err
	}

	errorAssembler, err := array.NewErrorAssembler(cfg.MaxErrorStringLength, cfg.ErrorAssembler,
		scope.NewSubScope("error"))
	if err != nil {
		return Executor{}, err
	}

	return Executor{
		jobStore:           &jobStore,
		jobDefinitionCache: definition.NewCache(cfg.JobDefCacheSize),
		outputAssembler:    outputAssembler,
		errorAssembler:     errorAssembler,
	}, nil
}

func (e Executor) Start(ctx context.Context) error {
	if err := e.jobStore.Start(ctx); err != nil {
		return err
	}

	if err := e.outputAssembler.Start(ctx); err != nil {
		return err
	}

	if err := e.errorAssembler.Start(ctx); err != nil {
		return err
	}

	return nil
}

func init() {
	pluginmachinery.PluginRegistry().RegisterCorePlugin(
		core.PluginEntry{
			ID:                  executorName,
			RegisteredTaskTypes: []core.TaskType{arrayTaskType},
			LoadPlugin:          createNewExecutorPlugin,
			IsDefault:           false,
		})
}

func createNewExecutorPlugin(ctx context.Context, iCtx core.SetupContext) (core.Plugin, error) {
	awsClient, err := aws.GetClient()
	if err != nil {
		return nil, err
	}

	exec, err := NewExecutor(ctx, awsClient, batchConfig.GetConfig(), iCtx.EnqueueOwner(), iCtx.MetricsScope().NewSubScope(executorName))
	if err != nil {
		return nil, err
	}

	if err = exec.Start(ctx); err != nil {
		return nil, err
	}

	return exec, nil
}
