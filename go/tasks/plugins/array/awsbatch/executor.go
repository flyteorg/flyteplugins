package awsbatch

import (
	"context"

	core2 "github.com/lyft/flyteplugins/go/tasks/plugins/array/core"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery"
	"github.com/lyft/flyteplugins/go/tasks/plugins/array/awsbatch/definition"

	"github.com/lyft/flyteplugins/go/tasks/plugins/array"
	config2 "github.com/lyft/flyteplugins/go/tasks/plugins/array/awsbatch/config"

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
	pluginConfig := config2.GetConfig()

	pluginState := &State{}
	if _, err := tCtx.PluginStateReader().Get(pluginState); err != nil {
		return core.UnknownTransition, errors.Wrapf(errors.CorruptedPluginState, err, "Failed to read unmarshal custom state")
	}

	var nextParentState core2.State
	var err error

	switch p, _ := pluginState.GetPhase(); p {
	case core2.PhaseStart:
		nextParentState, err = array.DetermineDiscoverability(ctx, tCtx, pluginState.State)
		pluginState.State = nextParentState

	case core2.PhasePreLaunch:
		pluginState, err = EnsureJobDefinition(ctx, tCtx, pluginConfig, e.jobStore.Client, e.jobDefinitionCache, pluginState)

	case core2.PhaseLaunch:
		pluginState, err = LaunchSubTasks(ctx, tCtx, e.jobStore, pluginConfig, pluginState)

	case core2.PhaseCheckingSubTaskExecutions:
		pluginState, err = CheckSubTasksState(ctx, tCtx.TaskExecutionMetadata(), e.jobStore, pluginConfig, pluginState)

	case core2.PhaseAssembleFinalOutput:
		nextParentState, err = array.AssembleFinalOutputs(ctx, e.outputAssembler, tCtx, pluginState)
		pluginState.State = nextParentState

	case core2.PhaseWriteToDiscovery:
		nextParentState, err = array.WriteToDiscovery(ctx, tCtx, pluginState.State)
		pluginState.State = nextParentState

	case core2.PhaseAssembleFinalError:
		nextParentState, err = array.AssembleFinalOutputs(ctx, e.errorAssembler, tCtx, pluginState)
		pluginState.State = nextParentState

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
	phaseInfo := core2.MapArrayStateToPluginPhase(ctx, pluginState.State)
	return core.DoTransitionType(core.TransitionTypeBestEffort, phaseInfo), nil
}

func (e Executor) Abort(ctx context.Context, tCtx core.TaskExecutionContext) error {
	//TODO: implement
	return nil
}

func (e Executor) Finalize(ctx context.Context, tCtx core.TaskExecutionContext) error {
	//TODO: implement
	return nil
}

func NewExecutor(ctx context.Context, awsClient aws.Client, cfg *config2.Config,
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
		jobStore:        &jobStore,
		outputAssembler: outputAssembler,
		errorAssembler:  errorAssembler,
	}, nil
}

func init() {
	pluginmachinery.PluginRegistry().RegisterCorePlugin(
		core.PluginEntry{
			ID:                  executorName,
			RegisteredTaskTypes: []core.TaskType{arrayTaskType},
			LoadPlugin:          GetNewExecutorPlugin,
			IsDefault:           false,
		})
}

func GetNewExecutorPlugin(ctx context.Context, iCtx core.SetupContext) (core.Plugin, error) {
	awsClient, err := aws.GetClient()
	if err != nil {
		return nil, err
	}

	return NewExecutor(ctx, awsClient, config2.GetConfig(), iCtx.EnqueueOwner(), iCtx.MetricsScope().NewSubScope(executorName))
}
