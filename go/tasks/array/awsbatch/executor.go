package awsbatch

import (
	"context"
	"time"

	"github.com/lyft/flytestdlib/logger"

	"github.com/lyft/flyteplugins/go/tasks/aws"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/utils"

	"github.com/lyft/flyteplugins/go/tasks/array"
	"github.com/lyft/flyteplugins/go/tasks/array/awsbatch/config"
	"github.com/lyft/flyteplugins/go/tasks/errors"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
)

const (
	executorName              = "aws-array"
	defaultPluginStateVersion = 0
	arrayTaskType             = "container_array"
)

type Executor struct {
	jobStore *JobStore
}

func (e Executor) GetID() string {
	return executorName
}

func (e Executor) GetProperties() core.PluginProperties {
	return core.PluginProperties{}
}

func (e Executor) Handle(ctx context.Context, tCtx core.TaskExecutionContext) (core.Transition, error) {
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
		nextState, err = LaunchSubTasks(ctx, tCtx, e.jobStore, pluginConfig, pluginState)

	case array.PhaseCheckingSubTaskExecutions:
		nextState, err = CheckSubTasksState(ctx, tCtx, e.jobStore, pluginConfig, pluginState)

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

func (e Executor) Abort(ctx context.Context, tCtx core.TaskExecutionContext) error {
	//TODO: implement
	return nil
}

func (e Executor) Finalize(ctx context.Context, tCtx core.TaskExecutionContext) error {
	//TODO: implement
	return nil
}

func NewExecutor(ctx context.Context, awsClient aws.Client, resyncPeriod time.Duration, cfg *config.Config,
	enqueueOwner core.EnqueueOwner, scope promutils.Scope) (Executor, error) {

	getRateLimiter := utils.NewRateLimiter("getRateLimiter", float64(cfg.GetRateLimiter.Rate),
		cfg.GetRateLimiter.Burst)
	defaultRateLimiter := utils.NewRateLimiter("defaultRateLimiter", float64(cfg.DefaultRateLimiter.Rate),
		cfg.DefaultRateLimiter.Burst)
	batchClient := NewBatchClient(awsClient, getRateLimiter, defaultRateLimiter)
	jobStore, err := NewJobStore(ctx, batchClient, cfg.JobStoreCacheSize, resyncPeriod, cfg.BatchChunkSize, EventHandler{
		Updated: func(ctx context.Context, event Event) {
			err := enqueueOwner(event.NewJob.OwnerReference)
			if err != nil {
				logger.Warnf(ctx, "Failed to enqueue owner [%v] of job [%v]. Error: %v", event.NewJob.OwnerReference, event.NewJob.Id)
			}
		},
	}, scope)

	if err != nil {
		return Executor{}, err
	}

	return Executor{
		jobStore: &jobStore,
	}, nil
}
