package webapi

import (
	"context"
	"fmt"

	clock2 "k8s.io/utils/clock"

	"github.com/lyft/flytestdlib/logger"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/webapi"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
)

var (
	clock clock2.Clock
)

func SetClockForTest(clck clock2.Clock) {
	clock = clck
}

func init() {
	clock = clock2.RealClock{}
}

func allocateToken(ctx context.Context, p webapi.Plugin, tCtx core.TaskExecutionContext, state *State, metrics Metrics) (
	newState *State, phaseInfo core.PhaseInfo, err error) {
	if len(p.GetConfig().ResourceQuotas) == 0 {
		// No quota, return success
		return &State{
			AllocationTokenRequestStartTime: clock.Now(),
			Phase:                           PhaseAllocationTokenAcquired,
		}, core.PhaseInfo{}, nil
	}

	ns, constraints, err := p.ResourceRequirements(ctx, tCtx)
	if err != nil {
		logger.Errorf(ctx, "Failed to calculate resource requirements for task. Error: %v", err)
		return nil, core.PhaseInfo{}, err
	}

	token := tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()
	allocationStatus, err := tCtx.ResourceManager().AllocateResource(ctx, ns, token, constraints)
	if err != nil {
		logger.Errorf(ctx, "Failed to allocate resources for task. Error: %v", err)
		return nil, core.PhaseInfo{}, err
	}

	switch allocationStatus {
	case core.AllocationStatusGranted:
		metrics.AllocationGranted.Inc(ctx)
		metrics.ResourceWaitTime.Observe(float64(clock.Since(state.AllocationTokenRequestStartTime).Milliseconds()))
		return &State{
			AllocationTokenRequestStartTime: clock.Now(),
			Phase:                           PhaseAllocationTokenAcquired,
		}, core.PhaseInfo{}, nil
	case core.AllocationStatusNamespaceQuotaExceeded:
	case core.AllocationStatusExhausted:
		metrics.AllocationNotGranted.Inc(ctx)
		logger.Infof(ctx, "Couldn't allocate token because allocation status is [%v].", allocationStatus.String())
		startTime := state.AllocationTokenRequestStartTime
		if startTime.IsZero() {
			startTime = clock.Now()
		}

		return &State{
			AllocationTokenRequestStartTime: startTime,
			Phase:                           PhaseNotStarted,
		}, core.PhaseInfo{}, nil
	default:
		return nil, core.PhaseInfo{}, fmt.Errorf("allocation status undefined")
	}

	return state, core.PhaseInfo{}, nil
}

func releaseToken(ctx context.Context, p webapi.Plugin, tCtx core.TaskExecutionContext, metrics Metrics) error {
	if len(p.GetConfig().ResourceQuotas) == 0 {
		// No quota, return success
		return nil
	}

	ns, _, err := p.ResourceRequirements(ctx, tCtx)
	if err != nil {
		logger.Errorf(ctx, "Failed to calculate resource requirements for task. Error: %v", err)
		return err
	}

	token := tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()
	err = tCtx.ResourceManager().ReleaseResource(ctx, ns, token)
	if err != nil {
		metrics.ResourceReleaseFailed.Inc(ctx)
		logger.Errorf(ctx, "Failed to release resources for task. Error: %v", err)
		return err
	}

	metrics.ResourceReleased.Inc(ctx)
	return nil
}
