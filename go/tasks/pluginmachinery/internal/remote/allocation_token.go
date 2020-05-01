package remote

import (
	"context"
	"fmt"
	"time"

	"github.com/lyft/flytestdlib/logger"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/remote"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
)

func allocateToken(ctx context.Context, p remote.Plugin, tCtx core.TaskExecutionContext, state *State) (
	newState *State, phaseInfo core.PhaseInfo, err error) {
	if len(p.GetPluginProperties().ResourceQuotas) == 0 {
		// No quota, return success
		return &State{
			AllocationTokenRequestStartTime: time.Now(),
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
		return &State{
			AllocationTokenRequestStartTime: time.Now(),
			Phase:                           PhaseAllocationTokenAcquired,
		}, core.PhaseInfo{}, nil
	case core.AllocationStatusNamespaceQuotaExceeded:
	case core.AllocationStatusExhausted:
		logger.Infof(ctx, "Couldn't allocate token because allocation status is [%v].", allocationStatus.String())
		startTime := state.AllocationTokenRequestStartTime
		if startTime.IsZero() {
			startTime = time.Now()
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

func releaseToken(ctx context.Context, p remote.Plugin, tCtx core.TaskExecutionContext, state *State) error {
	if len(p.GetPluginProperties().ResourceQuotas) == 0 {
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
		logger.Errorf(ctx, "Failed to release resources for task. Error: %v", err)
		return err
	}

	return nil
}
