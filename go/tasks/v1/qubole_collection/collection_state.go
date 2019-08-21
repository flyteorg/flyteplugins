package qubole_collection

import (
	"context"
	"fmt"
	idlCore "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"
	"github.com/lyft/flyteplugins/go/tasks/v1/errors"
	"github.com/lyft/flyteplugins/go/tasks/v1/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/v1/qubole_single"
	"github.com/lyft/flyteplugins/go/tasks/v1/qubole_single/client"
	"github.com/lyft/flyteplugins/go/tasks/v1/utils"
	utils2 "github.com/lyft/flytestdlib/utils"
)

type CollectionExecutionState struct {
	Phase CollectionExecutionPhase
	states []qubole_single.ExecutionState
}

func ConstructIndividualTaskExecutionContexts(ctx context.Context, tCtx core.TaskExecutionContext) error {
	// Read the initial list of queries
	hiveJob := plugins.QuboleHiveJob{}
	taskTemplate, err := tCtx.TaskReader().Read(ctx)
	if err != nil {
		return err
	} else if taskTemplate == nil {
		return errors.Errorf(errors.BadTaskSpecification, "nil task template")
	}

	err := utils.UnmarshalStruct(taskTemplate.GetCustom(), &hiveJob)
	if err != nil {
		return err
	}

	queryCollection := hiveJob.QueryCollection.Queries

}




// This function does everything - launches, gets allocation tokens, does everything.
func DoEverything(ctx context.Context, tCtx core.TaskExecutionContext, currentState CollectionExecutionState,
	quboleClient client.QuboleClient, secretsManager SecretsManager, cache utils2.AutoRefreshCache) (
	CollectionExecutionState, error) {

	newStates := make([]qubole_single.ExecutionState, len(currentState.states))

	for _, x := range currentState.states {
		// Handle each inner execution state
		newState, err := qubole_single.HandleExecutionState(ctx, tCtx, x, quboleClient, secretsManager, cache)
		if err != nil {
			return Copy(currentState), err
		}
		newStates = append(newStates, newState)
	}

	// Each of the little ExecutionState phases have now been updated.  It's time to update the larger state
	newExecutionPhase := DetermineCollectionPhaseFrom(newStates)
	return CollectionExecutionState{
		Phase:  newExecutionPhase,
		states: newStates,
	}, nil
}

func DetermineCollectionPhaseFrom(states []qubole_single.ExecutionState) CollectionExecutionPhase {
	for _, x := range states {
		// If any are Queued or NotStarted, then we continue to do everything
		if x.Phase < qubole_single.PhaseSubmitted {
			return PhaseDoingEverything
		}
	}

	for _, x := range states {
		// If any are Queued or NotStarted, then we continue to do everything
		if !qubole_single.InTerminalState(x) {
			return PhaseEverythingLaunched
		}
	}

	return PhaseAllQueriesTerminated
}

func MapCollectionExecutionToPhaseInfo(state CollectionExecutionState) core.PhaseInfo {
	var phaseInfo core.PhaseInfo

	taskInfo := ConstructTaskInfo(state)

	switch state.Phase {
	case PhaseDoingEverything, PhaseEverythingLaunched:
		if taskInfo != nil {
			phaseInfo = core.PhaseInfoRunning(uint8(len(phaseInfo.Info().Logs)), taskInfo)
		} else {
			phaseInfo = core.PhaseInfoRunning(core.DefaultPhaseVersion, phaseInfo)
		}

	case PhaseAllQueriesTerminated:
		notSucceeded := countNotSucceeded(state)
		if notSucceeded == 0 {
			phaseInfo = core.PhaseInfoSuccess(taskInfo)
		} else {
			phaseInfo = core.PhaseInfoRetryableFailure(errors.DownstreamSystemError,
				fmt.Sprintf("Errors in Qubole queries [%d/%d] queries failed", notSucceeded, len(state.states)), taskInfo)
		}

	}

	return phaseInfo
}

func countNotSucceeded(c CollectionExecutionState) int {
	var notSuccess = 0
	for _, x := range c.states {
		if x.Phase != qubole_single.PhaseQuerySucceeded {
			notSuccess++
		}
	}
	return notSuccess
}

func Copy(c CollectionExecutionState) CollectionExecutionState {
	newStates := make([]qubole_single.ExecutionState, len(c.states))
	for _, x := range c.states {
		newStates = append(newStates, qubole_single.Copy(x))
	}
	return CollectionExecutionState{Phase: c.Phase, states: newStates}
}

func ConstructTaskInfo(c CollectionExecutionState) *core.TaskInfo {
	logs := make([]*idlCore.TaskLog, 0, 1)
	for _, x := range c.states {
		if x.CommandId != "" {
			logs = append(logs, qubole_single.ConstructTaskLog(x))
		}
	}

	if len(logs) > 0 {
		return &core.TaskInfo{
			Logs: logs,
		}
	}
	return nil
}
