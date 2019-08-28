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

type CollectionExecutionPhase int

const (
	PhaseInitializing CollectionExecutionPhase = iota
	PhaseAttemptAllQueries
	PhaseAllQueriesLaunched
	PhaseAllQueriesTerminated
)

type CollectionExecutionState struct {
	Phase  CollectionExecutionPhase
	states []qubole_single.ExecutionState
}

// This phase just creates the zero values of the inner ExecutionStates. This simplifies the next step when we
// have to match up our new overridden task execution contexts with these inner ExecutionStates
func InitializeStates(ctx context.Context, tCtx core.TaskExecutionContext) (CollectionExecutionState, error) {
	originalTaskTemplate, err := tCtx.TaskReader().Read(ctx)
	if err != nil {
		return CollectionExecutionState{}, err
	}
	hiveJob := plugins.QuboleHiveJob{}
	err = utils.UnmarshalStruct(originalTaskTemplate.GetCustom(), &hiveJob)
	if err != nil {
		return CollectionExecutionState{}, err
	}
	queryCount := len(hiveJob.QueryCollection.Queries)
	if queryCount == 0 {
		return CollectionExecutionState{}, errors.Errorf(errors.BadTaskSpecification, "Collection is empty")
	}

	newStates := make([]qubole_single.ExecutionState, 0, queryCount)
	for range hiveJob.QueryCollection.Queries {
		newStates = append(newStates, qubole_single.ExecutionState{})
	}

	return CollectionExecutionState{
		Phase:  PhaseAttemptAllQueries,
		states: newStates,
	}, nil
}

// This function does everything - it attempts to launch queries by reading the task template, requesting allocation
// tokens, submitting to Qubole, and updating statuses of previously submitted Qubole queries.
func AttemptKickoffAndMonitoring(ctx context.Context, tCtx core.TaskExecutionContext, currentState CollectionExecutionState,
	quboleClient client.QuboleClient, secretsManager SecretsManager, cache utils2.AutoRefreshCache) (
	CollectionExecutionState, error) {

	// This will read the custom field of the task template, and transform the collection of queries into list of new
	// task templates with just one query each
	childTaskExecutionContexts, err := NewCustomTaskExecutionContexts(ctx, tCtx)
	if err != nil {
		return currentState, err
	}

	newStates := make([]qubole_single.ExecutionState, len(currentState.states))

	for i, s := range currentState.states {
		// Handle each inner execution state
		newState, err := qubole_single.HandleExecutionState(ctx, childTaskExecutionContexts[i], s, quboleClient, secretsManager, cache)
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

func Finalize(ctx context.Context, tCtx core.TaskExecutionContext, current CollectionExecutionState) error {
	for _, s := range current.states {
		err := qubole_single.Finalize(ctx, tCtx, s)
		if err != nil {
			return err
		}
	}
	return nil
}

func Abort(ctx context.Context, tCtx core.TaskExecutionContext, current CollectionExecutionState,
	client client.QuboleClient, secretsManager SecretsManager) error {
	for _, s := range current.states {
		err := qubole_single.Abort(ctx, tCtx, s, client, secretsManager)
		if err != nil {
			return err
		}
	}
	return nil
}

func DetermineCollectionPhaseFrom(states []qubole_single.ExecutionState) CollectionExecutionPhase {
	for _, x := range states {
		// If any have yet to be submitted, then we will continue to attempt both query kickoff, and monitoring.
		if qubole_single.IsNotYetSubmitted(x) {
			return PhaseAttemptAllQueries
		}
	}

	for _, x := range states {
		// If any are not finished, then we continue to monitor
		if !qubole_single.InTerminalState(x) {
			return PhaseAllQueriesLaunched
		}
	}

	return PhaseAllQueriesTerminated
}

func MapCollectionExecutionToPhaseInfo(state CollectionExecutionState) core.PhaseInfo {
	var phaseInfo core.PhaseInfo

	taskInfo := ConstructTaskInfo(state)

	switch state.Phase {
	case PhaseAttemptAllQueries, PhaseAllQueriesLaunched:
		if taskInfo != nil {
			phaseInfo = core.PhaseInfoRunning(uint8(len(taskInfo.Logs)), taskInfo)
		} else {
			phaseInfo = core.PhaseInfoRunning(core.DefaultPhaseVersion, nil)
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
	logs := make([]*idlCore.TaskLog, 0)
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
