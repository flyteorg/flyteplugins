package awsbatch

import (
	"context"

	"github.com/lyft/flyteplugins/go/tasks/plugins/array"
	"github.com/lyft/flyteplugins/go/tasks/plugins/array/arraystatus"
	"github.com/lyft/flyteplugins/go/tasks/plugins/array/awsbatch/config"
	"github.com/lyft/flytestdlib/bitarray"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
)

func newStatusCompactArray(count uint) bitarray.CompactArray {
	// TODO: This is fragile, we should introduce a TaskPhaseCount as the last element in the enum
	a, err := bitarray.NewCompactArray(count, bitarray.Item(core.PhasePermanentFailure))
	if err != nil {
		return bitarray.CompactArray{}
	}

	return a
}

func LaunchSubTasks(ctx context.Context, tCtx core.TaskExecutionContext, batchClient Client, pluginConfig *config.Config,
	currentState *State) (nextState *State, err error) {

	jobDefinition := ""
	batchInput, err := FlyteTaskToBatchInput(ctx, tCtx, jobDefinition, pluginConfig)
	if err != nil {
		return nil, err
	}

	size := currentState.GetExecutionArraySize()
	// If the original job was marked as an array (not a single job), then make sure to set it up correctly.
	if currentState.GetOriginalArraySize() > 1 {
		batchInput = UpdateBatchInputForArray(ctx, batchInput, int64(size))
	}

	j, err := batchClient.SubmitJob(ctx, batchInput)
	if err != nil {
		return nil, err
	}

	nextState = currentState.SetExternalJobID(j)
	nextState = nextState.SetPhase(array.PhaseCheckingSubTaskExecutions, 0).(*State)
	nextState = nextState.SetArrayStatus(arraystatus.ArrayStatus{
		Summary:  arraystatus.ArraySummary{},
		Detailed: newStatusCompactArray(uint(size)),
	}).(*State)

	return nextState, nil
}
