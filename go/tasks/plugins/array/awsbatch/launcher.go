package awsbatch

import (
	"context"
	array2 "github.com/lyft/flyteplugins/go/tasks/plugins/array"
	arraystatus2 "github.com/lyft/flyteplugins/go/tasks/plugins/array/arraystatus"
	config2 "github.com/lyft/flyteplugins/go/tasks/plugins/array/awsbatch/config"
	bitarray2 "github.com/lyft/flyteplugins/go/tasks/plugins/array/bitarray"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
)

func newStatusCompactArray(count uint) bitarray2.CompactArray {
	// TODO: This is fragile, we should introduce a TaskPhaseCount as the last element in the enum
	a, err := bitarray2.NewCompactArray(count, bitarray2.Item(core.PhasePermanentFailure))
	if err != nil {
		return bitarray2.CompactArray{}
	}

	return a
}

func LaunchSubTasks(ctx context.Context, tCtx core.TaskExecutionContext, batchClient Client, pluginConfig *config2.Config,
	currentState *array2.State) (nextState *array2.State, err error) {

	jobDefinition := ""
	batchInput, err := FlyteTaskToBatchInput(ctx, tCtx, jobDefinition, pluginConfig)
	if err != nil {
		return nil, err
	}

	size := currentState.GetExecutionArraySize()
	// If the original job was marked as an array (not a single job), then make sure to set it up correctly.
	if currentState.OriginalArraySize > 1 {
		batchInput = UpdateBatchInputForArray(ctx, batchInput, int64(size))
	}

	j, err := SubmitJob(ctx, batchInput)
	if err != nil {
		return nil, err
	}

	nextState = currentState.SetExternalJobID(j.Id)
	nextState = nextState.SetPhase(array2.PhaseCheckingSubTaskExecutions, 0)
	nextState = nextState.SetArrayStatus(arraystatus2.ArrayStatus{
		Summary:  arraystatus2.ArraySummary{},
		Detailed: newStatusCompactArray(uint(size)),
	})

	return nextState, nil
}
