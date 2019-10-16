package awsbatch

import (
	"context"
	"fmt"

	core2 "github.com/lyft/flyteplugins/go/tasks/plugins/array/core"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/plugins/array/arraystatus"
	"github.com/lyft/flyteplugins/go/tasks/plugins/array/awsbatch/config"
)

func LaunchSubTasks(ctx context.Context, tCtx core.TaskExecutionContext, batchClient Client, pluginConfig *config.Config,
	currentState *State) (nextState *State, err error) {

	jobDefinition := currentState.GetJobDefinitionArn()
	if len(jobDefinition) == 0 {
		return nil, fmt.Errorf("system error; no job definition created")
	}

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

	parentState := currentState.
		SetPhase(core2.PhaseCheckingSubTaskExecutions, 0).
		SetArrayStatus(arraystatus.ArrayStatus{
			Summary:  arraystatus.ArraySummary{},
			Detailed: core2.NewPhasesCompactArray(uint(size)),
		})

	nextState = currentState.SetExternalJobID(j)
	nextState.State = parentState

	return nextState, nil
}
