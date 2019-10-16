package awsbatch

import (
	"context"
	"fmt"

	idlCore "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"

	arrayCore "github.com/lyft/flyteplugins/go/tasks/plugins/array/core"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/plugins/array/arraystatus"
	"github.com/lyft/flyteplugins/go/tasks/plugins/array/awsbatch/config"
)

func LaunchSubTasks(ctx context.Context, tCtx core.TaskExecutionContext, batchClient Client, pluginConfig *config.Config,
	currentState *State) (nextState *State, logLinks []*idlCore.TaskLog, err error) {

	jobDefinition := currentState.GetJobDefinitionArn()
	if len(jobDefinition) == 0 {
		return nil, logLinks, fmt.Errorf("system error; no job definition created")
	}

	batchInput, err := FlyteTaskToBatchInput(ctx, tCtx, jobDefinition, pluginConfig)
	if err != nil {
		return nil, logLinks, err
	}

	size := currentState.GetExecutionArraySize()
	// If the original job was marked as an array (not a single job), then make sure to set it up correctly.
	if currentState.GetOriginalArraySize() > 1 {
		batchInput = UpdateBatchInputForArray(ctx, batchInput, int64(size))
	}

	j, err := batchClient.SubmitJob(ctx, batchInput)
	if err != nil {
		return nil, logLinks, err
	}

	parentState := currentState.
		SetPhase(arrayCore.PhaseCheckingSubTaskExecutions, 0).
		SetArrayStatus(arraystatus.ArrayStatus{
			Summary:  arraystatus.ArraySummary{},
			Detailed: arrayCore.NewPhasesCompactArray(uint(size)),
		})

	nextState = currentState.SetExternalJobID(j)
	nextState.State = parentState

	// Add job link to task execution.
	logLinks = []*idlCore.TaskLog{
		GetJobTaskLog(currentState.GetExecutionArraySize(), batchClient.GetAccountID(),
			batchClient.GetRegion(), "", j),
	}

	return nextState, logLinks, nil
}
