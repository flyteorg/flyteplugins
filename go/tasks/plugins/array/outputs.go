package array

import (
	"context"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
	"github.com/lyft/flyteplugins/go/tasks/plugins/array/errorcollector"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/ioutils"

	core2 "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
)

// Assembles a single outputs.pb that contain all the outputs of the subtasks and write them to the final OutputWriter.
// This step can potentially be expensive (hence the metrics) and should be offloaded to a separate step.
func AssembleFinalOutputs(ctx context.Context, tCtx core.TaskExecutionContext, state State) (State, error) {
	// Otherwise, run the data catalog steps - create and submit work items to the catalog processor,
	// build input readers
	outputReaders, err := ConstructOutputReaders(ctx, tCtx.DataStore(), tCtx.OutputWriter().GetOutputPrefixPath(),
		int(state.GetOriginalArraySize()))
	if err != nil {
		return state, err
	}

	finalOutputs := make([]*core2.Literal, 0, state.GetOriginalArraySize())
	for idx, subTaskPhaseIdx := range state.GetArrayStatus().Detailed.GetItems() {
		existingPhase := core.Phases[subTaskPhaseIdx]
		if existingPhase.IsSuccess() {
			output, executionError, err := outputReaders[idx].Read(ctx)
			if err != nil {
				return nil, err
			}

			if executionError == nil && output != nil {
				finalOutputs = append(finalOutputs, &core2.Literal{
					Value: &core2.Literal_Map{
						Map: output,
					},
				})
			}
		} else {
			// TODO: Do we need the names of the outputs in the literalMap here?
			finalOutputs = append(finalOutputs, &core2.Literal{
				Value: &core2.Literal_Map{
					Map: &core2.LiteralMap{},
				},
			})
		}
	}

	err = tCtx.OutputWriter().Put(ctx, ioutils.NewInMemoryOutputReader(&core2.LiteralMap{
		Literals: map[string]*core2.Literal{
			"arr": {
				Value: &core2.Literal_Collection{
					Collection: &core2.LiteralCollection{
						Literals: finalOutputs,
					},
				},
			},
		},
	}, nil))

	if err != nil {
		return nil, err
	}

	state = state.SetPhase(PhaseAssembleFinalOutput, 0)
	return state, nil
}

// Assembles a single error.pb that contain all the errors of the subtasks and write them to the final OutputWriter.
// This step can potentially be expensive (hence the metrics) and should be offloaded to a separate step.
func AssembleFinalErrors(ctx context.Context, tCtx core.TaskExecutionContext, maxErrorMessageLength int, state State) (State, error) {
	// Otherwise, run the data catalog steps - create and submit work items to the catalog processor,
	// build input readers
	outputReaders, err := ConstructOutputReaders(ctx, tCtx.DataStore(), tCtx.OutputWriter().GetOutputPrefixPath(),
		int(state.GetOriginalArraySize()))
	if err != nil {
		return state, err
	}

	ec := errorcollector.NewErrorMessageCollector()
	for idx, subTaskPhaseIdx := range state.GetArrayStatus().Detailed.GetItems() {
		existingPhase := core.Phases[subTaskPhaseIdx]
		if existingPhase.IsFailure() {
			_, executionError, err := outputReaders[idx].Read(ctx)
			if err != nil {
				return nil, err
			}

			if executionError != nil {
				ec.Collect(idx, executionError.String())
			}
		}
	}

	if ec.Length() > 0 {
		err = tCtx.OutputWriter().Put(ctx, ioutils.NewInMemoryOutputReader(nil, &io.ExecutionError{
			ExecutionError: &core2.ExecutionError{
				Code:     "",
				Message:  ec.Summary(maxErrorMessageLength),
				ErrorUri: "",
			},
			IsRecoverable: false,
		}))

		if err != nil {
			return nil, err
		}
	}

	state = state.SetPhase(PhaseRetryableFailure, 0)
	return state, nil
}
