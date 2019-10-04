package array

import (
	"context"

	"github.com/lyft/flyteidl/clients/go/coreutils"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
	"github.com/lyft/flyteplugins/go/tasks/plugins/array/errorcollector"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/ioutils"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	pluginCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
)

func appendSubTaskOutput(outputs map[string]interface{}, subTaskOutput *core.LiteralMap,
	expectedSize int64) {

	for key, val := range subTaskOutput.GetLiterals() {
		arr, exists := outputs[key]
		if !exists {
			arr = make([]interface{}, 0, expectedSize)
		}

		arr = append(arr.([]interface{}), val)
		outputs[key] = arr
	}
}

func appendEmptyOutputs(vars map[string]*core.Variable, outputs map[string]interface{}) {
	for key := range vars {
		existingVal, found := outputs[key]
		if !found {
			existingVal = make([]interface{}, 0, 1)
		}

		existingVal = append(existingVal.([]interface{}), &core.Literal{})
		outputs[key] = existingVal
	}
}

// Assembles a single outputs.pb that contain all the outputs of the subtasks and write them to the final OutputWriter.
// This step can potentially be expensive (hence the metrics) and should be offloaded to a separate step.
func AssembleFinalOutputs(ctx context.Context, tCtx pluginCore.TaskExecutionContext, state State) (State, error) {
	// Otherwise, run the data catalog steps - create and submit work items to the catalog processor,
	// build input readers
	outputReaders, err := ConstructOutputReaders(ctx, tCtx.DataStore(), tCtx.OutputWriter().GetOutputPrefixPath(),
		int(state.GetOriginalArraySize()))
	if err != nil {
		return state, err
	}

	taskTemplate, err := tCtx.TaskReader().Read(ctx)
	if err != nil {
		return nil, err
	}

	outputVariables := taskTemplate.GetInterface().GetOutputs()
	if outputVariables == nil || outputVariables.GetVariables() == nil {
		// If the task has no outputs, bail early.
		state = state.SetPhase(PhaseSuccess, 0)
		return state, nil
	}

	finalOutputs := map[string]interface{}{}
	for idx, subTaskPhaseIdx := range state.GetArrayStatus().Detailed.GetItems() {
		existingPhase := pluginCore.Phases[subTaskPhaseIdx]
		if existingPhase.IsSuccess() {
			output, executionError, err := outputReaders[idx].Read(ctx)
			if err != nil {
				return nil, err
			}

			if executionError == nil && output != nil {
				appendSubTaskOutput(finalOutputs, output, state.GetOriginalArraySize())
				continue
			}
		}

		// TODO: Do we need the names of the outputs in the literalMap here?
		appendEmptyOutputs(outputVariables.GetVariables(), finalOutputs)
	}

	outputs, err := coreutils.MakeLiteralForMap(finalOutputs)
	if err != nil {
		return nil, err
	}

	err = tCtx.OutputWriter().Put(ctx, ioutils.NewInMemoryOutputReader(outputs.GetMap(), nil))

	if err != nil {
		return nil, err
	}

	state = state.SetPhase(PhaseSuccess, 0)
	return state, nil
}

// Assembles a single error.pb that contain all the errors of the subtasks and write them to the final OutputWriter.
// This step can potentially be expensive (hence the metrics) and should be offloaded to a separate step.
func AssembleFinalErrors(ctx context.Context, tCtx pluginCore.TaskExecutionContext, maxErrorMessageLength int, state State) (State, error) {
	// Otherwise, run the data catalog steps - create and submit work items to the catalog processor,
	// build input readers
	outputReaders, err := ConstructOutputReaders(ctx, tCtx.DataStore(), tCtx.OutputWriter().GetOutputPrefixPath(),
		int(state.GetOriginalArraySize()))
	if err != nil {
		return state, err
	}

	ec := errorcollector.NewErrorMessageCollector()
	for idx, subTaskPhaseIdx := range state.GetArrayStatus().Detailed.GetItems() {
		existingPhase := pluginCore.Phases[subTaskPhaseIdx]
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
			ExecutionError: &core.ExecutionError{
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
