package array

import (
	"context"
	"fmt"

	arrayCore "github.com/lyft/flyteplugins/go/tasks/plugins/array/core"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/workqueue"
	"github.com/lyft/flytestdlib/bitarray"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/storage"

	"github.com/lyft/flyteidl/clients/go/coreutils"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
	"github.com/lyft/flyteplugins/go/tasks/plugins/array/errorcollector"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/ioutils"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	pluginCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
)

// Represents an indexed work queue that aggregates outputs of sub tasks.
type OutputAssembler struct {
	workqueue.IndexedWorkQueue
}

func (o OutputAssembler) Queue(id workqueue.WorkItemID, item *outputAssembleItem) error {
	return o.IndexedWorkQueue.Queue(id, item)
}

type outputAssembleItem struct {
	outputPrefix storage.DataReference
	varNames     []string
	finalPhases  bitarray.CompactArray
	outputWriter io.OutputWriter
	dataStore    *storage.DataStore
}

type assembleOutputsWorker struct {
}

func (w assembleOutputsWorker) Process(ctx context.Context, workItem workqueue.WorkItem) (workqueue.WorkStatus, error) {
	i := workItem.(*outputAssembleItem)

	outputReaders, err := ConstructOutputReaders(ctx, i.dataStore, i.outputPrefix, int(i.finalPhases.ItemsCount))
	if err != nil {
		return workqueue.WorkStatusNotDone, err
	}

	finalOutputs := map[string]interface{}{}
	for idx, subTaskPhaseIdx := range i.finalPhases.GetItems() {
		existingPhase := pluginCore.Phases[subTaskPhaseIdx]
		if existingPhase.IsSuccess() {
			output, executionError, err := outputReaders[idx].Read(ctx)
			if err != nil {
				return workqueue.WorkStatusNotDone, err
			}

			if executionError == nil && output != nil {
				appendSubTaskOutput(finalOutputs, output, int64(i.finalPhases.ItemsCount))
				continue
			}
		}

		// TODO: Do we need the names of the outputs in the literalMap here?
		appendEmptyOutputs(i.varNames, finalOutputs)
	}

	outputs, err := coreutils.MakeLiteralForMap(finalOutputs)
	if err != nil {
		return workqueue.WorkStatusNotDone, err
	}

	err = i.outputWriter.Put(ctx, ioutils.NewInMemoryOutputReader(outputs.GetMap(), nil))
	if err != nil {
		return workqueue.WorkStatusNotDone, err
	}

	return workqueue.WorkStatusSucceeded, nil
}

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

func appendEmptyOutputs(vars []string, outputs map[string]interface{}) {
	for _, varName := range vars {
		existingVal, found := outputs[varName]
		if !found {
			existingVal = make([]interface{}, 0, 1)
		}

		existingVal = append(existingVal.([]interface{}), coreutils.MustMakeLiteral(nil))
		outputs[varName] = existingVal
	}
}

// Assembles a single outputs.pb that contain all the outputs of the subtasks and write them to the final OutputWriter.
// This step can potentially be expensive (hence the metrics) and why it's offloaded to a background process.
func AssembleFinalOutputs(ctx context.Context, assemblyQueue OutputAssembler, tCtx pluginCore.TaskExecutionContext,
	terminalPhase arrayCore.Phase, state *arrayCore.State) (*arrayCore.State, error) {

	// Otherwise, run the data catalog steps - create and submit work items to the catalog processor,
	// build input readers
	workItemID := tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()
	w, found, err := assemblyQueue.Get(workItemID)
	if err != nil {
		return nil, err
	}

	// If the work item is not found in the queue, it's either never queued, was evicted, or we are recovering from a
	// reboot. Add it again.
	if !found {
		taskTemplate, err := tCtx.TaskReader().Read(ctx)
		if err != nil {
			return nil, err
		}

		outputVariables := taskTemplate.GetInterface().GetOutputs()
		if outputVariables == nil || outputVariables.GetVariables() == nil {
			// If the task has no outputs, bail early.
			state = state.SetPhase(terminalPhase, 0)
			return state, nil
		}

		varNames := make([]string, 0, len(outputVariables.GetVariables()))

		err = assemblyQueue.Queue(workItemID, &outputAssembleItem{
			outputWriter: tCtx.OutputWriter(),
			varNames:     varNames,
			finalPhases:  state.GetArrayStatus().Detailed,
			outputPrefix: tCtx.OutputWriter().GetOutputPrefixPath(),
			dataStore:    tCtx.DataStore(),
		})

		if err != nil {
			return nil, err
		}

		w, found, err = assemblyQueue.Get(workItemID)
		if err != nil {
			return nil, err
		}

		if !found {
			return nil, fmt.Errorf("couldn't find work item [%v] after immediately adding it", workItemID)
		}
	}

	switch w.Status() {
	case workqueue.WorkStatusSucceeded:
		state = state.SetPhase(terminalPhase, 0)
	case workqueue.WorkStatusFailed:
		state = state.SetExecutionErr(&core.ExecutionError{
			Message: w.Error().Error(),
		})

		state = state.SetPhase(arrayCore.PhaseRetryableFailure, 0)
	}

	return state, nil
}

type assembleErrorsWorker struct {
	maxErrorMessageLength int
}

func (a assembleErrorsWorker) Process(ctx context.Context, workItem workqueue.WorkItem) (workqueue.WorkStatus, error) {
	w := workItem.(*outputAssembleItem)
	outputReaders, err := ConstructOutputReaders(ctx, w.dataStore, w.outputPrefix, int(w.finalPhases.ItemsCount))
	if err != nil {
		return workqueue.WorkStatusNotDone, err
	}

	ec := errorcollector.NewErrorMessageCollector()
	for idx, subTaskPhaseIdx := range w.finalPhases.GetItems() {
		existingPhase := pluginCore.Phases[subTaskPhaseIdx]
		if existingPhase.IsFailure() {
			isError, err := outputReaders[idx].IsError(ctx)

			if err != nil {
				return workqueue.WorkStatusNotDone, err
			}

			if isError {
				executionError, err := outputReaders[idx].ReadError(ctx)
				if err != nil {
					return workqueue.WorkStatusNotDone, err
				}

				ec.Collect(idx, executionError.String())
			}
		}
	}

	if ec.Length() > 0 {
		err = w.outputWriter.Put(ctx, ioutils.NewInMemoryOutputReader(nil, &io.ExecutionError{
			ExecutionError: &core.ExecutionError{
				Code:     "",
				Message:  ec.Summary(a.maxErrorMessageLength),
				ErrorUri: "",
			},
			IsRecoverable: false,
		}))

		if err != nil {
			return workqueue.WorkStatusNotDone, err
		}
	}

	return workqueue.WorkStatusSucceeded, nil
}

func NewOutputAssembler(workQueueConfig workqueue.Config, scope promutils.Scope) (OutputAssembler, error) {
	q, err := workqueue.NewIndexedWorkQueue(assembleOutputsWorker{}, workQueueConfig, scope)
	if err != nil {
		return OutputAssembler{}, err
	}

	return OutputAssembler{
		IndexedWorkQueue: q,
	}, nil
}

func NewErrorAssembler(maxErrorMessageLength int, workQueueConfig workqueue.Config, scope promutils.Scope) (OutputAssembler, error) {
	q, err := workqueue.NewIndexedWorkQueue(assembleErrorsWorker{
		maxErrorMessageLength: maxErrorMessageLength,
	}, workQueueConfig, scope)

	if err != nil {
		return OutputAssembler{}, err
	}

	return OutputAssembler{
		IndexedWorkQueue: q,
	}, nil
}
