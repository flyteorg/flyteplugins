package array

import (
	"context"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/catalog"
	"github.com/lyft/flyteplugins/go/tasks/v1/array/bitarray"

	"github.com/lyft/flyteplugins/go/tasks/v1/array/arraystatus"

	"strconv"

	structpb "github.com/golang/protobuf/ptypes/struct"
	idlPlugins "github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/io"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/ioutils"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/utils"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/workqueue"
	"github.com/lyft/flyteplugins/go/tasks/v1/errors"
	"github.com/lyft/flyteplugins/go/tasks/v1/flytek8s"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/storage"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const K8sPodKind = "pod"
const maxDataSize = int64(50000000)

type Phase uint8

const (
	PhaseNotStarted Phase = iota
	PhaseCreateMappingFile
	PhaseMappingFileCreated
	PhaseJobSubmitted
	PhaseJobsFinished
)

type State struct {
	currentPhase    Phase
	reason          string
	actualArraySize int
	arrayStatus     arraystatus.ArrayStatus
	catalogResults  *bitarray.BitSet
}

func (s State) GetReason() string {
	return s.reason
}

func (s State) GetActualArraySize() int {
	return s.actualArraySize
}

func (s State) GetPhase() Phase {
	return s.currentPhase
}

func (s State) GetArrayStatus() arraystatus.ArrayStatus {
	return s.arrayStatus
}

func (s *State) SetReason(reason string) *State {
	s.reason = reason
	return s
}

func (s *State) SetActualArraySize(size int) *State {
	s.actualArraySize = size
	return s
}

func (s *State) SetPhase(newPhase Phase) *State {
	s.currentPhase = newPhase
	return s
}

func (s *State) SetArrayStatus(state arraystatus.ArrayStatus) *State {
	s.arrayStatus = state
	return s
}

/*
  Discovery for sub-tasks
  Build mapping file
---
  submit jobs (either as a batch or individually)
---BestEffort
  Detect changes to individual job states
    - Check failure ratios
---
  Submit to discovery

*/

func ToArrayJob(structObj *structpb.Struct) (*idlPlugins.ArrayJob, error) {
	arrayJob := &idlPlugins.ArrayJob{}
	err := utils.UnmarshalStruct(structObj, arrayJob)
	return arrayJob, err
}

// Check if there are any previously cached tasks. If there are we will only submit an ArrayJob for the
// non-cached tasks. The ArrayJob is now a different size, and each task will get a new index location
// which is different than their original location. To find the original index we construct an indexLookup array.
// The subtask can find it's original index value in indexLookup[JOB_ARRAY_INDEX] where JOB_ARRAY_INDEX is an
// environment variable in the pod
func DetermineDiscoverability(ctx context.Context, tCtx core.TaskExecutionContext, state State,
	catalogReader workqueue.IndexedWorkQueue) (State, error) {

	// Check that the taskTemplate is valid
	taskTemplate, err := tCtx.TaskReader().Read(ctx)
	if err != nil {
		return state, err
	} else if taskTemplate == nil {
		return state, errors.Errorf(errors.BadTaskSpecification, "Required value not set, taskTemplate is nil")
	}

	// Extract the custom plugin pb
	arrayJob, err := ToArrayJob(taskTemplate.GetCustom())
	if err != nil {
		return state, err
	} else if arrayJob == nil {
		return state, errors.Errorf(errors.BadTaskSpecification, "Could not extract custom array job")
	}

	// If the task is not discoverable, then skip data catalog work and move directly to launch
	if taskTemplate.Metadata == nil || !taskTemplate.Metadata.Discoverable {
		logger.Infof(ctx, "Task is not discoverable, moving to launch phase...")
		// TODO: set the phase to launch and return
	}

	// Otherwise, run the data catalog steps - create and submit work items to the catalog processor,
	// build input readers
	inputReaders, err := ConstructInputReaders(ctx, tCtx.DataStore(), tCtx.InputReader().GetInputPrefixPath(), int(arrayJob.Size))
	// build output writers
	outputWriters, err := ConstructOutputWriters(ctx, tCtx.DataStore(), tCtx.OutputWriter().GetOutputPrefixPath(), int(arrayJob.Size))

	// build work items from inputs and outputs
	workItems := ConstructCatalogReaderWorkItems(tCtx.TaskReader(), inputReaders, outputWriters)

	doneCheckingCatalog, catalogResults, err := CheckCatalog(ctx, catalogReader, workItems)

	//  If all done, store catalog results, and move to writing the mapping file.
	if doneCheckingCatalog {
		state.currentPhase = PhaseCreateMappingFile
		state.catalogResults = catalogResults
	}

	return state, nil
}

func CheckCatalog(ctx context.Context, catalogReader workqueue.IndexedWorkQueue, workItems []*catalog.ReaderWorkItem) (
	bool, *bitarray.BitSet, error) {

	catalogResults := bitarray.NewBitSet(uint(len(workItems)))

	// enqueue work items
	for _, w := range workItems {
		err := catalogReader.Queue(w)
		if err != nil {
			return false, catalogResults, errors.Wrapf(errors.DownstreamSystemError, err,
				"Error enqueuing work item %s", w.GetId())
		}
	}

	// Immediately read back from the work queue, and store results into a bitset if available

	for idx, w := range workItems {
		retrievedItem, found, err := catalogReader.Get(w.GetId())
		if err != nil {
			return false, catalogResults, err
		}
		if !found {
			logger.Warnf(ctx, "Item just placed into Catalog work queue has disappeared")
		}

		if retrievedItem.GetWorkStatus() != workqueue.WorkStatusDone {
			logger.Debugf(ctx, "Found at least one catalog work item unfinished, skipping rest of round. ID %s",
				retrievedItem.GetId())
			return false, catalogResults, nil
		}

		castedItem, ok := retrievedItem.(*catalog.ReaderWorkItem)
		if !ok {
			return false, catalogResults, errors.Errorf(errors.DownstreamSystemError,
				"Failed to cast when reading from work queue.")
		}

		if castedItem.IsCached() {
			catalogResults.Set(uint(idx))
		}
	}

	return true, catalogResults, nil
}

func ConstructCatalogReaderWorkItems(taskReader core.TaskReader, inputs []io.InputReader,
	outputs []io.OutputWriter) []*catalog.ReaderWorkItem {

	workItems := make([]*catalog.ReaderWorkItem, len(inputs))
	for idx, inputReader := range inputs {
		item := catalog.NewReaderWorkItem(workqueue.WorkItemID(inputReader.GetInputPath()), taskReader, inputReader, outputs[idx])
		workItems = append(workItems, item)
	}

	return workItems
}

func ConstructInputReaders(ctx context.Context, dataStore *storage.DataStore, inputPrefix storage.DataReference,
	size int) ([]io.InputReader, error) {

	inputReaders := make([]io.InputReader, size)
	for i := 0; i < int(size); i++ {

		indexedInputLocation, err := ioutils.GetPath(ctx, dataStore, inputPrefix, strconv.Itoa(i))
		if err != nil {
			return inputReaders, err
		}

		inputReader := ioutils.NewRemoteFileInputReader(ctx, dataStore, ioutils.NewInputFilePaths(ctx, dataStore, indexedInputLocation))
		inputReaders = append(inputReaders, inputReader)
	}
	return inputReaders, nil
}

func ConstructOutputWriters(ctx context.Context, dataStore *storage.DataStore, outputPrefix storage.DataReference,
	size int) ([]io.OutputWriter, error) {

	// turn into output writer
	outputWriters := make([]io.OutputWriter, size)

	for i := 0; i < int(size); i++ {
		dataReference, err := ioutils.GetPath(ctx, dataStore, outputPrefix, strconv.Itoa(i))
		if err != nil {
			return outputWriters, err
		}
		writer := ioutils.NewSimpleOutputWriter(ctx, dataStore, ioutils.NewSimpleOutputFilePaths(ctx, dataStore, dataReference))
		outputWriters = append(outputWriters, writer)
	}
	return outputWriters, nil
}

// Note that Name is not set on the result object.
// It's up to the caller to set the Name before creating the object in K8s.
func FlyteArrayJobToK8sPodTemplate(ctx context.Context, tCtx core.TaskExecutionContext) (
	podTemplate v1.Pod, job *idlPlugins.ArrayJob, err error) {

	// Check that the taskTemplate is valid
	taskTemplate, err := tCtx.TaskReader().Read(ctx)
	if err != nil {
		return v1.Pod{}, nil, err
	} else if taskTemplate == nil {
		return v1.Pod{}, nil, errors.Errorf(errors.BadTaskSpecification, "Required value not set, taskTemplate is nil")
	}

	if taskTemplate.GetContainer() == nil {
		return v1.Pod{}, nil, errors.Errorf(errors.BadTaskSpecification,
			"Required value not set, taskTemplate Container")
	}

	var arrayJob *idlPlugins.ArrayJob
	if taskTemplate.GetCustom() != nil {
		arrayJob, err = ToArrayJob(taskTemplate.GetCustom())
		if err != nil {
			return v1.Pod{}, nil, err
		}
	}

	podSpec, err := flytek8s.ToK8sPodSpec(ctx, tCtx.TaskExecutionMetadata(), tCtx.TaskReader(), tCtx.InputReader(),
		tCtx.OutputWriter().GetOutputPrefixPath().String())
	if err != nil {
		return v1.Pod{}, nil, err
	}

	// TODO: confirm whether this can be done when creating the pod spec directly above
	podSpec.Containers[0].Command = taskTemplate.GetContainer().Command
	podSpec.Containers[0].Args = taskTemplate.GetContainer().Args

	return v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       K8sPodKind,
			APIVersion: v1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			// Note that name is missing here
			Namespace:       tCtx.TaskExecutionMetadata().GetNamespace(),
			Labels:          tCtx.TaskExecutionMetadata().GetLabels(),
			Annotations:     tCtx.TaskExecutionMetadata().GetAnnotations(),
			OwnerReferences: []metav1.OwnerReference{tCtx.TaskExecutionMetadata().GetOwnerReference()},
		},
		Spec: *podSpec,
	}, arrayJob, nil
}
