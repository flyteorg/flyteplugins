package array

import (
	"context"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/catalog"
	"github.com/lyft/flyteplugins/go/tasks/v1/array/bitarray"

	"github.com/lyft/flyteplugins/go/tasks/v1/array/arraystatus"

	"strconv"

	structpb "github.com/golang/protobuf/ptypes/struct"
	idlCore "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
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

type Phase uint8

const (
	PhaseNotStarted Phase = iota
	PhaseLaunch
	PhaseCheckingSubTaskExecutions
	PhaseWriteToDiscovery
	PhaseSuccess
	PhaseRetryableFailure
	PhasePermanentFailure
)

type State struct {
	currentPhase    Phase
	phaseVersion    uint32
	reason          string
	actualArraySize int
	arrayStatus     arraystatus.ArrayStatus

	// Which sub-tasks to cache, (using the original index, that is, the length is ArrayJob.size)
	writeToCatalog *bitarray.BitSet
}

func (s State) GetReason() string {
	return s.reason
}

func (s State) GetActualArraySize() int {
	return s.actualArraySize
}

func (s State) GetPhaseVersion() uint32 {
	return s.phaseVersion
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

func (s *State) SetPhaseVersion(version uint32) *State {
	s.phaseVersion = version
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

const (
	ErrorWorkQueue          errors.ErrorCode = "CATALOG_READER_QUEUE_FAILED"
	ErrorReaderWorkItemCast                  = "READER_WORK_ITEM_CAST_FAILED"
	ErrorInternalMismatch                    = "ARRAY_MISMATCH"
)

func ToArrayJob(structObj *structpb.Struct) (*idlPlugins.ArrayJob, error) {
	arrayJob := &idlPlugins.ArrayJob{}
	err := utils.UnmarshalStruct(structObj, arrayJob)
	return arrayJob, err
}

func SummaryToPhase(ctx context.Context, arrayJobProps *idlPlugins.ArrayJob, summary arraystatus.ArraySummary) Phase {
	minSuccesses := int64(1)
	if arrayJobProps != nil {
		minSuccesses = arrayJobProps.MinSuccesses
	}

	totalCount := int64(0)
	totalSuccesses := int64(0)
	totalFailures := int64(0)
	totalRunning := int64(0)
	for phase, count := range summary {
		totalCount += count
		if phase.IsTerminal() {
			if phase.IsSuccess() {
				totalSuccesses += count
			} else {
				// TODO: Split out retryable failures to be retried without doing the entire array task.
				// TODO: Other option: array tasks are only retriable as a full set and to get single task retriability
				// TODO: dynamic_task must be updated to not auto-combine to array tasks.  For scale reasons, it is
				// TODO: preferable to auto-combine to array tasks for now.
				totalFailures += count
			}
		} else {
			totalRunning += count
		}
	}

	if totalCount < minSuccesses {
		logger.Infof(ctx, "Array failed because totalCount[%v] < minSuccesses[%v]", totalCount, minSuccesses)
		return PhasePermanentFailure
	}

	// No chance to reach the required success numbers.
	if totalRunning+totalSuccesses < minSuccesses {
		logger.Infof(ctx, "Array failed early because totalRunning[%v] + totalSuccesses[%v] < minSuccesses[%v]",
			totalRunning, totalSuccesses, minSuccesses)
		return PhasePermanentFailure
	}

	if totalSuccesses >= minSuccesses && totalRunning == 0 {
		logger.Infof(ctx, "Array succeeded because totalSuccesses[%v] >= minSuccesses[%v]", totalSuccesses, minSuccesses)
		return PhaseSuccess
	}

	logger.Debugf(ctx, "Array is still running [Successes: %v, Failures: %v, Total: %v, MinSuccesses: %v]",
		totalSuccesses, totalFailures, totalCount, minSuccesses)
	return PhaseCheckingSubTaskExecutions
}

// Check if there are any previously cached tasks. If there are we will only submit an ArrayJob for the
// non-cached tasks. The ArrayJob is now a different size, and each task will get a new index location
// which is different than their original location. To find the original index we construct an indexLookup array.
// The subtask can find it's original index value in indexLookup[JOB_ARRAY_INDEX] where JOB_ARRAY_INDEX is an
// environment variable in the pod
func DetermineDiscoverability(ctx context.Context, tCtx core.TaskExecutionContext, state *State,
	catalogReader workqueue.IndexedWorkQueue) (*State, error) {

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
		state.currentPhase = PhaseLaunch
		return state, nil
	}

	// Otherwise, run the data catalog steps - create and submit work items to the catalog processor,
	// build input readers
	inputReaders, err := ConstructInputReaders(ctx, tCtx.DataStore(), tCtx.InputReader().GetInputPrefixPath(), int(arrayJob.Size))
	// build output writers
	outputWriters, err := ConstructOutputWriters(ctx, tCtx.DataStore(), tCtx.OutputWriter().GetOutputPrefixPath(), int(arrayJob.Size))
	// build work items from inputs and outputs
	workItems := ConstructCatalogReaderWorkItems(tCtx.TaskReader(), inputReaders, outputWriters)

	// Check catalog, and if we have responses from catalog for everything, then move to writing the mapping file.
	doneCheckingCatalog, catalogResults, cachedCount, err := CheckCatalog(ctx, catalogReader, workItems)
	if doneCheckingCatalog {
		// If all the sub-tasks are actually done, then we can just move on.
		if cachedCount == int(arrayJob.Size) {
			// TODO: This is not correct?  We still need to write parent level results?
			state.currentPhase = PhaseSuccess
			return state, nil
		}

		indexLookup := CatalogBitsetToLiteralCollection(catalogResults)
		// TODO: Is the right thing to use?  Haytham please take a look
		indexLookupPath, err := ioutils.GetIndexLookupPath(ctx, tCtx.DataStore(), tCtx.OutputWriter().GetOutputPrefixPath())
		if err != nil {
			return state, err
		}

		logger.Infof(ctx, "Writing indexlookup file to [%s], cached count [%d/%d], ",
			indexLookupPath, cachedCount, arrayJob.Size)
		err = tCtx.DataStore().WriteProtobuf(ctx, indexLookupPath, storage.Options{}, indexLookup)
		if err != nil {
			return state, err
		}

		state.currentPhase = PhaseLaunch
		state.actualArraySize = int(arrayJob.Size) - cachedCount
	}

	return state, nil
}

func WriteToDiscovery(ctx context.Context, tCtx core.TaskExecutionContext, catalogWriter workqueue.IndexedWorkQueue,
	state *State) (*State, error) {

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

	// input readers
	inputReaders, err := ConstructInputReaders(ctx, tCtx.DataStore(), tCtx.InputReader().GetInputPrefixPath(), int(arrayJob.Size))

	// output reader
	outputReaders, err := ConstructOutputReaders(ctx, tCtx.DataStore(), tCtx.OutputWriter().GetOutputPrefixPath(), int(arrayJob.Size))

	// Create catalog put items, but only put the ones that were not originally cached (as read from the catalog results bitset)
	catalogWriterItems, err := ConstructCatalogWriterItems(*tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetID().TaskId,
		tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetID(), taskTemplate.Metadata.DiscoveryVersion,
		*taskTemplate.Interface, inputReaders, outputReaders)

	// All launched sub-tasks should have written outputs to catalog, mark as success.
	return state, nil
}

func ConstructCatalogWriterItems(keyId idlCore.Identifier, taskExecId idlCore.TaskExecutionIdentifier,
	cacheVersion string, taskInterface idlCore.TypedInterface, inputReaders []io.InputReader,
	outputReaders []io.OutputReader) ([]workqueue.WorkItem, error) {

	writerWorkItems := make([]workqueue.WorkItem, 0, len(inputReaders))

	if len(inputReaders) != len(outputReaders) {
		return nil, errors.Errorf(ErrorInternalMismatch, "Length different building catalog writer items %d %d",
			len(inputReaders), len(outputReaders))
	}

	for idx, input := range inputReaders {
		output := outputReaders[idx]
		wi := catalog.NewWriterWorkItem(workqueue.WorkItemID(input.GetInputPrefixPath()), core.CatalogKey{
			Identifier:     keyId,
			InputReader:    input,
			CacheVersion:   cacheVersion,
			TypedInterface: taskInterface,
		}, output, core.CatalogMetadata{
			TaskExecutionIdentifier: &taskExecId,
		})
		writerWorkItems = append(writerWorkItems, wi)
	}
	return writerWorkItems, nil
}

func NewLiteralScalarOfInteger(number int64) *idlCore.Literal {
	return &idlCore.Literal{
		Value: &idlCore.Literal_Scalar{
			Scalar: &idlCore.Scalar{
				Value: &idlCore.Scalar_Primitive{
					Primitive: &idlCore.Primitive{
						Value: &idlCore.Primitive_Integer{
							Integer: number,
						},
					},
				},
			},
		},
	}
}

func CatalogBitsetToLiteralCollection(catalogResults *bitarray.BitSet) *idlCore.LiteralCollection {
	literals := make([]*idlCore.Literal, 0, catalogResults.Len())
	for i := 0; i < catalogResults.Len(); i++ {
		if !catalogResults.IsSet(uint(i)) {
			literals = append(literals, NewLiteralScalarOfInteger(int64(i)))
		}
	}
	return &idlCore.LiteralCollection{
		Literals: literals,
	}
}

func CheckCatalog(ctx context.Context, catalogReader workqueue.IndexedWorkQueue, workItems []*catalog.ReaderWorkItem) (
	bool, *bitarray.BitSet, int, error) {

	var cachedCount = 0
	catalogResults := bitarray.NewBitSet(uint(len(workItems)))

	// enqueue work items
	for _, w := range workItems {
		err := catalogReader.Queue(w)
		if err != nil {
			return false, catalogResults, cachedCount, errors.Wrapf(ErrorWorkQueue, err,
				"Error enqueuing work item %s", w.GetId())
		}
	}

	// Immediately read back from the work queue, and store results into a bitset if available
	for idx, w := range workItems {
		retrievedItem, found, err := catalogReader.Get(w.GetId())
		if err != nil {
			return false, catalogResults, cachedCount, err
		}
		if !found {
			logger.Warnf(ctx, "Item just placed into Catalog work queue has disappeared")
		}

		if retrievedItem.GetWorkStatus() != workqueue.WorkStatusDone {
			logger.Debugf(ctx, "Found at least one catalog work item unfinished, skipping rest of round. ID %s",
				retrievedItem.GetId())
			return false, catalogResults, cachedCount, nil
		}

		castedItem, ok := retrievedItem.(*catalog.ReaderWorkItem)
		if !ok {
			return false, catalogResults, cachedCount, errors.Errorf(ErrorReaderWorkItemCast,
				"Failed to cast when reading from work queue.")
		}

		if castedItem.IsCached() {
			catalogResults.Set(uint(idx))
			cachedCount++
		}
	}

	return true, catalogResults, cachedCount, nil
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
		indexedInputLocation, err := dataStore.ConstructReference(ctx, inputPrefix, strconv.Itoa(i))
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

	outputWriters := make([]io.OutputWriter, size)

	for i := 0; i < int(size); i++ {
		dataReference, err := dataStore.ConstructReference(ctx, outputPrefix, strconv.Itoa(i))
		if err != nil {
			return outputWriters, err
		}
		writer := ioutils.NewSimpleOutputWriter(ctx, dataStore, ioutils.NewSimpleOutputFilePaths(ctx, dataStore, dataReference))
		outputWriters = append(outputWriters, writer)
	}
	return outputWriters, nil
}

func ConstructOutputReaders(ctx context.Context, dataStore *storage.DataStore, outputPrefix storage.DataReference,
	size int) ([]io.OutputReader, error) {

	outputReaders := make([]io.OutputReader, size)

	for i := 0; i < int(size); i++ {
		dataReference, err := dataStore.ConstructReference(ctx, outputPrefix, strconv.Itoa(i))
		if err != nil {
			return outputReaders, err
		}
		outputPath := ioutils.NewSimpleOutputFilePaths(ctx, dataStore, dataReference)
		reader := ioutils.NewRemoteFileOutputReader(ctx, dataStore, outputPath, int64(999999999))
		outputReaders = append(outputReaders, reader)
	}
	return outputReaders, nil
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
