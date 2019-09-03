package array

import (
	"context"
	"time"

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
	PhaseStart Phase = iota
	PhaseLaunch
	PhaseCheckingSubTaskExecutions
	PhaseWriteToDiscovery
	PhaseSuccess
	PhaseRetryableFailure
	PhasePermanentFailure
)

type State struct {
	currentPhase       Phase
	phaseVersion       uint32
	reason             string
	executionErr       *idlCore.ExecutionError
	executionArraySize int
	originalArraySize  int64
	arrayStatus        arraystatus.ArrayStatus

	// Which sub-tasks to cache, (using the original index, that is, the length is ArrayJob.size)
	writeToCatalog *bitarray.BitSet
}

func (s State) GetReason() string {
	return s.reason
}

func (s State) GetExecutionArraySize() int {
	return s.executionArraySize
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
	s.executionArraySize = size
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
	ErrorK8sArrayGeneric                     = "ARRAY_JOB_GENERIC_FAILURE"
)

func ToArrayJob(structObj *structpb.Struct) (*idlPlugins.ArrayJob, error) {
	arrayJob := &idlPlugins.ArrayJob{}
	err := utils.UnmarshalStruct(structObj, arrayJob)
	return arrayJob, err
}

func GetPhaseVersionOffset(currentPhase Phase, length int64) uint32 {
	// NB: Make sure this is the last/highest value of the Phase!
	return uint32(length * int64(core.PhasePermanentFailure) * int64(currentPhase))
}

// Any state of the plugin needs to map to a core.PhaseInfo (which in turn will map to Admin events) so that the rest
// of the Flyte platform can understand what's happening. That is, each possible state that our plugin state
// machine returns should map to a unique (core.Phase, core.PhaseInfo.version).
// Info fields will always be nil, because we're going to send log links individually. This simplifies our state
// handling as we don't have to keep an ever growing list of log links (our batch jobs can be 5000 sub-tasks, keeping
// all the log links takes up a lot of space).
func MapArrayStateToPluginPhase(_ context.Context, state State) core.PhaseInfo {

	var phaseInfo core.PhaseInfo
	t := time.Now()
	nowTaskInfo := &core.TaskInfo{OccurredAt: &t}

	switch state.currentPhase {
	case PhaseStart:
		phaseInfo = core.PhaseInfoInitializing(t, core.DefaultPhaseVersion, state.GetReason())

	case PhaseLaunch:
		// The first time we return a Running core.Phase, we can just use the version inside the state object itself.
		version := state.phaseVersion
		phaseInfo = core.PhaseInfoRunning(version, nowTaskInfo)

	case PhaseCheckingSubTaskExecutions:
		// For future Running core.Phases, we have to make sure we don't use an earlier Admin version number,
		// which means we need to offset things.
		version := GetPhaseVersionOffset(state.currentPhase, state.originalArraySize) + state.phaseVersion
		phaseInfo = core.PhaseInfoRunning(version, nowTaskInfo)

	case PhaseWriteToDiscovery:
		version := GetPhaseVersionOffset(state.currentPhase, state.originalArraySize) + state.phaseVersion
		phaseInfo = core.PhaseInfoRunning(version, nowTaskInfo)

	case PhaseSuccess:
		phaseInfo = core.PhaseInfoSuccess(nowTaskInfo)

	case PhaseRetryableFailure:
		if state.executionErr != nil {
			phaseInfo = core.PhaseInfoFailed(core.PhaseRetryableFailure, state.executionErr, nowTaskInfo)
		} else {
			phaseInfo = core.PhaseInfoRetryableFailure(ErrorK8sArrayGeneric, state.reason, nowTaskInfo)
		}

	case PhasePermanentFailure:
		if state.executionErr != nil {
			phaseInfo = core.PhaseInfoFailed(core.PhasePermanentFailure, state.executionErr, nowTaskInfo)
		} else {
			phaseInfo = core.PhaseInfoFailure(ErrorK8sArrayGeneric, state.reason, nowTaskInfo)
		}
	}

	return phaseInfo
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
				// TODO: Other option: array tasks are only retryable as a full set and to get single task retriability
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

	// Save this in the state
	state.originalArraySize = arrayJob.Size

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
		state.executionArraySize = int(arrayJob.Size) - cachedCount
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
		*taskTemplate.Interface, state.writeToCatalog, inputReaders, outputReaders)

	if len(catalogWriterItems) == 0 {
		state.currentPhase = PhaseSuccess
	}

	allWritten, err := WriteToCatalog(ctx, catalogWriter, catalogWriterItems)
	if allWritten {
		state.currentPhase = PhaseSuccess
	}

	return state, nil
}

func WriteToCatalog(ctx context.Context, catalogWriter workqueue.IndexedWorkQueue,
	workItems []*catalog.WriterWorkItem) (bool, error) {

	// Enqueue work items
	for _, w := range workItems {
		err := catalogWriter.Queue(w)
		if err != nil {
			return false, errors.Wrapf(ErrorWorkQueue, err,
				"Error enqueuing work item %s", w.GetId())
		}
	}

	// Immediately read back from the work queue, and see if it's done.
	var allDone = true
	for _, w := range workItems {
		retrievedItem, found, err := catalogWriter.Get(w.GetId())
		if err != nil {
			return false, err
		}
		if !found {
			logger.Warnf(ctx, "Item just placed into Catalog work queue has disappeared")
			allDone = false
			continue
		}

		if retrievedItem.GetWorkStatus() != workqueue.WorkStatusDone {
			logger.Debugf(ctx, "Found at least one catalog work item unfinished, skipping rest of round. ID %s",
				retrievedItem.GetId())
			return false, nil
		}
	}

	return allDone, nil
}

func ConstructCatalogWriterItems(keyId idlCore.Identifier, taskExecId idlCore.TaskExecutionIdentifier,
	cacheVersion string, taskInterface idlCore.TypedInterface, whichTasksToCache *bitarray.BitSet,
	inputReaders []io.InputReader, outputReaders []io.OutputReader) ([]*catalog.WriterWorkItem, error) {

	writerWorkItems := make([]*catalog.WriterWorkItem, 0, len(inputReaders))

	if len(inputReaders) != len(outputReaders) {
		return nil, errors.Errorf(ErrorInternalMismatch, "Length different building catalog writer items %d %d",
			len(inputReaders), len(outputReaders))
	}

	for idx, input := range inputReaders {
		if !whichTasksToCache.IsSet(uint(idx)) {
			continue
		}
		output := outputReaders[idx]
		wi := catalog.NewWriterWorkItem(workqueue.WorkItemID(input.GetInputPrefixPath()), catalog.Key{
			Identifier:     keyId,
			InputReader:    input,
			CacheVersion:   cacheVersion,
			TypedInterface: taskInterface,
		}, output, catalog.Metadata{
			TaskExecutionIdentifier: &taskExecId,
		})
		writerWorkItems = append(writerWorkItems, &wi)
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
