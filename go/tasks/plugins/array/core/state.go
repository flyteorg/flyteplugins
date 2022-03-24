package core

import (
	"context"
	"fmt"
	"time"

	"github.com/flyteorg/flytestdlib/errors"

	"github.com/flyteorg/flyteplugins/go/tasks/plugins/array/arraystatus"
	"github.com/flyteorg/flytestdlib/bitarray"

	idlCore "github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	idlPlugins "github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/plugins"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/utils"
	"github.com/flyteorg/flytestdlib/logger"
	structpb "github.com/golang/protobuf/ptypes/struct"
)

//go:generate mockery -all -case=underscore
//go:generate enumer -type=Phase

type Phase uint8

const (
	PhaseStart Phase = iota
	PhasePreLaunch
	PhaseLaunch
	PhaseWaitingForResources
	PhaseCheckingSubTaskExecutions
	PhaseAssembleFinalOutput
	PhaseWriteToDiscovery
	PhaseWriteToDiscoveryThenFail
	PhaseSuccess
	PhaseAssembleFinalError
	PhaseRetryableFailure
	PhasePermanentFailure
)

type State struct {
	CurrentPhase         Phase                   `json:"phase"`
	PhaseVersion         uint32                  `json:"phaseVersion"`
	Reason               string                  `json:"reason"`
	ExecutionErr         *idlCore.ExecutionError `json:"err"`
	ExecutionArraySize   int                     `json:"arraySize"`
	OriginalArraySize    int64                   `json:"originalArraySize"`
	ArrayStatus          arraystatus.ArrayStatus `json:"arrayStatus"`
	OriginalMinSuccesses int64                   `json:"minSuccess"`

	// Which sub-tasks to cache, (using the original index, that is, the length is ArrayJob.size)
	IndexesToCache *bitarray.BitSet `json:"indexesToCache"`

	// Tracks the number of subtask retries using the execution index
	RetryAttempts bitarray.CompactArray `json:"retryAttempts"`
}

func (s State) GetReason() string {
	return s.Reason
}

func (s State) GetExecutionArraySize() int {
	return s.ExecutionArraySize
}

func (s State) GetPhase() (phase Phase, version uint32) {
	return s.CurrentPhase, s.PhaseVersion
}

func (s State) GetArrayStatus() arraystatus.ArrayStatus {
	return s.ArrayStatus
}

func (s *State) GetOriginalArraySize() int64 {
	return s.OriginalArraySize
}

func (s *State) GetOriginalMinSuccesses() int64 {
	return s.OriginalMinSuccesses
}

func (s *State) GetIndexesToCache() *bitarray.BitSet {
	return s.IndexesToCache
}

func (s *State) GetExecutionErr() *idlCore.ExecutionError {
	return s.ExecutionErr
}

func (s *State) SetExecutionErr(err *idlCore.ExecutionError) *State {
	s.ExecutionErr = err
	return s
}

func (s *State) SetIndexesToCache(set *bitarray.BitSet) *State {
	s.IndexesToCache = set
	return s
}

func (s *State) SetOriginalArraySize(size int64) *State {
	s.OriginalArraySize = size
	return s
}

func (s *State) SetOriginalMinSuccesses(size int64) *State {
	s.OriginalMinSuccesses = size
	return s
}

func (s *State) SetReason(reason string) *State {
	s.Reason = reason
	return s
}

func (s *State) SetRetryAttempts(retryAttempts bitarray.CompactArray) *State {
	s.RetryAttempts = retryAttempts
	return s
}

func (s *State) SetExecutionArraySize(size int) *State {
	s.ExecutionArraySize = size
	return s
}

func (s *State) SetPhase(phase Phase, phaseVersion uint32) *State {
	s.CurrentPhase = phase
	s.PhaseVersion = phaseVersion
	return s
}

func (s *State) SetArrayStatus(state arraystatus.ArrayStatus) *State {
	s.ArrayStatus = state
	return s
}

const (
	ErrorWorkQueue        errors.ErrorCode = "CATALOG_READER_QUEUE_FAILED"
	ErrorInternalMismatch errors.ErrorCode = "ARRAY_MISMATCH"
	ErrorK8sArrayGeneric  errors.ErrorCode = "ARRAY_JOB_GENERIC_FAILURE"
)

func ToArrayJob(structObj *structpb.Struct, taskTypeVersion int32) (*idlPlugins.ArrayJob, error) {
	if structObj == nil {
		if taskTypeVersion == 0 {
			return &idlPlugins.ArrayJob{
				Parallelism: 0,
				Size:        1,
				SuccessCriteria: &idlPlugins.ArrayJob_MinSuccesses{
					MinSuccesses: 1,
				},
			}, nil
		}
		return &idlPlugins.ArrayJob{
			Parallelism: 0,
			Size:        1,
			SuccessCriteria: &idlPlugins.ArrayJob_MinSuccessRatio{
				MinSuccessRatio: 1.0,
			},
		}, nil
	}

	arrayJob := &idlPlugins.ArrayJob{}
	err := utils.UnmarshalStruct(structObj, arrayJob)
	return arrayJob, err
}

func GetPhaseVersionOffset(currentPhase Phase, length int64) uint32 {
	// NB: Make sure this is the last/highest value of the Phase!
	return uint32(length * (int64(core.PhasePermanentFailure) + 1) * int64(currentPhase))
}

// Any state of the plugin needs to map to a core.PhaseInfo (which in turn will map to Admin events) so that the rest
// of the Flyte platform can understand what's happening. That is, each possible state that our plugin state
// machine returns should map to a unique (core.Phase, core.PhaseInfo.version).
// Info fields will always be nil, because we're going to send log links individually. This simplifies our state
// handling as we don't have to keep an ever growing list of log links (our batch jobs can be 5000 sub-tasks, keeping
// all the log links takes up a lot of space).
func MapArrayStateToPluginPhase(_ context.Context, state *State, logLinks []*idlCore.TaskLog, subTaskMetadata []*SubTaskMetadata) (core.PhaseInfo, error) {
	phaseInfo := core.PhaseInfoUndefined
	t := time.Now()

	nowTaskInfo := &core.TaskInfo{
		OccurredAt:        &t,
		Logs:              logLinks,
		ExternalResources: make([]*core.ExternalResource, len(subTaskMetadata)),
	}

	for i, subTask := range subTaskMetadata {
		//var cacheStatus idlCore.CatalogCacheStatus
		phase := core.PhaseUndefined
		retryAttempt := uint32(0)

		if state.GetIndexesToCache().IsSet(uint(subTask.OriginalIndex)) {
			// task has been cached
			//cacheStatus = idlCore.CatalogCacheStatus_CACHE_HIT
			phase = core.PhaseSuccess
		} else {
			if subTask.ChildIndex < 0 || subTask.ChildIndex >= state.GetExecutionArraySize() {
				// TODO hamersaw - error warn!
				continue
			}

			// use default values if state has not been initialized yet
			if subTask.ChildIndex <= len(state.RetryAttempts.GetItems()) {
				retryAttempt = uint32(state.RetryAttempts.GetItem(subTask.ChildIndex))
			}
			if subTask.ChildIndex <= len(state.ArrayStatus.Detailed.GetItems()) {
				phase = core.Phases[state.ArrayStatus.Detailed.GetItem(subTask.ChildIndex)]
			}

			// TODO hamersaw - do we need to set CachePopulated if the task was cachable and a success?
		}

		nowTaskInfo.ExternalResources[i] = &core.ExternalResource{
			ExternalID:   *subTask.SubTaskID,
			//CacheStatus:  cacheStatus,
			Index:        uint32(subTask.OriginalIndex),
			//Logs:         subTask.Logs,
			RetryAttempt: uint32(retryAttempt),
			Phase:        phase,
		}
	}

	switch p, version := state.GetPhase(); p {
	case PhaseStart:
		phaseInfo = core.PhaseInfoInitializing(t, core.DefaultPhaseVersion, state.GetReason(), &core.TaskInfo{OccurredAt: &t})

	case PhasePreLaunch:
		version := GetPhaseVersionOffset(p, 1) + version
		phaseInfo = core.PhaseInfoRunning(version, nowTaskInfo)

	case PhaseLaunch:
		// The first time we return a Running core.Phase, we can just use the version inside the state object itself.
		phaseInfo = core.PhaseInfoRunning(version, nowTaskInfo)

	case PhaseWaitingForResources:
		phaseInfo = core.PhaseInfoWaitingForResourcesInfo(t, version, state.GetReason(), nowTaskInfo)

	case PhaseCheckingSubTaskExecutions:
		// For future Running core.Phases, we have to make sure we don't use an earlier Admin version number,
		// which means we need to offset things.
		fallthrough

	case PhaseAssembleFinalOutput:
		fallthrough

	case PhaseAssembleFinalError:
		fallthrough

	case PhaseWriteToDiscoveryThenFail:
		fallthrough

	case PhaseWriteToDiscovery:
		// If the array task has 0 inputs we need to ensure the phaseVersion changes so that the
		// task can progess. Therefore we default to task length 1 to ensure phase updates.
		length := int64(1)
		if state.GetOriginalArraySize() != 0 {
			length = state.GetOriginalArraySize()
		}

		version := GetPhaseVersionOffset(p, length) + version
		phaseInfo = core.PhaseInfoRunning(version, nowTaskInfo)

	case PhaseSuccess:
		phaseInfo = core.PhaseInfoSuccess(nowTaskInfo)

	case PhaseRetryableFailure:
		if state.GetExecutionErr() != nil {
			phaseInfo = core.PhaseInfoFailed(core.PhaseRetryableFailure, state.GetExecutionErr(), nowTaskInfo)
		} else {
			phaseInfo = core.PhaseInfoRetryableFailure(ErrorK8sArrayGeneric, state.GetReason(), nowTaskInfo)
		}

	case PhasePermanentFailure:
		if state.GetExecutionErr() != nil {
			phaseInfo = core.PhaseInfoFailed(core.PhasePermanentFailure, state.GetExecutionErr(), nowTaskInfo)
		} else {
			phaseInfo = core.PhaseInfoSystemFailure(ErrorK8sArrayGeneric, state.GetReason(), nowTaskInfo)
		}
	default:
		return phaseInfo, fmt.Errorf("failed to map custom state phase to core phase. State Phase [%v]", p)
	}

	return phaseInfo, nil
}

func SummaryToPhase(ctx context.Context, minSuccesses int64, summary arraystatus.ArraySummary) Phase {
	totalCount := int64(0)
	totalSuccesses := int64(0)
	totalPermanentFailures := int64(0)
	totalRetryableFailures := int64(0)
	totalRunning := int64(0)
	totalWaitingForResources := int64(0)
	for phase, count := range summary {
		totalCount += count

		switch phase {
		case core.PhaseSuccess:
			totalSuccesses += count
		case core.PhasePermanentFailure:
			totalPermanentFailures += count
		case core.PhaseRetryableFailure:
			totalRetryableFailures += count
		case core.PhaseWaitingForResources:
			totalWaitingForResources += count
		default:
			totalRunning += count
		}
	}

	if totalCount < minSuccesses {
		logger.Infof(ctx, "Array failed because totalCount[%v] < minSuccesses[%v]", totalCount, minSuccesses)
		return PhaseWriteToDiscoveryThenFail
	}

	// No chance to reach the required success numbers.
	if totalRunning+totalSuccesses+totalWaitingForResources+totalRetryableFailures < minSuccesses {
		logger.Infof(ctx, "Array failed early because total failures > minSuccesses[%v]. Snapshot totalRunning[%v] + totalSuccesses[%v] + totalWaitingForResource[%v] + totalRetryableFailures[%v]",
			minSuccesses, totalRunning, totalSuccesses, totalWaitingForResources, totalRetryableFailures)
		return PhaseWriteToDiscoveryThenFail
	}

	if totalWaitingForResources > 0 {
		logger.Infof(ctx, "Array is still running and waiting for resources totalWaitingForResources[%v]", totalWaitingForResources)
		return PhaseWaitingForResources
	}
	if totalSuccesses >= minSuccesses && totalRunning == 0 {
		logger.Infof(ctx, "Array succeeded because totalSuccesses[%v] >= minSuccesses[%v]", totalSuccesses, minSuccesses)
		return PhaseWriteToDiscovery
	}

	logger.Debugf(ctx, "Array is still running [Successes: %v, PermanentFailures: %v, RetryableFailures: %v, Total: %v, MinSuccesses: %v]",
		totalSuccesses, totalPermanentFailures, totalRetryableFailures, totalCount, minSuccesses)
	return PhaseCheckingSubTaskExecutions
}

func InvertBitSet(input *bitarray.BitSet, limit uint) *bitarray.BitSet {
	output := bitarray.NewBitSet(limit)
	for i := uint(0); i < limit; i++ {
		if !input.IsSet(i) {
			output.Set(i)
		}
	}

	return output
}

func NewPhasesCompactArray(count uint) bitarray.CompactArray {
	// TODO: This is fragile, we should introduce a TaskPhaseCount as the last element in the enum
	a, err := bitarray.NewCompactArray(count, bitarray.Item(len(core.Phases)-1))
	if err != nil {
		logger.Warnf(context.Background(), "Failed to create compact array with provided parameters [count: %v]",
			count)
		return bitarray.CompactArray{}
	}

	return a
}

// CalculateOriginalIndex computes the original index of a sub-task.
func CalculateOriginalIndex(childIdx int, toCache *bitarray.BitSet) int {
	var sum = 0
	for i := uint(0); i < toCache.Cap(); i++ {
		if toCache.IsSet(i) {
			sum++
		}

		if childIdx+1 == sum {
			return int(i)
		}
	}

	return -1
}
