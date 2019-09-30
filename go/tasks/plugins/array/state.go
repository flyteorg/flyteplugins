package array

import (
	"context"
	"time"

	"github.com/lyft/flyteplugins/go/tasks/plugins/array/arraystatus"
	"github.com/lyft/flytestdlib/bitarray"

	structpb "github.com/golang/protobuf/ptypes/struct"
	idlCore "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	idlPlugins "github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"
	"github.com/lyft/flyteplugins/go/tasks/errors"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/utils"
	"github.com/lyft/flytestdlib/logger"
)

type Phase uint8

const (
	PhaseStart Phase = iota
	PhasePreLaunch
	PhaseLaunch
	PhaseCheckingSubTaskExecutions
	PhaseWriteToDiscovery
	PhaseSuccess
	PhaseRetryableFailure
	PhasePermanentFailure
)

type State interface {
	GetReason() string
	GetExecutionErr() *idlCore.ExecutionError
	GetExecutionArraySize() int
	GetPhase() (phase Phase, version uint32)
	GetArrayStatus() arraystatus.ArrayStatus
	GetOriginalArraySize() int64
	GetOriginalMinSuccesses() int64
	GetIndexesToCache() *bitarray.BitSet

	SetReason(reason string) State
	SetExecutionErr(err *idlCore.ExecutionError) State
	SetActualArraySize(size int) State
	SetPhase(phase Phase, phaseVersion uint32) State
	SetArrayStatus(state arraystatus.ArrayStatus) State
	SetOriginalArraySize(size int64) State
	SetOriginalMinSuccesses(size int64) State
	SetIndexesToCache(set *bitarray.BitSet) State
}

type StateImpl struct {
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
}

func (s StateImpl) GetReason() string {
	return s.Reason
}

func (s StateImpl) GetExecutionArraySize() int {
	return s.ExecutionArraySize
}

func (s StateImpl) GetPhase() (phase Phase, version uint32) {
	return s.CurrentPhase, s.PhaseVersion
}

func (s StateImpl) GetArrayStatus() arraystatus.ArrayStatus {
	return s.ArrayStatus
}

func (s *StateImpl) GetOriginalArraySize() int64 {
	return s.OriginalArraySize
}

func (s *StateImpl) GetOriginalMinSuccesses() int64 {
	return s.OriginalMinSuccesses
}

func (s *StateImpl) GetIndexesToCache() *bitarray.BitSet {
	return s.IndexesToCache
}

func (s *StateImpl) GetExecutionErr() *idlCore.ExecutionError {
	return s.ExecutionErr
}

func (s *StateImpl) SetExecutionErr(err *idlCore.ExecutionError) State {
	s.ExecutionErr = err
	return s
}

func (s *StateImpl) SetIndexesToCache(set *bitarray.BitSet) State {
	s.IndexesToCache = set
	return s
}

func (s *StateImpl) SetOriginalArraySize(size int64) State {
	s.OriginalArraySize = size
	return s
}

func (s *StateImpl) SetOriginalMinSuccesses(size int64) State {
	s.OriginalMinSuccesses = size
	return s
}

func (s *StateImpl) SetReason(reason string) State {
	s.Reason = reason
	return s
}

func (s *StateImpl) SetActualArraySize(size int) State {
	s.ExecutionArraySize = size
	return s
}

func (s *StateImpl) SetPhase(phase Phase, phaseVersion uint32) State {
	s.CurrentPhase = phase
	s.PhaseVersion = phaseVersion
	return s
}

func (s *StateImpl) SetArrayStatus(state arraystatus.ArrayStatus) State {
	s.ArrayStatus = state
	return s
}

const (
	ErrorWorkQueue        errors.ErrorCode = "CATALOG_READER_QUEUE_FAILED"
	ErrorInternalMismatch                  = "ARRAY_MISMATCH"
	ErrorK8sArrayGeneric                   = "ARRAY_JOB_GENERIC_FAILURE"
)

func ToArrayJob(structObj *structpb.Struct) (*idlPlugins.ArrayJob, error) {
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
func MapArrayStateToPluginPhase(_ context.Context, state State) core.PhaseInfo {

	var phaseInfo core.PhaseInfo
	t := time.Now()
	nowTaskInfo := &core.TaskInfo{OccurredAt: &t}

	switch p, version := state.GetPhase(); p {
	case PhaseStart:
		phaseInfo = core.PhaseInfoInitializing(t, core.DefaultPhaseVersion, state.GetReason())

	case PhaseLaunch:
		// The first time we return a Running core.Phase, we can just use the version inside the state object itself.
		phaseInfo = core.PhaseInfoRunning(version, nowTaskInfo)

	case PhaseCheckingSubTaskExecutions:
		// For future Running core.Phases, we have to make sure we don't use an earlier Admin version number,
		// which means we need to offset things.
		version := GetPhaseVersionOffset(p, state.GetOriginalArraySize()) + version
		phaseInfo = core.PhaseInfoRunning(version, nowTaskInfo)

	case PhaseWriteToDiscovery:
		version := GetPhaseVersionOffset(p, state.GetOriginalArraySize()) + version
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
			phaseInfo = core.PhaseInfoFailure(ErrorK8sArrayGeneric, state.GetReason(), nowTaskInfo)
		}
	}

	return phaseInfo
}

func SummaryToPhase(ctx context.Context, minSuccesses int64, summary arraystatus.ArraySummary) Phase {
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

func invertBitSet(input *bitarray.BitSet) *bitarray.BitSet {
	output := bitarray.NewBitSet(input.Cap())
	for i := uint(0); i < input.Cap(); i++ {
		if !input.IsSet(i) {
			output.Set(i)
		}
	}

	return output
}
