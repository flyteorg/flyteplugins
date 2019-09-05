package array

import (
	"context"
	"time"

	"github.com/lyft/flyteplugins/go/tasks/array/arraystatus"
	"github.com/lyft/flyteplugins/go/tasks/array/bitarray"

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

func (s State) GetPhase() (phase Phase, version uint32) {
	return s.currentPhase, s.phaseVersion
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

func (s *State) SetPhase(phase Phase, phaseVersion uint32) *State {
	s.currentPhase = phase
	s.phaseVersion = phaseVersion
	return s
}

func (s *State) SetArrayStatus(state arraystatus.ArrayStatus) *State {
	s.arrayStatus = state
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
