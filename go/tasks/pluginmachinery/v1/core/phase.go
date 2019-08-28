package core

import (
	"time"

	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
)

const DefaultPhaseVersion = uint8(0)

type Phase int8

const (
	// Does not mean an error, but simply states that we dont know the state in this round, try again later. But can be used to signal a system error too
	PhaseUndefined Phase = iota
	PhaseNotReady
	// Indicates plugin is not ready to submit the request as it is waiting for resources
	PhaseWaitingForResources
	// Indicates plugin has submitted the execution, but it has not started executing yet
	PhaseQueued
	// The system has started the pre-execution process, like container download, cluster startup etc
	PhaseInitializing
	// Indicates that the task has started executing
	PhaseRunning
	// Indicates that the task has completed successfully
	PhaseSuccess
	// Indicates that the Failure is recoverable, by re-executing the task if retries permit
	PhaseRetryableFailure
	// Indicate that the failure is non recoverable even if retries exist
	PhasePermanentFailure
)

func (p Phase) String() string {
	switch p {
	case PhaseNotReady:
		return "not-ready"
	case PhaseQueued:
		return "queued"
	case PhaseInitializing:
		return "init"
	case PhaseSuccess:
		return "success"
	case PhasePermanentFailure:
		return "perma-fail"
	case PhaseRetryableFailure:
		return "retryable-failure"
	case PhaseRunning:
		return "running"
	case PhaseUndefined:
		fallthrough
	default:
		return "undefined"
	}
}

// Returns true if the given phase is failure, retryable failure or success
func (p Phase) IsTerminal() bool {
	return p == PhasePermanentFailure || p == PhaseRetryableFailure || p == PhaseSuccess
}


type TaskInfo struct {
	// log information for the task execution
	Logs []*core.TaskLog
	// Set this value to the intended time when the status occurred at. If not provided, will be defaulted to the current
	// time at the time of publishing the event.
	OccurredAt *time.Time
	// Custom Event information that the plugin would like to expose to the front-end
	CustomInfo *structpb.Struct
}

// Additional info that should be sent to the front end. The Information is sent to the front-end if it meets certain
// criterion, for example currently, it is sent only if an event was not already sent for
type PhaseInfo struct {
	// Observed Phase of the launched Task execution
	phase Phase
	// Phase version. by default this can be left as empty => 0. This can be used if there is some additional information
	// to be provided to the Control plane. Phase information is immutable in control plane for a given Phase, unless
	// a new version is provided.
	version uint8
	// In case info needs to be provided
	info *TaskInfo
	// If only an error is observed. It is complementary to info
	err *core.ExecutionError
	// reason why the current phase exists.
	reason string
}

func (p *PhaseInfo) Phase() Phase {
	return p.phase
}

func (p *PhaseInfo) Version() uint8 {
	return p.version
}

func (p *PhaseInfo) Reason() string {
	return p.reason
}

func (p *PhaseInfo) Info() *TaskInfo {
	return p.info
}

func (p *PhaseInfo) Err() *core.ExecutionError {
	return p.err
}

// Undefined entity, associated with an error
var PhaseInfoUndefined = PhaseInfo{phase: PhaseUndefined}

func phaseInfo(p Phase, v uint8, err *core.ExecutionError, info *TaskInfo) PhaseInfo {
	if info == nil {
		t := time.Now()
		info = &TaskInfo{
			OccurredAt: &t,
		}
	}
	return PhaseInfo{
		phase:   p,
		version: v,
		info:    info,
		err:     err,
	}
}

// Return in the case the plugin is not ready to start
func PhaseInfoNotReady(t time.Time, version uint8, reason string) PhaseInfo {
	pi := phaseInfo(PhaseNotReady, version, nil, &TaskInfo{OccurredAt: &t})
	pi.reason = reason
	return pi
}

// Return in the case the plugin is not ready to start
func PhaseInfoWaitingForResources(t time.Time, version uint8, reason string) PhaseInfo {
	pi := phaseInfo(PhaseWaitingForResources, version, nil, &TaskInfo{OccurredAt: &t})
	pi.reason = reason
	return pi
}

func PhaseInfoQueued(t time.Time, version uint8, reason string) PhaseInfo {
	pi := phaseInfo(PhaseQueued, version, nil, &TaskInfo{OccurredAt: &t})
	pi.reason = reason
	return pi
}

func PhaseInfoInitializing(t time.Time, version uint8, reason string) PhaseInfo {
	pi := phaseInfo(PhaseInitializing, version, nil, &TaskInfo{OccurredAt: &t})
	pi.reason = reason
	return pi
}

func PhaseInfoFailed(p Phase, err *core.ExecutionError, info *TaskInfo) PhaseInfo {
	if err == nil {
		err = &core.ExecutionError{
			Code:    "Unknown",
			Message: "Unknown error message",
		}
	}
	return phaseInfo(p, DefaultPhaseVersion, err, info)
}

func PhaseInfoRunning(version uint8, info *TaskInfo) PhaseInfo {
	return phaseInfo(PhaseRunning, version, nil, info)
}

func PhaseInfoSuccess(info *TaskInfo) PhaseInfo {
	return phaseInfo(PhaseSuccess, DefaultPhaseVersion, nil, info)
}

func PhaseInfoFailure(code, reason string, info *TaskInfo) PhaseInfo {
	return PhaseInfoFailed(PhasePermanentFailure, &core.ExecutionError{Code: code, Message: reason}, info)
}

func PhaseInfoRetryableFailure(code, reason string, info *TaskInfo) PhaseInfo {
	return PhaseInfoFailed(PhaseRetryableFailure, &core.ExecutionError{Code: code, Message: reason}, info)
}
