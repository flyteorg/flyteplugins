package core

// Type of Transition, refer to Transition to understand what transition means
type TransitionType int

const (
	// The transition is eventually consistent. For all the state written may not be visible in the next call, but eventually will persist
	// Best to use when the plugin logic is completely idempotent. This is also the most performant option.
	TransitionTypeEphemeral TransitionType = iota
	// This transition tries its best to make the latest state visible for every consecutive read. But, it is possible to go back in time,
	// i.e. monotonic consistency is violated (in rare cases).
	TransitionTypeBestEffort
	// This maintains read after write consistency. But, it is still possible that the write fails and the same transition may re-occur.
	TransitionTypeBarrier
)

type TransitionTo int

const (
	// This transition indicates that the plugin is still handling the task and Plugin.Handle should be continually invoked.
	// If the Phase or phase version has changed the state will be stored, but otherwise its like a no-op/no-change
	TransitionToNoTransition TransitionTo = iota
	// Completes the call from handle. The next call (when depends on the TransitionType) will be Finalize, handle -> finalize -> success
	TransitionToSuccess
	// Completes a call from handle -> finalize and then handle again
	TransitionToRetryableFailure
	// Completes a call from handle -> finalize -> failure
	TransitionToPermanentFailure
)

// A Plugin Handle method returns a Transition. This transition indicates to the Flyte framework that if the plugin wants to continue "Handle"ing this task,
// or if wants to move the task to success, attempt a retry or fail. The transition automatically sends an event to Admin service which shows the plugin
// provided information in the Console/cli etc
// The information to be published is in the PhaseInfo structure. Transition Type indicates the type of consistency for subsequent handle calls and transitionTo
// indicates if the transition is to a different state out of the handle method.
// the PhaseInfo structure is very important and is used to record events in Admin. Only if the Phase + PhaseVersion was not previously observed, will an event be published to Admin
// there are only a configurable number of phaseversion usable. Usually it is preferred to be a monotonically increasing sequence
type Transition struct {
	ttype        TransitionType
	transitionTo TransitionTo
	info         PhaseInfo
}

func (t Transition) Type() TransitionType {
	return t.ttype
}

func (t Transition) ToState() TransitionTo {
	return t.transitionTo
}

func (t Transition) Info() PhaseInfo {
	return t.info
}

// Unknown/Undefined transition. To be returned when an error is observed
var UnknownTransition = Transition{TransitionTypeEphemeral, TransitionToNoTransition, PhaseInfoUndefined}

// Creates and returns a new Transition based on the PhaseInfo.Phase
// Phases: PhaseNotReady, PhaseQueued, PhaseInitializing, PhaseRunning will cause the system to continue invoking Handle
func DoTransitionType(ttype TransitionType, info PhaseInfo) Transition {
	t := Transition{ttype: ttype, info: info}
	switch phase {
	case PhaseNotReady, PhaseQueued, PhaseInitializing, PhaseRunning:
		t.transitionTo = TransitionToNoTransition
	case PhaseSuccess:
		t.transitionTo = TransitionToSuccess
	case PhaseRetryableFailure:
		t.transitionTo = TransitionToRetryableFailure
	case PhasePermanentFailure:
		t.transitionTo = TransitionToPermanentFailure
	case PhaseUndefined:
		fallthrough
	default:
		t = UnknownTransition
	}
	return t
}

// Same as DoTransition, but TransitionTime is always Ephemeral
func DoTransition(info PhaseInfo) Transition {
	return DoTransitionType(TransitionTypeEphemeral, info)
}
