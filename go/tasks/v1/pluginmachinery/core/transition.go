package core

type TransitionType int

const (
	TransitionTypeEphemeral TransitionType = iota
	TransitionTypeBestEffort
	TransitionTypeBarrier
)

type TransitionTo int

const (
	TransitionToNoTransition TransitionTo = iota
	TransitionToSuccess
	TransitionToRetryableFailure
	TransitionToPermanentFailure
)

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

var UnknownTransition = Transition{TransitionTypeEphemeral, TransitionToNoTransition, PhaseInfo{}}

func DoTransitionType(ttype TransitionType, info PhaseInfo) Transition {
	t := Transition{ttype: ttype, info: info}
	switch info.phase {
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

func DoTransition(info PhaseInfo) Transition {
	return DoTransitionType(TransitionTypeEphemeral, info)
}
