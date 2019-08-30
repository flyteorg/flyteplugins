package qubole_single

import (
	"github.com/magiconair/properties/assert"
	"testing"
)

func TestInTerminalState(t *testing.T) {
	var stateTests = []struct {
		phase ExecutionPhase
		isTerminal bool
	}{
		{phase: PhaseNotStarted, isTerminal: false},
		{phase: PhaseQueued, isTerminal: false},
		{phase: PhaseSubmitted, isTerminal: false},
		{phase: PhaseQuerySucceeded, isTerminal: true},
		{phase: PhaseQueryFailed, isTerminal: true},
	}

	for _, tt := range stateTests {
		t.Run(tt.phase.String(), func(t *testing.T) {
			e := ExecutionState{Phase: tt.phase}
			res := InTerminalState(e)
			assert.Equal(t, tt.isTerminal, res)
		})
	}
}



func TestIsNotYetSubmitted(t *testing.T) {

}

func TestCopy(t *testing.T) {

}

func TestGetQueryInfo(t *testing.T) {

}


func TestConstructTaskLog(t *testing.T) {

}

func TestConstructTaskInfo(t *testing.T) {

}

func TestMapExecutionStateToPhaseInfo(t *testing.T) {

}
