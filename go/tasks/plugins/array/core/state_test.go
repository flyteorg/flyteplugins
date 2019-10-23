package core

import (
	"context"
	"testing"

	"github.com/lyft/flytestdlib/bitarray"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/stretchr/testify/assert"
)

func TestGetPhaseVersionOffset(t *testing.T) {
	length := int64(100)
	checkSubTasksOffset := GetPhaseVersionOffset(PhaseAssembleFinalOutput, length)
	discoverWriteOffset := GetPhaseVersionOffset(PhaseWriteToDiscovery, length)
	// There are 9 possible core.Phases, from PhaseUndefined to PhasePermanentFailure
	assert.Equal(t, uint32(length*9), discoverWriteOffset-checkSubTasksOffset)
}

func TestNewLiteralScalarOfInteger(t *testing.T) {

}

func TestCatalogBitsetToLiteralCollection(t *testing.T) {

}

func TestInvertBitSet(t *testing.T) {
	input := bitarray.NewBitSet(4)
	input.Set(0)
	input.Set(2)
	input.Set(3)

	expected := bitarray.NewBitSet(4)
	expected.Set(1)
	expected.Set(4)

	actual := InvertBitSet(input)
	assertBitSetsEqual(t, expected, actual, 4)
}

func assertBitSetsEqual(t testing.TB, b1, b2 *bitarray.BitSet, len int) {
	if b1 == nil {
		assert.Nil(t, b2)
	} else if b2 == nil {
		assert.FailNow(t, "b2 must not be nil")
	}

	assert.Equal(t, b1.Cap(), b2.Cap())
	for i := uint(0); i < uint(len); i++ {
		assert.Equal(t, b1.IsSet(i), b2.IsSet(i), "At index %v", i)
	}
}

func TestMapArrayStateToPluginPhase(t *testing.T) {
	ctx := context.Background()

	t.Run("start", func(t *testing.T) {
		s := State{
			CurrentPhase: PhaseStart,
		}
		phaseInfo := MapArrayStateToPluginPhase(ctx, &s, nil)
		assert.Equal(t, core.PhaseInitializing, phaseInfo.Phase())
	})

	t.Run("launch", func(t *testing.T) {
		s := State{
			CurrentPhase: PhaseLaunch,
			PhaseVersion: 0,
		}
		phaseInfo := MapArrayStateToPluginPhase(ctx, &s, nil)
		assert.Equal(t, core.PhaseRunning, phaseInfo.Phase())
	})

	t.Run("monitoring subtasks", func(t *testing.T) {
		s := State{
			CurrentPhase:       PhaseCheckingSubTaskExecutions,
			PhaseVersion:       8,
			OriginalArraySize:  10,
			ExecutionArraySize: 5,
		}
		phaseInfo := MapArrayStateToPluginPhase(ctx, &s, nil)
		assert.Equal(t, core.PhaseRunning, phaseInfo.Phase())
		assert.Equal(t, uint32(188), phaseInfo.Version())
	})

	t.Run("write to discovery", func(t *testing.T) {
		s := State{
			CurrentPhase:       PhaseWriteToDiscovery,
			PhaseVersion:       8,
			OriginalArraySize:  10,
			ExecutionArraySize: 5,
		}
		phaseInfo := MapArrayStateToPluginPhase(ctx, &s, nil)
		assert.Equal(t, core.PhaseRunning, phaseInfo.Phase())
		assert.Equal(t, uint32(278), phaseInfo.Version())
	})

	t.Run("success", func(t *testing.T) {
		s := State{
			CurrentPhase: PhaseSuccess,
			PhaseVersion: 0,
		}
		phaseInfo := MapArrayStateToPluginPhase(ctx, &s, nil)
		assert.Equal(t, core.PhaseSuccess, phaseInfo.Phase())
	})

	t.Run("retryable failure", func(t *testing.T) {
		s := State{
			CurrentPhase: PhaseRetryableFailure,
			PhaseVersion: 0,
		}
		phaseInfo := MapArrayStateToPluginPhase(ctx, &s, nil)
		assert.Equal(t, core.PhaseRetryableFailure, phaseInfo.Phase())
	})

	t.Run("permanent failure", func(t *testing.T) {
		s := State{
			CurrentPhase: PhasePermanentFailure,
			PhaseVersion: 0,
		}
		phaseInfo := MapArrayStateToPluginPhase(ctx, &s, nil)
		assert.Equal(t, core.PhasePermanentFailure, phaseInfo.Phase())
	})
}
