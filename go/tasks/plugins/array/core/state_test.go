package core

import (
	"context"
	"testing"

	"github.com/lyft/flytestdlib/bitarray"

	idlCore "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/stretchr/testify/assert"
)

func TestGetPhaseVersionOffset(t *testing.T) {
	length := int64(100)
	maxCheckingSubTasks := GetPhaseVersionOffset(PhaseCheckingSubTaskExecutions, length)
	maxPhaseWriteToDiscovery := GetPhaseVersionOffset(PhaseWriteToDiscovery, length)
	assert.NotEqual(t, int(maxCheckingSubTasks), int(maxPhaseWriteToDiscovery))
}

func TestMapArrayStateToPluginPhase(t *testing.T) {
	ctx := context.Background()

	t.Run("start", func(t *testing.T) {
		s := &State{
			CurrentPhase: PhaseStart,
		}

		phaseInfo := MapArrayStateToPluginPhase(ctx, s, []*idlCore.TaskLog{})
		assert.Equal(t, core.PhaseInitializing, phaseInfo.Phase())
	})
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
