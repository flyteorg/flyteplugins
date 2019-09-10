package core

import (
	"fmt"
	"testing"

	"github.com/magiconair/properties/assert"
)

func TestTransitionType_String(t *testing.T) {
	assert.Equal(t, TransitionTypeBestEffort.String(), "BestEffort")
	assert.Equal(t, TransitionTypeBarrier.String(), "Barrier")
	assert.Equal(t, TransitionTypeEphemeral.String(), "Ephemeral")
}

func ExampleTransition_String() {
	trns := DoTransitionType(TransitionTypeBestEffort, PhaseInfoUndefined)
	fmt.Println(trns.String())
	// Output: BestEffort,Phase<undefined:0 <nil> Reason:>
}

func TestDoTransition(t *testing.T) {
	t.Run("unknown", func(t *testing.T) {
		trns := DoTransition(PhaseInfoUndefined)
		assert.Equal(t, TransitionTypeEphemeral, trns.Type())
		assert.Equal(t, PhaseInfoUndefined, trns.Info())
		assert.Equal(t, PhaseUndefined, trns.Info().Phase())
	})

	t.Run("someInfo", func(t *testing.T) {
		pInfo := PhaseInfoSuccess(nil)
		trns := DoTransition(pInfo)
		assert.Equal(t, TransitionTypeEphemeral, trns.Type())
		assert.Equal(t, pInfo, trns.Info())
		assert.Equal(t, PhaseSuccess, trns.Info().Phase())
	})
}

func TestDoTransitionType(t *testing.T) {
	t.Run("unknown", func(t *testing.T) {
		trns := DoTransitionType(TransitionTypeBarrier, PhaseInfoUndefined)
		assert.Equal(t, TransitionTypeBarrier, trns.Type())
		assert.Equal(t, PhaseInfoUndefined, trns.Info())
		assert.Equal(t, PhaseUndefined, trns.Info().Phase())
	})

	t.Run("someInfo", func(t *testing.T) {
		pInfo := PhaseInfoSuccess(nil)
		trns := DoTransitionType(TransitionTypeBestEffort, pInfo)
		assert.Equal(t, TransitionTypeBestEffort, trns.Type())
		assert.Equal(t, pInfo, trns.Info())
		assert.Equal(t, PhaseSuccess, trns.Info().Phase())
	})
}