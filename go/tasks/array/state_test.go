package array

import (
	"context"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetPhaseVersionOffset(t *testing.T) {
	length := int64(100)
	checkSubTasksOffset := GetPhaseVersionOffset(PhaseCheckingSubTaskExecutions, length)
	discoverWriteOffset := GetPhaseVersionOffset(PhaseWriteToDiscovery, length)
	// There are 9 possible core.Phases, from PhaseUndefined to PhasePermanentFailure
	assert.Equal(t, uint32(length*9), discoverWriteOffset-checkSubTasksOffset)
}

func TestMapArrayStateToPluginPhase(t *testing.T) {
	ctx := context.Background()

	t.Run("start", func(t *testing.T) {
		s := State{
			CurrentPhase: PhaseStart,
		}
		phaseInfo := MapArrayStateToPluginPhase(ctx, s)
		assert.Equal(t, core.PhaseInitializing, phaseInfo.Phase())
	})

	t.Run("launch", func(t *testing.T) {
		s := State{
			CurrentPhase: PhaseLaunch,
			PhaseVersion: 0,
		}
		phaseInfo := MapArrayStateToPluginPhase(ctx, s)
		assert.Equal(t, core.PhaseRunning, phaseInfo.Phase())
	})

	t.Run("monitoring subtasks", func(t *testing.T) {
		s := State{
			CurrentPhase:       PhaseCheckingSubTaskExecutions,
			PhaseVersion:       8,
			OriginalArraySize:  10,
			ExecutionArraySize: 5,
		}
		phaseInfo := MapArrayStateToPluginPhase(ctx, s)
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
		phaseInfo := MapArrayStateToPluginPhase(ctx, s)
		assert.Equal(t, core.PhaseRunning, phaseInfo.Phase())
		assert.Equal(t, uint32(278), phaseInfo.Version())
	})

	t.Run("success", func(t *testing.T) {
		s := State{
			CurrentPhase: PhaseSuccess,
			PhaseVersion: 0,
		}
		phaseInfo := MapArrayStateToPluginPhase(ctx, s)
		assert.Equal(t, core.PhaseSuccess, phaseInfo.Phase())
	})

	t.Run("retryable failure", func(t *testing.T) {
		s := State{
			CurrentPhase: PhaseRetryableFailure,
			PhaseVersion: 0,
		}
		phaseInfo := MapArrayStateToPluginPhase(ctx, s)
		assert.Equal(t, core.PhaseRetryableFailure, phaseInfo.Phase())
	})

	t.Run("permanent failure", func(t *testing.T) {
		s := State{
			CurrentPhase: PhasePermanentFailure,
			PhaseVersion: 0,
		}
		phaseInfo := MapArrayStateToPluginPhase(ctx, s)
		assert.Equal(t, core.PhasePermanentFailure, phaseInfo.Phase())
	})
}
