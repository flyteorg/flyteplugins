package array

import (
	"context"
	"testing"

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
		s := &StateImpl{
			CurrentPhase: PhaseStart,
		}
		phaseInfo := MapArrayStateToPluginPhase(ctx, s)
		assert.Equal(t, core.PhaseInitializing, phaseInfo.Phase())
	})
}

func TestNewLiteralScalarOfInteger(t *testing.T) {

}

func TestCatalogBitsetToLiteralCollection(t *testing.T) {

}
