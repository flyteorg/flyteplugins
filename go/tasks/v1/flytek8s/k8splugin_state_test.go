package flytek8s

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/lyft/flyteplugins/go/tasks/v1/types"
)

func TestRetrieveK8sObjectStatus(t *testing.T) {
	status := k8sObjectExists
	phase := types.TaskPhaseRunning
	customState := storeK8sObjectStatus(status, phase)

	retrievedStatus, retrievedPhase, err := retrieveK8sObjectStatus(customState)
	assert.NoError(t, err)
	assert.Equal(t, status, retrievedStatus)
	assert.Equal(t, phase, retrievedPhase)
}

func TestRetrieveK8sObjectStatus2(t *testing.T) {
	status := 2
	phase := 4
	customState := make(map[string]interface{})
	customState[statusKey] = status
	customState[terminalTaskPhaseKey] = phase

	retrievedStatus, retrievedPhase, err := retrieveK8sObjectStatus(customState)
	assert.NoError(t, err)
	assert.Equal(t, status, int(retrievedStatus))
	assert.Equal(t, phase, int(retrievedPhase))
}