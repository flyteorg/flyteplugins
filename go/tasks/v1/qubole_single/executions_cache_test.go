package qubole_single

import (
	"context"
	"github.com/lyft/flyteplugins/go/tasks/v1/k8splugins/mocks"
	"github.com/lyft/flyteplugins/go/tasks/v1/qubole_single/client"
	quboleMocks "github.com/lyft/flyteplugins/go/tasks/v1/qubole_single/client/mocks"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

func TestQuboleHiveExecutionsCache_SyncQuboleQuery(t *testing.T) {
	ctx := context.Background()

	t.Run("terminal state return unchanged", func(t *testing.T) {
		mockCache := &mocks.AutoRefreshCache{}
		mockQubole := &quboleMocks.QuboleClient{}
		testScope := promutils.NewTestScope()
		secrets := &secretsManager{quboleKey: "test key"}

		q := QuboleHiveExecutionsCache{
			AutoRefreshCache: mockCache,
			quboleClient:     mockQubole,
			secretsManager:   secrets,
			scope:            testScope,
		}

		state := ExecutionState{
			Phase: PhaseQuerySucceeded,
		}
		newState, action, err := q.SyncQuboleQuery(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, utils.Unchanged, action)
		assert.Equal(t, state, newState)
	})

	t.Run("move to success", func(t *testing.T) {
		mockCache := &mocks.AutoRefreshCache{}
		mockQubole := &quboleMocks.QuboleClient{}
		testScope := promutils.NewTestScope()
		secrets := &secretsManager{quboleKey: "test key"}

		q := QuboleHiveExecutionsCache{
			AutoRefreshCache: mockCache,
			quboleClient:     mockQubole,
			secretsManager:   secrets,
			scope:            testScope,
		}

		state := ExecutionState{
			CommandId: "123456",
			Phase: PhaseSubmitted,
		}
		mockQubole.On("GetCommandStatus", mock.Anything, mock.MatchedBy(func(commandId string) bool {
			return commandId == state.CommandId
		}), mock.Anything).Return(client.QuboleStatusDone, nil)

		newCacheItem, action, err := q.SyncQuboleQuery(ctx, state)
		newExecutionState := newCacheItem.(ExecutionState)
		assert.NoError(t, err)
		assert.Equal(t, utils.Update, action)
		assert.Equal(t, PhaseQuerySucceeded, newExecutionState.Phase)
	})
}
