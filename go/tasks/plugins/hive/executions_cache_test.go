package hive

import (
	"context"
	"github.com/lyft/flyteplugins/go/tasks/plugins/hive/client"
	quboleMocks "github.com/lyft/flyteplugins/go/tasks/plugins/hive/client/mocks"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/utils"
	stdlibMocks "github.com/lyft/flytestdlib/utils/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

func TestQuboleHiveExecutionsCache_SyncQuboleQuery(t *testing.T) {
	ctx := context.Background()

	t.Run("terminal state return unchanged", func(t *testing.T) {
		mockCache := &stdlibMocks.AutoRefreshCache{}
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
		cacheItem := ExecutionStateCacheItem{
			ExecutionState: state,
			Id: "some-id",
		}
		newCacheItem, action, err := q.SyncQuboleQuery(ctx, cacheItem)
		assert.NoError(t, err)
		assert.Equal(t, utils.Unchanged, action)
		assert.Equal(t, cacheItem, newCacheItem)
	})

	t.Run("move to success", func(t *testing.T) {
		mockCache := &stdlibMocks.AutoRefreshCache{}
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
			Phase:     PhaseSubmitted,
		}
		cacheItem := ExecutionStateCacheItem{
			ExecutionState: state,
			Id: "some-id",
		}
		mockQubole.On("GetCommandStatus", mock.Anything, mock.MatchedBy(func(commandId string) bool {
			return commandId == state.CommandId
		}), mock.Anything).Return(client.QuboleStatusDone, nil)

		newCacheItem, action, err := q.SyncQuboleQuery(ctx, cacheItem)
		newExecutionState := newCacheItem.(ExecutionStateCacheItem)
		assert.NoError(t, err)
		assert.Equal(t, utils.Update, action)
		assert.Equal(t, PhaseQuerySucceeded, newExecutionState.Phase)
	})
}
