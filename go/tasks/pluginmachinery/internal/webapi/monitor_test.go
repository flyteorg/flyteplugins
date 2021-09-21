package webapi

import (
	"context"
	"testing"
	"time"

	"github.com/flyteorg/flytestdlib/cache"
	"github.com/flyteorg/flytestdlib/promutils"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/util/workqueue"

	core2 "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"

	"github.com/stretchr/testify/mock"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/stretchr/testify/assert"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	internalMocks "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/internal/webapi/mocks"
)

func Test_monitor(t *testing.T) {
	ctx := context.Background()
	tCtx := &mocks.TaskExecutionContext{}
	ctxMeta := &mocks.TaskExecutionMetadata{}
	execID := &mocks.TaskExecutionID{}
	execID.OnGetGeneratedName().Return("generated_name")
	execID.OnGetID().Return(core.TaskExecutionIdentifier{})
	ctxMeta.OnGetTaskExecutionID().Return(execID)
	tCtx.OnTaskExecutionMetadata().Return(ctxMeta)

	client := &internalMocks.Client{}
	client.OnStatusMatch(ctx, mock.Anything).Return(core2.PhaseInfoSuccess(nil), nil)

	cacheObj, err := cache.NewAutoRefreshCache(rand.String(5), func(ctx context.Context, batch cache.Batch) (updatedBatch []cache.ItemSyncResponse, err error) {
		return []cache.ItemSyncResponse{
			{
				ID:     batch[0].GetID(),
				Item:   batch[0].GetItem(),
				Action: cache.Update,
			},
		}, nil
	}, workqueue.DefaultControllerRateLimiter(), time.Second, 10, 10, promutils.NewTestScope())
	assert.NoError(t, err)
	cacheObj.GetOrCreate("generated_name", CacheItem{Resource: "fake_resource"})

	s := &State{}
	newState, phaseInfo, err := monitor(ctx, tCtx, client, cacheObj, s)
	assert.NoError(t, err)
	assert.NotNil(t, newState)
	assert.NotNil(t, phaseInfo)
	assert.Equal(t, core2.PhaseSuccess.String(), phaseInfo.Phase().String())

	cachedItem, err := cacheObj.GetOrCreate("generated_name", CacheItem{Resource: "fake_resource"})
	assert.NoError(t, err)
	assert.Equal(t, core2.PhaseSuccess.String(), cachedItem.(CacheItem).Phase.String())
}
