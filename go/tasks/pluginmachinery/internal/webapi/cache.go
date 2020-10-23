package webapi

import (
	"context"

	"github.com/lyft/flytestdlib/promutils"
	"k8s.io/client-go/util/workqueue"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/webapi"

	"github.com/lyft/flytestdlib/cache"

	"github.com/lyft/flyteplugins/go/tasks/errors"
	stdErrors "github.com/lyft/flytestdlib/errors"

	"github.com/lyft/flytestdlib/logger"
)

const (
	BadReturnCodeError stdErrors.ErrorCode = "RETURNED_UNKNOWN"
)

// Client interface needed for resource cache to fetch latest updates for resources.
type Client interface {
	// Get multiple resources that match all the keys. If the plugin hits any failure, it should stop and return
	// the failure. This batch will not be processed further.
	Get(ctx context.Context, cached webapi.ResourceMeta) (latest webapi.ResourceMeta, err error)

	// Status checks the status of a given resource and translates it to a Flyte-understandable PhaseInfo.
	Status(ctx context.Context, resource webapi.ResourceMeta) (phase core.PhaseInfo, err error)
}

// A generic AutoRefresh cache that uses webapi.Plugin as a client to fetch items' status.
type ResourceCache struct {
	// AutoRefresh
	cache.AutoRefresh
	client Client
}

// A wrapper for each item in the cache.
type CacheItem struct {
	State
}

// This basically grab an updated status from Client and store it in the cache
// All other handling should be in the synchronous loop.
func (q *ResourceCache) SyncResource(ctx context.Context, batch cache.Batch) (
	updatedBatch []cache.ItemSyncResponse, err error) {

	resp := make([]cache.ItemSyncResponse, 0, len(batch))
	for _, resource := range batch {
		// Cast the item back to the thing we want to work with.
		cacheItem, ok := resource.GetItem().(CacheItem)
		if !ok {
			logger.Errorf(ctx, "Sync loop - Error casting cache object into CacheItem")
			return nil, errors.Errorf(errors.CacheFailed, "Failed to cast [%v]", batch[0].GetID())
		}

		if len(resource.GetID()) == 0 {
			logger.Warnf(ctx, "Sync loop - ResourceKey is blank for [%s] skipping", resource.GetID())
			resp = append(resp, cache.ItemSyncResponse{
				ID:     resource.GetID(),
				Item:   resource.GetItem(),
				Action: cache.Unchanged,
			})

			continue
		}

		logger.Debugf(ctx, "Sync loop - processing resource with cache key [%s]",
			resource.GetID())

		if cacheItem.State.Phase.IsTerminal() {
			logger.Debugf(ctx, "Sync loop - resource cache key [%v] in terminal state [%s]",
				resource.GetID())

			resp = append(resp, cache.ItemSyncResponse{
				ID:     resource.GetID(),
				Item:   resource.GetItem(),
				Action: cache.Unchanged,
			})

			continue
		}

		// Get an updated status
		logger.Debugf(ctx, "Querying Plugin for %s - %s", resource.GetID(),
			resource.GetID())
		newResource, err := q.client.Get(ctx, cacheItem.ResourceMeta)
		if err != nil {
			logger.Errorf(ctx, "Error retrieving resource [%s]. Error: %v", resource.GetID(), err)
			cacheItem.SyncFailureCount++
			// Make sure we don't return nil for the first argument, because that deletes it from the cache.
			resp = append(resp, cache.ItemSyncResponse{
				ID:     resource.GetID(),
				Item:   cacheItem,
				Action: cache.Update,
			})

			continue
		}

		newPhase, err := q.client.Status(ctx, newResource)
		if err != nil {
			return nil, err
		}

		if (cacheItem.LatestPhaseInfo == core.PhaseInfo{}) ||
			newPhase.Phase() != cacheItem.LatestPhaseInfo.Phase() {

			newPluginPhase, err := ToPluginPhase(newPhase.Phase())
			if err != nil {
				return nil, err
			}

			logger.Infof(ctx, "Moving Phase for %s %s from %s to %s", resource.GetID(),
				resource.GetID(), cacheItem.Phase, newPluginPhase)

			cacheItem.LatestPhaseInfo = newPhase
			cacheItem.Phase = newPluginPhase
			cacheItem.ResourceMeta = newResource

			resp = append(resp, cache.ItemSyncResponse{
				ID:     resource.GetID(),
				Item:   cacheItem,
				Action: cache.Update,
			})
		}
	}

	return resp, nil
}

// We need some way to translate results we get from Client, into a plugin phase
// NB: This function should only return plugin phases that are greater than (">") phases that represent states before
//     the query was kicked off. That is, it will never make sense to go back to PhaseNotStarted, after we've
//     submitted the request through Client.
func ToPluginPhase(s core.Phase) (Phase, error) {
	switch s {

	case core.PhaseUndefined:
		fallthrough
	case core.PhaseNotReady:
		fallthrough
	case core.PhaseInitializing:
		fallthrough
	case core.PhaseWaitingForResources:
		fallthrough
	case core.PhaseQueued:
		fallthrough
	case core.PhaseRunning:
		return PhaseResourcesCreated, nil
	case core.PhaseSuccess:
		return PhaseSucceeded, nil
	case core.PhasePermanentFailure:
		fallthrough
	case core.PhaseRetryableFailure:
		return PhaseFailed, nil
	default:
		return PhaseFailed, errors.Errorf(BadReturnCodeError, "default fallthrough case")
	}
}

func NewResourceCache(ctx context.Context, name string, client Client, cfg webapi.CachingConfig,
	scope promutils.Scope) (ResourceCache, error) {

	q := ResourceCache{
		client: client,
	}

	autoRefreshCache, err := cache.NewAutoRefreshCache(name, q.SyncResource,
		workqueue.DefaultControllerRateLimiter(), cfg.ResyncInterval.Duration, cfg.Workers, cfg.Size,
		scope.NewSubScope("cache"))

	if err != nil {
		logger.Errorf(ctx, "Could not create AutoRefreshCache. Error: [%s]", err)
		return q, errors.Wrapf(errors.CacheFailed, err, "Error creating AutoRefreshCache")
	}

	q.AutoRefresh = autoRefreshCache
	return q, nil
}
