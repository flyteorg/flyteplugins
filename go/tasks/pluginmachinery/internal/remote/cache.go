package remote

import (
	"context"
	"time"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/remote"

	"k8s.io/client-go/util/workqueue"

	"github.com/lyft/flytestdlib/cache"

	"github.com/lyft/flyteplugins/go/tasks/errors"
	stdErrors "github.com/lyft/flytestdlib/errors"

	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/promutils"
)

const ResyncDuration = 30 * time.Second

const (
	BadQuboleReturnCodeError stdErrors.ErrorCode = "QUBOLE_RETURNED_UNKNOWN"
)

type ResourceCache struct {
	cache.AutoRefresh
	client remote.Plugin
	scope  promutils.Scope
	cfg    remote.CachingProperties
}

func NewResourceCache(ctx context.Context, name string, client remote.Plugin, cfg remote.CachingProperties,
	scope promutils.Scope) (ResourceCache, error) {

	q := ResourceCache{
		client: client,
		scope:  scope,
		cfg:    cfg,
	}

	autoRefreshCache, err := cache.NewAutoRefreshCache(name, q.SyncResource,
		workqueue.DefaultControllerRateLimiter(), cfg.ResyncInterval, cfg.Workers, cfg.Size, scope.NewSubScope("cache"))
	if err != nil {
		logger.Errorf(ctx, "Could not create AutoRefreshCache in QuboleHiveExecutor. [%s]", err)
		return q, errors.Wrapf(errors.CacheFailed, err, "Error creating AutoRefreshCache")
	}

	q.AutoRefresh = autoRefreshCache
	return q, nil
}

type CacheItem struct {
	State

	// This ID is the cache key and so will need to be unique across all objects in the cache (it will probably be
	// unique across all of Flyte) and needs to be deterministic.
	// This will also be used as the allocation token for now.
	Identifier string `json:"id"`
}

func (e CacheItem) ID() string {
	return e.Identifier
}

// This basically grab an updated status from the Qubole API and store it in the cache
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

		if len(cacheItem.ResourceKey.Name) == 0 {
			logger.Warnf(ctx, "Sync loop - ResourceKey is blank for [%s] skipping", cacheItem.Identifier)
			resp = append(resp, cache.ItemSyncResponse{
				ID:     resource.GetID(),
				Item:   resource.GetItem(),
				Action: cache.Unchanged,
			})

			continue
		}

		logger.Debugf(ctx, "Sync loop - processing resource [%s] - cache key [%s]",
			cacheItem.ResourceKey.Name, cacheItem.Identifier)

		if cacheItem.State.Phase.IsTerminal() {
			logger.Debugf(ctx, "Sync loop - resource key [%s] in terminal state [%s]",
				cacheItem.ResourceKey.Name, cacheItem.Identifier)

			resp = append(resp, cache.ItemSyncResponse{
				ID:     resource.GetID(),
				Item:   resource.GetItem(),
				Action: cache.Unchanged,
			})

			continue
		}

		// Get an updated status
		logger.Debugf(ctx, "Querying Qubole for %s - %s", cacheItem.ResourceKey.Name,
			cacheItem.Identifier)
		resource, err := q.client.Get(ctx, cacheItem.ResourceKey)
		if err != nil {
			logger.Errorf(ctx, "Error retrieving resource [%s]. Error: %v", cacheItem.ResourceKey.Name, err)
			cacheItem.SyncFailureCount++
			// Make sure we don't return nil for the first argument, because that deletes it from the cache.
			resp = append(resp, cache.ItemSyncResponse{
				ID:     cacheItem.ResourceKey.Name,
				Item:   cacheItem,
				Action: cache.Update,
			})

			continue
		}

		newPhase, err := q.client.Status(ctx, resource)
		if err != nil {
			return nil, err
		}

		if (cacheItem.LatestPhaseInfo == core.PhaseInfo{}) ||
			newPhase.Phase() != cacheItem.LatestPhaseInfo.Phase() {

			newPluginPhase, err := ToPluginPhase(newPhase.Phase())
			if err != nil {
				return nil, err
			}

			logger.Infof(ctx, "Moving Phase for %s %s from %s to %s", cacheItem.ResourceKey.Name,
				cacheItem.Identifier, cacheItem.Phase, newPluginPhase)

			cacheItem.LatestPhaseInfo = newPhase
			cacheItem.Phase = newPluginPhase

			resp = append(resp, cache.ItemSyncResponse{
				ID:     cacheItem.ResourceKey.Name,
				Item:   cacheItem,
				Action: cache.Update,
			})
		}
	}

	return resp, nil
}

// We need some way to translate results we get from Qubole, into a plugin phase
// NB: This function should only return plugin phases that are greater than (">") phases that represent states before
//     the query was kicked off. That is, it will never make sense to go back to PhaseNotStarted, after we've
//     submitted the query to Qubole.
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
		return PhaseFailed, errors.Errorf(BadQuboleReturnCodeError, "default fallthrough case")
	}
}
