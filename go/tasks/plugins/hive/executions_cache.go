package hive

import (
	"context"
	"time"

	"github.com/lyft/flyteplugins/go/tasks/errors"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/plugins/hive/client"
	"github.com/lyft/flyteplugins/go/tasks/plugins/hive/config"

	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/utils"
)

const ResyncDuration = 30 * time.Second

const (
	BadQuboleReturnCodeError errors.ErrorCode = "QUBOLE_RETURNED_UNKNOWN"
)

type QuboleHiveExecutionsCache struct {
	utils.AutoRefreshCache
	quboleClient  client.QuboleClient
	secretManager core.SecretManager
	scope         promutils.Scope
	cfg           *config.Config
}

func NewQuboleHiveExecutionsCache(ctx context.Context, quboleClient client.QuboleClient,
	secretManager core.SecretManager, cfg *config.Config, scope promutils.Scope) (QuboleHiveExecutionsCache, error) {

	rateLimiter := utils.NewRateLimiter("qubole-api-updater", 5, 15)
	q := QuboleHiveExecutionsCache{
		quboleClient:  quboleClient,
		secretManager: secretManager,
		scope:         scope,
		cfg:           cfg,
	}
	autoRefreshCache, err := utils.NewAutoRefreshCache(q.SyncQuboleQuery, rateLimiter, ResyncDuration, cfg.LruCacheSize, scope)
	if err != nil {
		logger.Errorf(ctx, "Could not create AutoRefreshCache in QuboleHiveExecutor. [%s]", err)
		return q, errors.Wrapf(errors.CacheFailed, err, "Error creating AutoRefreshCache")
	}
	q.AutoRefreshCache = autoRefreshCache
	return q, nil
}

type ExecutionStateCacheItem struct {
	ExecutionState

	// This ID is the cache key and so will need to be unique across all objects in the cache (it will probably be
	// unique across all of Flyte) and needs to be deterministic.
	// This will also be used as the allocation token for now.
	Id string `json:"id"`
}

func (e ExecutionStateCacheItem) ID() string {
	return e.Id
}

// This basically grab an updated status from the Qubole API and store it in the cache
// All other handling should be in the synchronous loop.
func (q *QuboleHiveExecutionsCache) SyncQuboleQuery(ctx context.Context, obj utils.CacheItem) (
	utils.CacheItem, utils.CacheSyncAction, error) {

	// Cast the item back to the thing we want to work with.
	executionStateCacheItem, ok := obj.(ExecutionStateCacheItem)
	if !ok {
		logger.Errorf(ctx, "Sync loop - Error casting cache object into ExecutionState")
		return obj, utils.Unchanged, errors.Errorf(errors.CacheFailed, "Failed to cast [%v]", obj)
	}

	if executionStateCacheItem.CommandId == "" {
		logger.Warnf(ctx, "Sync loop - CommandID is blank for [%s] skipping", executionStateCacheItem.Id)
		return executionStateCacheItem, utils.Unchanged, nil
	}

	logger.Debugf(ctx, "Sync loop - processing Hive job [%s] - cache key [%s]",
		executionStateCacheItem.CommandId, executionStateCacheItem.Id)

	quboleApiKey, err := q.secretManager.Get(ctx, q.cfg.QuboleTokenKey)
	if err != nil {
		return executionStateCacheItem, utils.Unchanged, err
	}

	if InTerminalState(executionStateCacheItem.ExecutionState) {
		logger.Debugf(ctx, "Sync loop - Qubole id [%s] in terminal state [%s]",
			executionStateCacheItem.CommandId, executionStateCacheItem.Id)

		return executionStateCacheItem, utils.Unchanged, nil
	}

	// Get an updated status from Qubole
	logger.Debugf(ctx, "Querying Qubole for %s - %s", executionStateCacheItem.CommandId, executionStateCacheItem.Id)
	commandStatus, err := q.quboleClient.GetCommandStatus(ctx, executionStateCacheItem.CommandId, quboleApiKey)
	if err != nil {
		logger.Errorf(ctx, "Error from Qubole command %s", executionStateCacheItem.CommandId)
		executionStateCacheItem.SyncFailureCount++
		// Make sure we don't return nil for the first argument, because that deletes it from the cache.
		return executionStateCacheItem, utils.Update, err
	}
	newExecutionPhase, err := QuboleStatusToExecutionPhase(commandStatus)
	if err != nil {
		return executionStateCacheItem, utils.Unchanged, err
	}

	if newExecutionPhase > executionStateCacheItem.Phase {
		logger.Infof(ctx, "Moving ExecutionPhase for %s %s from %s to %s", executionStateCacheItem.CommandId,
			executionStateCacheItem.Id, executionStateCacheItem.Phase, newExecutionPhase)

		executionStateCacheItem.Phase = newExecutionPhase

		return executionStateCacheItem, utils.Update, nil
	}

	return executionStateCacheItem, utils.Unchanged, nil
}

// We need some way to translate results we get from Qubole, into a plugin phase
// NB: This function should only return plugin phases that are greater than (">") phases that represent states before
//     the query was kicked off. That is, it will never make sense to go back to PhaseNotStarted, after we've
//     submitted the query to Qubole.
func QuboleStatusToExecutionPhase(s client.QuboleStatus) (ExecutionPhase, error) {
	switch s {
	case client.QuboleStatusDone:
		return PhaseQuerySucceeded, nil
	case client.QuboleStatusCancelled:
		return PhaseQueryFailed, nil
	case client.QuboleStatusError:
		return PhaseQueryFailed, nil
	case client.QuboleStatusWaiting:
		return PhaseSubmitted, nil
	case client.QuboleStatusRunning:
		return PhaseSubmitted, nil
	case client.QuboleStatusUnknown:
		return PhaseQueryFailed, errors.Errorf(BadQuboleReturnCodeError, "Qubole returned status Unknown")
	default:
		return PhaseQueryFailed, errors.Errorf(BadQuboleReturnCodeError, "default fallthrough case")
	}
}
