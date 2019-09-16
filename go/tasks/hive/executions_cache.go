package hive

import (
	"context"
	"github.com/lyft/flyteplugins/go/tasks/errors"
	"github.com/lyft/flyteplugins/go/tasks/hive/client"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/utils"
	"time"
)

const ResyncDuration = 30 * time.Second

const (
	BadQuboleReturnCodeError errors.ErrorCode = "QUBOLE_RETURNED_UNKNOWN"
)

type QuboleHiveExecutionsCache struct {
	utils.AutoRefreshCache
	quboleClient   client.QuboleClient
	secretsManager SecretsManager
	scope          promutils.Scope
}

func NewQuboleHiveExecutionsCache(ctx context.Context, quboleClient client.QuboleClient,
	secretsManager SecretsManager, size int, scope promutils.Scope) (QuboleHiveExecutionsCache, error) {

	rateLimiter := utils.NewRateLimiter("qubole-api-updater", 5, 15)
	q := QuboleHiveExecutionsCache{
		quboleClient:     quboleClient,
		secretsManager:   secretsManager,
		scope:            scope,
	}
	autoRefreshCache, err := utils.NewAutoRefreshCache(q.SyncQuboleQuery, rateLimiter, ResyncDuration, size, scope)
	if err != nil {
		logger.Errorf(ctx, "Could not create AutoRefreshCache in QuboleHiveExecutor. [%s]", err)
		return q, errors.Wrapf( errors.CacheFailed, err, "Error creating AutoRefreshCache")
	}
	q.AutoRefreshCache = autoRefreshCache
	return q, nil
}

// This basically grab an updated status from the Qubole API and store it in the cache
// All other handling should be in the synchronous loop.
func (q *QuboleHiveExecutionsCache) SyncQuboleQuery(ctx context.Context, obj utils.CacheItem) (
	utils.CacheItem, utils.CacheSyncAction, error) {

	// Cast the item back to the thing we want to work with.
	executionState, ok := obj.(ExecutionState)
	if !ok {
		logger.Errorf(ctx, "Sync loop - Error casting cache object into ExecutionState")
		return obj, utils.Unchanged, errors.Errorf(errors.CacheFailed, "Failed to cast [%v]", obj)
	}

	if executionState.CommandId == "" {
		logger.Warnf(ctx, "Sync loop - CommandID is blank for [%s] skipping", executionState.Id)
		return executionState, utils.Unchanged, nil
	}

	logger.Debugf(ctx, "Sync loop - processing Hive job [%s] - cache key [%s]",
		executionState.CommandId, executionState.Id)

	quboleApiKey, err := q.secretsManager.GetToken()
	if err != nil {
		return executionState, utils.Unchanged, err
	}

	if InTerminalState(executionState) {
		logger.Debugf(ctx, "Sync loop - Qubole id [%s] in terminal state [%s]",
			executionState.CommandId, executionState.Id)

		return executionState, utils.Unchanged, nil
	}

	// Get an updated status from Qubole
	logger.Debugf(ctx, "Querying Qubole for %s - %s", executionState.CommandId, executionState.Id)
	commandStatus, err := q.quboleClient.GetCommandStatus(ctx, executionState.CommandId, quboleApiKey)
	if err != nil {
		logger.Errorf(ctx, "Error from Qubole command %s", executionState.CommandId)
		executionState.SyncQuboleApiFailures++
		// Make sure we don't return nil for the first argument, because that deletes it from the cache.
		return executionState, utils.Update, err
	}
	newExecutionPhase, err := QuboleStatusToExecutionPhase(commandStatus)
	if err != nil {
		return executionState, utils.Unchanged, err
	}

	if newExecutionPhase > executionState.Phase {
		logger.Infof(ctx, "Moving ExecutionPhase for %s %s from %s to %s", executionState.CommandId,
			executionState.Id, executionState.Phase, newExecutionPhase)

		executionState.Phase = newExecutionPhase

		return executionState, utils.Update, nil
	}

	return executionState, utils.Unchanged, nil
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
