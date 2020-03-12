package presto

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"

	"github.com/lyft/flyteplugins/go/tasks/plugins/presto/client"

	"time"

	"github.com/lyft/flytestdlib/cache"

	idlCore "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/utils"
	"github.com/lyft/flyteplugins/go/tasks/plugins/presto/config"

	"github.com/lyft/flyteplugins/go/tasks/errors"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flytestdlib/logger"

	"github.com/lyft/flyteplugins/go/tasks/plugins/svc"
)

type ExecutionPhase int

const (
	PhaseNotStarted ExecutionPhase = iota
	PhaseQueued                    // resource manager token gotten
	PhaseSubmitted                 // Sent off to Presto
	PhaseQuerySucceeded
	PhaseQueryFailed
)

func (p ExecutionPhase) String() string {
	switch p {
	case PhaseNotStarted:
		return "PhaseNotStarted"
	case PhaseQueued:
		return "PhaseQueued"
	case PhaseSubmitted:
		return "PhaseSubmitted"
	case PhaseQuerySucceeded:
		return "PhaseQuerySucceeded"
	case PhaseQueryFailed:
		return "PhaseQueryFailed"
	}
	return "Bad Presto execution phase"
}

type ExecutionState struct {
	Phase ExecutionPhase

	// This will store the command ID from Presto
	CommandID string `json:"command_id,omitempty"`
	URI       string `json:"uri,omitempty"`

	CurrentPrestoQuery Query `json:"current_presto_query,omitempty"`
	QueryCount         int   `json:"query_count,omitempty"`

	// This number keeps track of the number of failures within the sync function. Without this, what happens in
	// the sync function is entirely opaque. Note that this field is completely orthogonal to Flyte system/node/task
	// level retries, just errors from hitting the Presto API, inside the sync loop
	SyncFailureCount int `json:"sync_failure_count,omitempty"`

	// In kicking off the Presto command, this is the number of failures
	CreationFailureCount int `json:"creation_failure_count,omitempty"`

	// The time the execution first requests for an allocation token
	AllocationTokenRequestStartTime time.Time `json:"allocation_token_request_start_time,omitempty"`
}

type Query struct {
	Statement         string                   `json:"statement,omitempty"`
	ExtraArgs         client.PrestoExecuteArgs `json:"extra_args,omitempty"`
	TempTableName     string                   `json:"temp_table_name,omitempty"`
	ExternalTableName string                   `json:"external_table_name,omitempty"`
	ExternalLocation  string                   `json:"external_location"`
}

// This is the main state iteration
func HandleExecutionState(
	ctx context.Context,
	tCtx core.TaskExecutionContext,
	currentState ExecutionState,
	prestoClient svc.ServiceClient,
	executionsCache cache.AutoRefresh,
	metrics ExecutorMetrics) (ExecutionState, error) {

	var transformError error
	var newState ExecutionState

	switch currentState.Phase {
	case PhaseNotStarted:
		newState, transformError = GetAllocationToken(ctx, tCtx, currentState, metrics)

	case PhaseQueued:
		prestoQuery, err := GetNextQuery(ctx, tCtx, currentState)
		if err != nil {
			return ExecutionState{}, err
		}
		currentState.CurrentPrestoQuery = prestoQuery
		newState, transformError = KickOffQuery(ctx, tCtx, currentState, prestoClient, executionsCache)

	case PhaseSubmitted:
		newState, transformError = MonitorQuery(ctx, tCtx, currentState, executionsCache)

	case PhaseQuerySucceeded:
		if currentState.QueryCount < 4 {
			// If there are still Presto statements to execute, increment the query count, reset the phase to get a new
			// allocation token, and continue executing the remaining statements
			currentState.QueryCount++
			currentState.Phase = PhaseQueued
		}
		newState = currentState
		transformError = nil

	case PhaseQueryFailed:
		newState = currentState
		transformError = nil
	}

	return newState, transformError
}

func GetAllocationToken(
	ctx context.Context,
	tCtx core.TaskExecutionContext,
	currentState ExecutionState,
	metric ExecutorMetrics) (ExecutionState, error) {

	newState := ExecutionState{}
	uniqueID := tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()

	routingGroup, err := composeResourceNamespaceWithRoutingGroup(ctx, tCtx)
	if err != nil {
		return newState, errors.Wrapf(errors.ResourceManagerFailure, err, "Error getting query info when requesting allocation token %s", uniqueID)
	}

	resourceConstraintsSpec := createResourceConstraintsSpec(ctx, tCtx, routingGroup)

	allocationStatus, err := tCtx.ResourceManager().AllocateResource(ctx, routingGroup, uniqueID, resourceConstraintsSpec)
	if err != nil {
		logger.Errorf(ctx, "Resource manager failed for TaskExecId [%s] token [%s]. error %s",
			tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetID(), uniqueID, err)
		return newState, errors.Wrapf(errors.ResourceManagerFailure, err, "Error requesting allocation token %s", uniqueID)
	}
	logger.Infof(ctx, "Allocation result for [%s] is [%s]", uniqueID, allocationStatus)

	// Emitting the duration this execution has been waiting for a token allocation
	if currentState.AllocationTokenRequestStartTime.IsZero() {
		newState.AllocationTokenRequestStartTime = time.Now()
	} else {
		newState.AllocationTokenRequestStartTime = currentState.AllocationTokenRequestStartTime
	}
	waitTime := time.Since(newState.AllocationTokenRequestStartTime)
	metric.ResourceWaitTime.Observe(waitTime.Seconds())

	if allocationStatus == core.AllocationStatusGranted {
		newState.Phase = PhaseQueued
	} else if allocationStatus == core.AllocationStatusExhausted {
		newState.Phase = PhaseNotStarted
	} else if allocationStatus == core.AllocationStatusNamespaceQuotaExceeded {
		newState.Phase = PhaseNotStarted
	} else {
		return newState, errors.Errorf(errors.ResourceManagerFailure, "Got bad allocation result [%s] for token [%s]",
			allocationStatus, uniqueID)
	}

	return newState, nil
}

func composeResourceNamespaceWithRoutingGroup(ctx context.Context, tCtx core.TaskExecutionContext) (core.ResourceNamespace, error) {
	routingGroup, _, _, _, err := GetQueryInfo(ctx, tCtx)
	if err != nil {
		return "", err
	}
	clusterPrimaryLabel := resolveRoutingGroup(ctx, routingGroup, config.GetPrestoConfig())
	return core.ResourceNamespace(clusterPrimaryLabel), nil
}

// This function is the link between the output written by the SDK, and the execution side. It extracts the query
// out of the task template.
func GetQueryInfo(ctx context.Context, tCtx core.TaskExecutionContext) (
	routingGroup string,
	catalog string,
	schema string,
	statement string,
	err error) {

	taskTemplate, err := tCtx.TaskReader().Read(ctx)
	if err != nil {
		return "", "", "", "", err
	}

	prestoQuery := plugins.PrestoQuery{}
	if err := utils.UnmarshalStruct(taskTemplate.GetCustom(), &prestoQuery); err != nil {
		return "", "", "", "", err
	}

	if err := validatePrestoStatement(prestoQuery); err != nil {
		return "", "", "", "", err
	}

	routingGroup = prestoQuery.RoutingGroup
	catalog = prestoQuery.Catalog
	schema = prestoQuery.Schema
	statement = prestoQuery.Statement

	logger.Debugf(ctx, "QueryInfo: query: [%v], routingGroup: [%v], catalog: [%v], schema: [%v]", statement, routingGroup, catalog, schema)
	return
}

func validatePrestoStatement(prestoJob plugins.PrestoQuery) error {
	if prestoJob.Statement == "" {
		return errors.Errorf(errors.BadTaskSpecification,
			"Query could not be found. Please ensure that you are at least on Flytekit version 0.3.0 or later.")
	}
	return nil
}

func resolveRoutingGroup(ctx context.Context, routingGroup string, prestoCfg *config.Config) string {
	if routingGroup == "" {
		logger.Debugf(ctx, "Input routing group is an empty string; falling back to using the default routing group [%v]", prestoCfg.DefaultRoutingGroup)
		return prestoCfg.DefaultRoutingGroup
	}

	for _, routingGroupCfg := range prestoCfg.RoutingGroupConfigs {
		if routingGroup == routingGroupCfg.Name {
			logger.Debugf(ctx, "Found the Presto routing group: [%v]", routingGroupCfg.Name)
			return routingGroup
		}
	}

	logger.Debugf(ctx, "Cannot find the routing group [%v] in configmap; "+
		"falling back to using the default routing group [%v]", routingGroup, prestoCfg.DefaultRoutingGroup)
	return prestoCfg.DefaultRoutingGroup
}

func createResourceConstraintsSpec(ctx context.Context, _ core.TaskExecutionContext, routingGroup core.ResourceNamespace) core.ResourceConstraintsSpec {
	cfg := config.GetPrestoConfig()
	constraintsSpec := core.ResourceConstraintsSpec{
		ProjectScopeResourceConstraint:   nil,
		NamespaceScopeResourceConstraint: nil,
	}
	if cfg.RoutingGroupConfigs == nil {
		logger.Infof(ctx, "No routing group config is found. Returning an empty resource constraints spec")
		return constraintsSpec
	}
	for _, routingGroupCfg := range cfg.RoutingGroupConfigs {
		if routingGroupCfg.Name == string(routingGroup) {
			constraintsSpec.ProjectScopeResourceConstraint = &core.ResourceConstraint{Value: int64(float64(routingGroupCfg.Limit) * routingGroupCfg.ProjectScopeQuotaProportionCap)}
			constraintsSpec.NamespaceScopeResourceConstraint = &core.ResourceConstraint{Value: int64(float64(routingGroupCfg.Limit) * routingGroupCfg.NamespaceScopeQuotaProportionCap)}
			break
		}
	}
	logger.Infof(ctx, "Created a resource constraints spec: [%v]", constraintsSpec)
	return constraintsSpec
}

func GetNextQuery(
	ctx context.Context,
	tCtx core.TaskExecutionContext,
	currentState ExecutionState) (Query, error) {

	switch currentState.QueryCount {
	case 0:
		prestoCfg := config.GetPrestoConfig()
		tempTableName := generateRandomString(32)
		routingGroup, catalog, schema, statement, err := GetQueryInfo(ctx, tCtx)
		if err != nil {
			return Query{}, err
		}

		statement = fmt.Sprintf(`CREATE TABLE hive.flyte_temporary_tables.%s_temp AS %s`, tempTableName, statement)

		prestoQuery := Query{
			Statement: statement,
			ExtraArgs: client.PrestoExecuteArgs{
				RoutingGroup: resolveRoutingGroup(ctx, routingGroup, prestoCfg),
				Catalog:      catalog,
				Schema:       schema,
			},
			TempTableName:     tempTableName + "_temp",
			ExternalTableName: tempTableName + "_external",
		}

		return prestoQuery, nil

	case 1:
		cfg := config.GetPrestoConfig()
		externalLocation := getExternalLocation(cfg.AwsS3ShardFormatter, cfg.AwsS3ShardCount)

		statement := fmt.Sprintf(`
CREATE TABLE hive.flyte_temporary_tables.%s (LIKE hive.flyte_temporary_tables.%s)"
WITH (format = 'PARQUET', external_location = '%s')`,
			currentState.CurrentPrestoQuery.ExternalTableName,
			currentState.CurrentPrestoQuery.TempTableName,
			externalLocation,
		)
		currentState.CurrentPrestoQuery.Statement = statement
		return currentState.CurrentPrestoQuery, nil

	case 2:
		statement := fmt.Sprintf(`
INSERT INTO hive.flyte_temporary_tables.%s
SELECT *
FROM hive.flyte_temporary_tables.%s`,
			currentState.CurrentPrestoQuery.ExternalTableName,
			currentState.CurrentPrestoQuery.TempTableName,
		)
		currentState.CurrentPrestoQuery.Statement = statement
		return currentState.CurrentPrestoQuery, nil

	case 3:
		statement := fmt.Sprintf(`DROP TABLE %s`, currentState.CurrentPrestoQuery.TempTableName)
		currentState.CurrentPrestoQuery.Statement = statement
		return currentState.CurrentPrestoQuery, nil

	case 4:
		statement := fmt.Sprintf(`DROP TABLE %s`, currentState.CurrentPrestoQuery.ExternalTableName)
		currentState.CurrentPrestoQuery.Statement = statement
		return currentState.CurrentPrestoQuery, nil

	default:
		return currentState.CurrentPrestoQuery, nil
	}
}

func generateRandomString(length int) string {
	const letters = "0123456789abcdefghijklmnopqrstuvwxyz"
	bytes, err := generateRandomBytes(length)
	if err != nil {
		return ""
	}
	for i, b := range bytes {
		bytes[i] = letters[b%byte(len(letters))]
	}
	return string(bytes)
}

func generateRandomBytes(length int) ([]byte, error) {
	b := make([]byte, length)
	_, err := rand.Read(b)
	// Note that err == nil only if we read len(b) bytes.
	if err != nil {
		return nil, err
	}

	return b, nil
}

func getExternalLocation(shardFormatter string, shardLength int) string {
	shardCount := strings.Count(shardFormatter, "{}")
	for i := 0; i < shardCount; i++ {
		shardFormatter = strings.Replace(shardFormatter, "{}", generateRandomString(shardLength), 1)
	}

	return shardFormatter + generateRandomString(32) + "/"
}

func KickOffQuery(
	ctx context.Context,
	tCtx core.TaskExecutionContext,
	currentState ExecutionState,
	prestoClient svc.ServiceClient,
	cache cache.AutoRefresh) (ExecutionState, error) {

	uniqueID := tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()

	statement := currentState.CurrentPrestoQuery.Statement
	extraArgs := currentState.CurrentPrestoQuery.ExtraArgs

	response, err := prestoClient.ExecuteCommand(ctx, statement, extraArgs)
	if err != nil {
		// If we failed, we'll keep the NotStarted state
		currentState.CreationFailureCount = currentState.CreationFailureCount + 1
		logger.Warnf(ctx, "Error creating Presto query for %s, failure counts %d. Error: %s", uniqueID, currentState.CreationFailureCount, err)
	} else {
		executeResponse := response.(client.PrestoExecuteResponse)

		// If we succeed, then store the command id returned from Presto, and update our state. Also, add to the
		// AutoRefreshCache so we start getting updates for its status.
		commandID := executeResponse.ID
		logger.Infof(ctx, "Created Presto ID [%s] for token %s", commandID, uniqueID)
		currentState.CommandID = commandID
		currentState.Phase = PhaseSubmitted
		currentState.URI = executeResponse.NextURI

		executionStateCacheItem := ExecutionStateCacheItem{
			ExecutionState: currentState,
			Identifier:     uniqueID,
		}

		// The first time we put it in the cache, we know it won't have succeeded so we don't need to look at it
		_, err := cache.GetOrCreate(uniqueID, executionStateCacheItem)
		if err != nil {
			// This means that our cache has fundamentally broken... return a system error
			logger.Errorf(ctx, "Cache failed to GetOrCreate for execution [%s] cache key [%s], owner [%s]. Error %s",
				tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetID(), uniqueID,
				tCtx.TaskExecutionMetadata().GetOwnerReference(), err)
			return currentState, err
		}
	}

	return currentState, nil
}

func MonitorQuery(
	ctx context.Context,
	tCtx core.TaskExecutionContext,
	currentState ExecutionState,
	cache cache.AutoRefresh) (ExecutionState, error) {

	uniqueID := tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()
	executionStateCacheItem := ExecutionStateCacheItem{
		ExecutionState: currentState,
		Identifier:     uniqueID,
	}

	cachedItem, err := cache.GetOrCreate(uniqueID, executionStateCacheItem)
	if err != nil {
		// This means that our cache has fundamentally broken... return a system error
		logger.Errorf(ctx, "Cache is broken on execution [%s] cache key [%s], owner [%s]. Error %s",
			tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetID(), uniqueID,
			tCtx.TaskExecutionMetadata().GetOwnerReference(), err)
		return currentState, errors.Wrapf(errors.CacheFailed, err, "Error when GetOrCreate while monitoring")
	}

	cachedExecutionState, ok := cachedItem.(ExecutionStateCacheItem)
	if !ok {
		logger.Errorf(ctx, "Error casting cache object into ExecutionState")
		return currentState, errors.Errorf(errors.CacheFailed, "Failed to cast [%v]", cachedItem)
	}

	// If there were updates made to the state, we'll have picked them up automatically. Nothing more to do.
	return cachedExecutionState.ExecutionState, nil
}

func MapExecutionStateToPhaseInfo(state ExecutionState) core.PhaseInfo {
	var phaseInfo core.PhaseInfo
	t := time.Now()

	switch state.Phase {
	case PhaseNotStarted:
		phaseInfo = core.PhaseInfoNotReady(t, core.DefaultPhaseVersion, "Haven't received allocation token")
	case PhaseQueued:
		// TODO: Turn into config
		if state.CreationFailureCount > 5 {
			phaseInfo = core.PhaseInfoRetryableFailure("PrestoFailure", "Too many creation attempts", nil)
		} else {
			phaseInfo = core.PhaseInfoQueued(t, uint32(state.CreationFailureCount), "Waiting for Presto launch")
		}
	case PhaseSubmitted:
		phaseInfo = core.PhaseInfoRunning(core.DefaultPhaseVersion, ConstructTaskInfo(state))

	case PhaseQuerySucceeded:
		phaseInfo = core.PhaseInfoSuccess(ConstructTaskInfo(state))

	case PhaseQueryFailed:
		phaseInfo = core.PhaseInfoFailure(errors.DownstreamSystemError, "Query failed", ConstructTaskInfo(state))
	}

	return phaseInfo
}

func ConstructTaskInfo(e ExecutionState) *core.TaskInfo {
	logs := make([]*idlCore.TaskLog, 0, 1)
	t := time.Now()
	if e.CommandID != "" {
		logs = append(logs, ConstructTaskLog(e))
		return &core.TaskInfo{
			Logs:       logs,
			OccurredAt: &t,
		}
	}

	return nil
}

func ConstructTaskLog(e ExecutionState) *idlCore.TaskLog {
	return &idlCore.TaskLog{
		Name:          fmt.Sprintf("Status: %s [%s]", e.Phase, e.CommandID),
		MessageFormat: idlCore.TaskLog_UNKNOWN,
		Uri:           e.URI,
	}
}

func Abort(ctx context.Context, currentState ExecutionState, client svc.ServiceClient) error {
	// Cancel Presto query if non-terminal state
	if !InTerminalState(currentState) && currentState.CommandID != "" {
		err := client.KillCommand(ctx, currentState.CommandID)
		if err != nil {
			logger.Errorf(ctx, "Error terminating Presto command in Finalize [%s]", err)
			return err
		}
	}
	return nil
}

func Finalize(ctx context.Context, tCtx core.TaskExecutionContext, _ ExecutionState) error {
	// Release allocation token
	uniqueID := tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()
	routingGroup, err := composeResourceNamespaceWithRoutingGroup(ctx, tCtx)
	if err != nil {
		return errors.Wrapf(errors.ResourceManagerFailure, err, "Error getting query info when releasing allocation token %s", uniqueID)
	}

	err = tCtx.ResourceManager().ReleaseResource(ctx, routingGroup, uniqueID)

	if err != nil {
		logger.Errorf(ctx, "Error releasing allocation token [%s] in Finalize [%s]", uniqueID, err)
		return err
	}
	return nil
}

func InTerminalState(e ExecutionState) bool {
	return e.Phase == PhaseQuerySucceeded || e.Phase == PhaseQueryFailed
}

func IsNotYetSubmitted(e ExecutionState) bool {
	if e.Phase == PhaseNotStarted || e.Phase == PhaseQueued {
		return true
	}
	return false
}
