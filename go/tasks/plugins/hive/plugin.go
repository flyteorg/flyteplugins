package hive

import (
	"context"

	"github.com/lyft/flyteplugins/go/tasks/errors"

	"github.com/lyft/flyteplugins/go/tasks/plugins/hive/client"
	"github.com/lyft/flyteplugins/go/tasks/plugins/hive/config"

	pluginMachinery "github.com/lyft/flyteplugins/go/tasks/pluginmachinery"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/remote"
)

type QuboleHivePlugin struct {
	client         client.QuboleClient
	resourceQuotas map[core.ResourceNamespace]int
	properties     remote.PluginProperties
}

func (q QuboleHivePlugin) GetPluginProperties() remote.PluginProperties {
	return q.properties
}

func (q QuboleHivePlugin) ResourceRequirements(ctx context.Context, tCtx remote.PluginContext) (
	namespace core.ResourceNamespace, constraints core.ResourceConstraintsSpec, err error) {
	uniqueID := tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()

	clusterPrimaryLabel, err := composeResourceNamespaceWithClusterPrimaryLabel(ctx, tCtx)
	if err != nil {
		return "", core.ResourceConstraintsSpec{}, errors.Wrapf(errors.ResourceManagerFailure, err, "Error getting query info when requesting allocation token %s", uniqueID)
	}

	resourceConstraintsSpec := createResourceConstraintsSpec(ctx, tCtx, clusterPrimaryLabel)
	return clusterPrimaryLabel, resourceConstraintsSpec, nil
}

func (q QuboleHivePlugin) Create(ctx context.Context, tCtx remote.PluginContext) (createdResources remote.ResourceKey, err error) {
	panic("implement me")
}

func (q QuboleHivePlugin) Get(ctx context.Context, key remote.ResourceKey) (resource remote.Resource, err error) {
	panic("implement me")
}

func (q QuboleHivePlugin) Delete(ctx context.Context, key remote.ResourceKey) error {
	panic("implement me")
}

func (q QuboleHivePlugin) Status(ctx context.Context, resource remote.Resource) (phase core.PhaseInfo, err error) {
	panic("implement me")
}

func QuboleHivePluginLoader(ctx context.Context, iCtx remote.PluginSetupContext) (
	remote.Plugin, error) {

	cfg := config.GetQuboleConfig()

	return QuboleHivePlugin{
		client: client.NewQuboleClient(cfg),
		properties: remote.PluginProperties{
			ResourceQuotas:   BuildResourceConfig(cfg.ClusterConfigs),
			ReadRateLimiter:  cfg.ReadRateLimiter,
			WriteRateLimiter: cfg.WriteRateLimiter,
			Caching:          cfg.Caching,
		},
	}, nil
}

func init() {
	pluginMachinery.PluginRegistry().RegisterRemotePlugin(
		remote.PluginEntry{
			ID:                 quboleHiveExecutorID,
			SupportedTaskTypes: []core.TaskType{hiveTaskType},
			PluginLoader:       QuboleHivePluginLoader,
		})
}
