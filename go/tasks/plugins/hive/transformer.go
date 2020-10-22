package hive

import (
	"context"
	"fmt"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"
	"github.com/lyft/flyteplugins/go/tasks/errors"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/remote"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/utils"
	"github.com/lyft/flyteplugins/go/tasks/plugins/hive/config"
	"github.com/lyft/flytestdlib/logger"
)

func BuildResourceConfig(cfg []config.ClusterConfig) map[core.ResourceNamespace]int {
	resourceConfig := make(map[core.ResourceNamespace]int, len(cfg))

	for _, clusterCfg := range cfg {
		resourceConfig[core.ResourceNamespace(clusterCfg.PrimaryLabel)] = clusterCfg.Limit
	}

	return resourceConfig
}

func composeResourceNamespaceWithClusterPrimaryLabel(ctx context.Context, tCtx remote.TaskExecutionContext) (core.ResourceNamespace, error) {
	_, _, clusterLabelOverride, _, _, err := GetQueryInfo(ctx, tCtx)
	if err != nil {
		return "", err
	}

	clusterPrimaryLabel := getClusterPrimaryLabel(ctx, tCtx, clusterLabelOverride)
	return core.ResourceNamespace(clusterPrimaryLabel), nil
}

func createResourceConstraintsSpec(ctx context.Context, _ remote.TaskExecutionContext, targetClusterPrimaryLabel core.ResourceNamespace) core.ResourceConstraintsSpec {
	cfg := config.GetQuboleConfig()
	constraintsSpec := core.ResourceConstraintsSpec{
		ProjectScopeResourceConstraint:   nil,
		NamespaceScopeResourceConstraint: nil,
	}
	if cfg.ClusterConfigs == nil {
		logger.Infof(ctx, "No cluster config is found. Returning an empty resource constraints spec")
		return constraintsSpec
	}
	for _, cluster := range cfg.ClusterConfigs {
		if cluster.PrimaryLabel == string(targetClusterPrimaryLabel) {
			constraintsSpec.ProjectScopeResourceConstraint = &core.ResourceConstraint{Value: int64(float64(cluster.Limit) * cluster.ProjectScopeQuotaProportionCap)}
			constraintsSpec.NamespaceScopeResourceConstraint = &core.ResourceConstraint{Value: int64(float64(cluster.Limit) * cluster.NamespaceScopeQuotaProportionCap)}
			break
		}
	}
	logger.Infof(ctx, "Created a resource constraints spec: [%v]", constraintsSpec)
	return constraintsSpec
}

func validateQuboleHiveJob(hiveJob plugins.QuboleHiveJob) error {
	if hiveJob.Query == nil {
		return errors.Errorf(errors.BadTaskSpecification,
			"Query could not be found. Please ensure that you are at least on Flytekit version 0.3.0 or later.")
	}
	return nil
}

// This function is the link between the output written by the SDK, and the execution side. It extracts the query
// out of the task template.
func GetQueryInfo(ctx context.Context, tCtx remote.TaskExecutionContext) (
	taskName, query, cluster string, tags []string, timeoutSec uint32, err error) {

	taskTemplate, err := tCtx.TaskReader().Read(ctx)
	if err != nil {
		return "", "", "", []string{}, 0, err
	}

	hiveJob := plugins.QuboleHiveJob{}
	err = utils.UnmarshalStruct(taskTemplate.GetCustom(), &hiveJob)
	if err != nil {
		return "", "", "", []string{}, 0, err
	}

	if err := validateQuboleHiveJob(hiveJob); err != nil {
		return "", "", "", []string{}, 0, err
	}

	query = hiveJob.Query.GetQuery()
	cluster = hiveJob.ClusterLabel
	timeoutSec = hiveJob.Query.TimeoutSec
	taskName = taskTemplate.Id.Name
	tags = hiveJob.Tags
	tags = append(tags, fmt.Sprintf("ns:%s", tCtx.TaskExecutionMetadata().GetNamespace()))
	for k, v := range tCtx.TaskExecutionMetadata().GetLabels() {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}

	logger.Debugf(ctx, "QueryInfo: query: [%v], cluster: [%v], timeoutSec: [%v], tags: [%v]", query, cluster, timeoutSec, tags)
	return
}

func mapLabelToPrimaryLabel(ctx context.Context, quboleCfg *config.Config, label string) (primaryLabel string, found bool) {
	primaryLabel = quboleCfg.DefaultClusterLabel
	found = false

	if label == "" {
		logger.Debugf(ctx, "Input cluster label is an empty string; falling back to using the default primary label [%v]", label, primaryLabel)
		return
	}

	// Using a linear search because N is small and because of ClusterConfig's struct definition
	// which is determined specifically for the readability of the corresponding configmap yaml file
	for _, clusterCfg := range quboleCfg.ClusterConfigs {
		for _, l := range clusterCfg.Labels {
			if label != "" && l == label {
				logger.Debugf(ctx, "Found the primary label [%v] for label [%v]", clusterCfg.PrimaryLabel, label)
				primaryLabel, found = clusterCfg.PrimaryLabel, true
				break
			}
		}
	}

	if !found {
		logger.Debugf(ctx, "Cannot find the primary cluster label for label [%v] in configmap; "+
			"falling back to using the default primary label [%v]", label, primaryLabel)
	}

	return primaryLabel, found
}

func mapProjectDomainToDestinationClusterLabel(ctx context.Context, tCtx remote.TaskExecutionContext, quboleCfg *config.Config) (string, bool) {
	tExecID := tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetID()
	project := tExecID.NodeExecutionId.GetExecutionId().GetProject()
	domain := tExecID.NodeExecutionId.GetExecutionId().GetDomain()
	logger.Debugf(ctx, "No clusterLabelOverride. Finding the pre-defined cluster label for (project: %v, domain: %v)", project, domain)
	// Using a linear search because N is small
	for _, m := range quboleCfg.DestinationClusterConfigs {
		if project == m.Project && domain == m.Domain {
			logger.Debugf(ctx, "Found the pre-defined cluster label [%v] for (project: %v, domain: %v)", m.ClusterLabel, project, domain)
			return m.ClusterLabel, true
		}
	}

	// This function finds the label, not primary label, so in the case where no mapping is found, this function should return an empty string
	return "", false
}

func getClusterPrimaryLabel(ctx context.Context, tCtx remote.TaskExecutionContext, clusterLabelOverride string) string {
	cfg := config.GetQuboleConfig()

	// If override is not empty and if it has a mapping, we return the mapped primary label
	if clusterLabelOverride != "" {
		if primaryLabel, found := mapLabelToPrimaryLabel(ctx, cfg, clusterLabelOverride); found {
			return primaryLabel
		}
	}

	// If override is empty or if the override does not have a mapping, we return the primary label mapped using (project, domain)
	if clusterLabel, found := mapProjectDomainToDestinationClusterLabel(ctx, tCtx, cfg); found {
		primaryLabel, _ := mapLabelToPrimaryLabel(ctx, cfg, clusterLabel)
		return primaryLabel
	}

	// Else we return the default primary label
	return cfg.DefaultClusterLabel
}
