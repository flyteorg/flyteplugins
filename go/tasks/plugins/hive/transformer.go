package hive

import (
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/plugins/hive/client"
	"github.com/lyft/flyteplugins/go/tasks/plugins/hive/config"
)

func QuboleStatusToPhase(status client.QuboleStatus) core.Phase {
	switch status {
	case client.QuboleStatusWaiting:
		return core.PhaseNotReady
	case client.QuboleStatusDone:
		return core.PhaseSuccess
	case client.QuboleStatusRunning:
		return core.PhaseRunning
	case client.QuboleStatusCancelled:
		fallthrough
	case client.QuboleStatusError:
		fallthrough
	default:
		return core.PhaseRetryableFailure
	}
}

func BuildResourceConfig(cfg []config.ClusterConfig) map[core.ResourceNamespace]int {
	resourceConfig := make(map[core.ResourceNamespace]int, len(cfg))

	for _, clusterCfg := range cfg {
		resourceConfig[core.ResourceNamespace(clusterCfg.PrimaryLabel)] = clusterCfg.Limit
	}

	return resourceConfig
}
