package raw_container

import (
	"context"

	v1 "k8s.io/api/core/v1"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery"
	pluginsCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/flytek8s"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/flytek8s/config"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/k8s"
	"github.com/lyft/flyteplugins/go/tasks/plugins/k8s/container"
)

const (
	containerTaskType     = "raw_container"
	flyteDataConfigVolume = "data-config-volume"
	flyteDataConfigPath   = "/etc/flyte/config-data"
	flyteDataConfigMap    = "flyte-data-config"
)

type Plugin struct {
	container.Plugin
}

// Creates a new Pod that will Exit on completion. The pods have no retries by design
func (Plugin) BuildResource(ctx context.Context, taskCtx pluginsCore.TaskExecutionContext) (k8s.Resource, error) {

	podSpec, err := ToK8sPodSpec(ctx, config.GetK8sPluginConfig().CoPilot, taskCtx.TaskExecutionMetadata(), taskCtx.TaskReader(), taskCtx.InputReader(), taskCtx.OutputWriter())
	if err != nil {
		return nil, err
	}

	pod := flytek8s.BuildPodWithSpec(podSpec)

	// We want to Also update the serviceAccount to the serviceaccount of the workflow
	pod.Spec.ServiceAccountName = taskCtx.TaskExecutionMetadata().GetK8sServiceAccount()

	return pod, nil
}

func init() {
	pluginmachinery.PluginRegistry().RegisterK8sPlugin(
		k8s.PluginEntry{
			ID:                  containerTaskType,
			RegisteredTaskTypes: []pluginsCore.TaskType{containerTaskType},
			ResourceToWatch:     &v1.Pod{},
			Plugin:              Plugin{},
			IsDefault:           false,
		})
}
