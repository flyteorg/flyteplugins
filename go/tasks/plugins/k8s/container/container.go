package container

import (
	"context"

	"github.com/lyft/flytestdlib/logger"

	v1 "k8s.io/api/core/v1"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery"
	pluginsCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/flytek8s"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/k8s"
)

const (
	containerTaskType = "container"
)

type containerTaskExecutor struct {
}

func (containerTaskExecutor) GetTaskPhase(ctx context.Context, pluginContext k8s.PluginContext, r k8s.Resource) (pluginsCore.PhaseInfo, error) {
	pod := r.(*v1.Pod)
	return flytek8s.GetTaskPhaseFromPod(ctx, pod, flytek8s.UserOnly)
}

// Creates a new Pod that will Exit on completion. The pods have no retries by design
func (containerTaskExecutor) BuildResource(ctx context.Context, taskCtx pluginsCore.TaskExecutionContext) (k8s.Resource, error) {

	task, err := taskCtx.TaskReader().Read(ctx)
	if err != nil {
		logger.Warnf(ctx, "failed to read task information when trying to construct Pod, err: %s", err.Error())
		return nil, err
	}
	c, err := flytek8s.ToK8sContainer(taskCtx.TaskExecutionMetadata(), task.GetContainer())
	if err != nil {
		return nil, err
	}
	err = flytek8s.AddFlyteModificationsForContainer(ctx, taskCtx, c)
	if err != nil {
		return nil, err
	}
	var podSpec v1.PodSpec
	flytek8s.AddFlyteModificationsForPodSpec(taskCtx, []v1.Container{*c}, []v1.ResourceRequirements{c.Resources}, &podSpec)
	return flytek8s.BuildPodWithSpec(&podSpec), nil
}

func (containerTaskExecutor) BuildIdentityResource(_ context.Context, _ pluginsCore.TaskExecutionMetadata) (k8s.Resource, error) {
	return flytek8s.BuildIdentityPod(), nil
}

func init() {
	pluginmachinery.PluginRegistry().RegisterK8sPlugin(
		k8s.PluginEntry{
			ID:                  containerTaskType,
			RegisteredTaskTypes: []pluginsCore.TaskType{containerTaskType},
			ResourceToWatch:     &v1.Pod{},
			Plugin:              containerTaskExecutor{},
			IsDefault:           true,
		})
}
