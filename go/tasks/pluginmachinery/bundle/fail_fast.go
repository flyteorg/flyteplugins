package bundle

import (
	"context"
	"fmt"
	"time"

	pluginMachinery "github.com/lyft/flyteplugins/go/tasks/pluginmachinery"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
)

const failFastExecutorName = "fail-fast"

type failFastHandler struct{}

func (h failFastHandler) GetID() string {
	return failFastExecutorName
}

func (h failFastHandler) GetProperties() core.PluginProperties {
	return core.PluginProperties{}
}

func (h failFastHandler) Handle(_ context.Context, tCtx core.TaskExecutionContext) (core.Transition, error) {
	occuredAt := time.Now()
	return core.DoTransition(core.PhaseInfoFailure("AlwaysFail",
		fmt.Sprintf("Task type [%v] not supported by platform for this project/domain/workflow",
			tCtx.TaskExecutionMetadata().GetTaskExecutionID()), &core.TaskInfo{
			OccurredAt: &occuredAt,
		})), nil
}

func (h failFastHandler) Abort(_ context.Context, _ core.TaskExecutionContext) error {
	return nil
}

func (h failFastHandler) Finalize(_ context.Context, _ core.TaskExecutionContext) error {
	return nil
}

func failFastPluginLoader(_ context.Context, _ core.SetupContext) (core.Plugin, error) {
	return &failFastHandler{}, nil
}

func init() {
	// TODO(katrogan): Once we move pluginmachinery to flyteidl make these task types named constants that flyteplugins
	// can reference in other handler definitions.
	// NOTE: these should match the constants defined flytekit
	taskTypes := []core.TaskType{
		"container", "sidecar", "container_array", "hive", "presto", "spark", "pytorch",
		"sagemaker_custom_training_job_task", "sagemaker_training_job_task", "sagemaker_hyperparameter_tuning_job_task",
	}
	pluginMachinery.PluginRegistry().RegisterCorePlugin(
		core.PluginEntry{
			ID:                  failFastExecutorName,
			RegisteredTaskTypes: taskTypes,
			LoadPlugin:          failFastPluginLoader,
			IsDefault:           false,
		})
}
