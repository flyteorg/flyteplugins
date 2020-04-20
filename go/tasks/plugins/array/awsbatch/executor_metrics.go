package awsbatch

import (
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/promutils/labeled"
)

type ExecutorMetrics struct {
	Scope                promutils.Scope
	BatchTasksStarted    labeled.Counter
	BatchTasksSuccess    labeled.Counter
	BatchTasksFailure    labeled.Counter
	BatchTasksTerminated labeled.Counter
}

func getAwsBatchExecutorMetrics(scope promutils.Scope) ExecutorMetrics {
	return ExecutorMetrics{
		Scope: scope,
		BatchTasksStarted: labeled.NewCounter("batch_task_started",
			"Batch tasks started", scope),
		BatchTasksSuccess: labeled.NewCounter("batch_task_success",
			"Batch tasks successful", scope),
		BatchTasksFailure: labeled.NewCounter("batch_task_failure",
			"Batch tasks failure", scope),
		BatchTasksTerminated: labeled.NewCounter("batch_task_terminated",
			"Batch tasks terminated", scope),
	}
}
