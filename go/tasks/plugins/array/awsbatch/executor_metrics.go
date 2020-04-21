package awsbatch

import (
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/promutils/labeled"
)

type ExecutorMetrics struct {
	Scope                promutils.Scope
	SubTasksSubmitted    labeled.Counter
	SubTasksSucceeded    labeled.Counter
	SubTasksFailed       labeled.Counter
	BatchTasksTerminated labeled.Counter
}

func getAwsBatchExecutorMetrics(scope promutils.Scope) ExecutorMetrics {
	return ExecutorMetrics{
		Scope: scope,
		SubTasksSubmitted: labeled.NewCounter("sub_task_submitted",
			"Sub tasks submitted", scope),
		SubTasksSucceeded: labeled.NewCounter("batch_task_success",
			"Batch tasks successful", scope),
		SubTasksFailed: labeled.NewCounter("batch_task_failure",
			"Batch tasks failure", scope),
		BatchTasksTerminated: labeled.NewCounter("batch_task_terminated",
			"Batch tasks terminated", scope),
	}
}
