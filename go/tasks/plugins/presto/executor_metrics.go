package presto

import (
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/promutils/labeled"
)

type ExecutorMetrics struct {
	Scope                 promutils.Scope
	Succeeded             labeled.Counter
	Failed                labeled.Counter
	ResourceReleased      labeled.Counter
	ResourceReleaseFailed labeled.Counter
	AllocationGranted     labeled.Counter
	AllocationNotGranted  labeled.Counter
}

func getPrestoExecutorMetrics(scope promutils.Scope) ExecutorMetrics {
	return ExecutorMetrics{
		Scope: scope,
		Succeeded: labeled.NewCounter("success",
			"Task finished successfully", scope, labeled.EmitUnlabeledMetric),
		Failed: labeled.NewCounter("failure",
			"Task failed", scope, labeled.EmitUnlabeledMetric),
		ResourceReleased: labeled.NewCounter("resource_release_success",
			"Resource allocation token released", scope, labeled.EmitUnlabeledMetric),
		ResourceReleaseFailed: labeled.NewCounter("resource_release_failed",
			"Error releasing allocation token", scope, labeled.EmitUnlabeledMetric),
		AllocationGranted: labeled.NewCounter("allocation_grant_success",
			"Allocation request granted", scope, labeled.EmitUnlabeledMetric),
		AllocationNotGranted: labeled.NewCounter("allocation_grant_failed",
			"Allocation request did not fail but not granted", scope, labeled.EmitUnlabeledMetric),
	}
}
