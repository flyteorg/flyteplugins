package presto

import (
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/promutils/labeled"
)

type ExecutorMetrics struct {
	Scope                 promutils.Scope
	ResourceReleased      labeled.Counter
	ResourceReleaseFailed labeled.Counter
	AllocationGranted     labeled.Counter
	AllocationNotGranted  labeled.Counter
}

func getPrestoExecutorMetrics(scope promutils.Scope) ExecutorMetrics {
	return ExecutorMetrics{
		Scope: scope,
		ResourceReleased: labeled.NewCounter("resource_release_success",
			"Resource allocation token released", scope),
		ResourceReleaseFailed: labeled.NewCounter("resource_release_failed",
			"Error releasing allocation token", scope),
		AllocationGranted: labeled.NewCounter("allocation_grant_success",
			"Allocation request granted", scope),
		AllocationNotGranted: labeled.NewCounter("allocation_grant_failed",
			"Allocation request did not fail but not granted", scope),
	}
}
