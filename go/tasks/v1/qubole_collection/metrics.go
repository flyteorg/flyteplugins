package qubole_collection

import (
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/promutils/labeled"
)

type HiveExecutorMetrics struct {
	Scope                 promutils.Scope
	ReleaseResourceFailed labeled.Counter
	AllocationGranted     labeled.Counter
	AllocationNotGranted  labeled.Counter
}

func NewHiveExecutorMetrics(scope promutils.Scope) HiveExecutorMetrics {
	return HiveExecutorMetrics{
		Scope: scope,
		ReleaseResourceFailed: labeled.NewCounter("released_resource_failed",
			"Error releasing allocation token", scope),
		AllocationGranted: labeled.NewCounter("allocation_granted",
			"Allocation request granted", scope),
		AllocationNotGranted: labeled.NewCounter("allocation_not_granted",
			"Allocation request did not fail but not granted", scope),
	}
}

