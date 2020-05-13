package hive

import (
	"fmt"
	"time"

	idlCore "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/lyft/flyteplugins/go/tasks/errors"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
)

type Resource struct {
	CommandID string
	Phase     ExecutionPhase
	URI       string
}

func (r Resource) ConstructTaskInfo() *core.TaskInfo {
	logs := make([]*idlCore.TaskLog, 0, 1)
	t := time.Now()
	logs = append(logs)
	return &core.TaskInfo{
		Logs: []*idlCore.TaskLog{
			{
				Name:          fmt.Sprintf("Status: %s [%s]", r.Phase, r.CommandID),
				MessageFormat: idlCore.TaskLog_UNKNOWN,
				Uri:           r.URI,
			},
		},
		OccurredAt: &t,
	}
}

func (r Resource) GetPhaseInfo() core.PhaseInfo {
	var phaseInfo core.PhaseInfo
	t := time.Now()

	switch r.Phase {
	case PhaseNotStarted:
		phaseInfo = core.PhaseInfoNotReady(t, core.DefaultPhaseVersion, "Haven't received allocation token")
	case PhaseQueued:
		phaseInfo = core.PhaseInfoQueued(t, core.DefaultPhaseVersion, "Waiting for Qubole launch")
	case PhaseSubmitted:
		phaseInfo = core.PhaseInfoRunning(core.DefaultPhaseVersion, r.ConstructTaskInfo())
	case PhaseQuerySucceeded:
		phaseInfo = core.PhaseInfoSuccess(r.ConstructTaskInfo())
	case PhaseQueryFailed:
		phaseInfo = core.PhaseInfoFailure(errors.DownstreamSystemError, "Query failed", r.ConstructTaskInfo())
	}

	return phaseInfo
}
