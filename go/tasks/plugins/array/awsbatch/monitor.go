package awsbatch

import (
	"context"

	arrayCore "github.com/lyft/flyteplugins/go/tasks/plugins/array/core"

	"github.com/lyft/flytestdlib/bitarray"

	"github.com/lyft/flyteplugins/go/tasks/plugins/array/arraystatus"
	"github.com/lyft/flyteplugins/go/tasks/plugins/array/awsbatch/config"
	"github.com/lyft/flyteplugins/go/tasks/plugins/array/errorcollector"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
)

func createSubJobList(count int) []*Job {
	res := make([]*Job, count)
	for i := range res {
		res[i] = &Job{
			Status: JobStatus{Phase: core.PhaseNotReady},
		}
	}

	return res
}

func CheckSubTasksState(ctx context.Context, taskMeta core.TaskExecutionMetadata, jobStore *JobStore,
	cfg *config.Config, currentState *State) (newState *State, err error) {

	newState = currentState
	parentState := currentState.State

	msg := errorcollector.NewErrorMessageCollector()
	newArrayStatus := arraystatus.ArrayStatus{
		Summary:  arraystatus.ArraySummary{},
		Detailed: arrayCore.NewPhasesCompactArray(uint(currentState.GetExecutionArraySize())),
	}

	jobName := taskMeta.GetTaskExecutionID().GetGeneratedName()
	job := jobStore.Get(jobName)
	// If job isn't currently being monitored (recovering from a restart?), add it to the sync-cache and return
	if job == nil {
		_, err = jobStore.GetOrCreate(jobName, &Job{
			ID:             *currentState.ExternalJobID,
			OwnerReference: taskMeta.GetOwnerID(),
			SubJobs:        createSubJobList(currentState.GetExecutionArraySize()),
		})

		if err != nil {
			return nil, err
		}

		return currentState, nil
	}

	for childIdx, subJob := range job.SubJobs {
		if subJob.Status.Phase.IsFailure() {
			if len(subJob.Status.Message) > 0 {
				msg.Collect(childIdx, subJob.Status.Message)
			}
		}

		newArrayStatus.Detailed.SetItem(childIdx, bitarray.Item(subJob.Status.Phase))
		newArrayStatus.Summary.Inc(subJob.Status.Phase)
	}

	parentState = parentState.SetArrayStatus(newArrayStatus)

	phase := arrayCore.SummaryToPhase(ctx, currentState.GetOriginalMinSuccesses()-currentState.GetOriginalArraySize()+int64(currentState.GetExecutionArraySize()), newArrayStatus.Summary)
	if phase == arrayCore.PhasePermanentFailure || phase == arrayCore.PhaseRetryableFailure {
		errorMsg := msg.Summary(cfg.MaxErrorStringLength)
		parentState = parentState.SetReason(errorMsg)
	}

	if phase == arrayCore.PhaseCheckingSubTaskExecutions {
		newPhaseVersion := uint32(0)
		if phase == arrayCore.PhaseCheckingSubTaskExecutions {
			// For now, the only changes to PhaseVersion and PreviousSummary occur for running array jobs.
			for phase, count := range parentState.GetArrayStatus().Summary {
				newPhaseVersion += uint32(phase) * uint32(count)
			}
		}

		parentState = parentState.SetPhase(phase, newPhaseVersion)
	} else {
		parentState = parentState.SetPhase(phase, core.DefaultPhaseVersion)
	}

	newState.State = parentState
	return newState, nil
}
