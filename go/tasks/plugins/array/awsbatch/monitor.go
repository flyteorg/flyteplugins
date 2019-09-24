package awsbatch

import (
	"context"
	"fmt"

	array2 "github.com/lyft/flyteplugins/go/tasks/plugins/array"
	arraystatus2 "github.com/lyft/flyteplugins/go/tasks/plugins/array/arraystatus"
	config2 "github.com/lyft/flyteplugins/go/tasks/plugins/array/awsbatch/config"
	bitarray2 "github.com/lyft/flyteplugins/go/tasks/plugins/array/bitarray"
	errorcollector2 "github.com/lyft/flyteplugins/go/tasks/plugins/array/errorcollector"

	core2 "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
)

func CheckSubTasksState(ctx context.Context, tCtx core.TaskExecutionContext, jobStore *JobStore, cfg *config2.Config, currentState *array2.State) (
	newState *array2.State, err error) {
	logLinks := make([]*core2.TaskLog, 0, 4)
	newState = currentState

	msg := errorcollector2.NewErrorMessageCollector()
	newArrayStatus := arraystatus2.ArrayStatus{
		Summary:  arraystatus2.ArraySummary{},
		Detailed: newStatusCompactArray(uint(currentState.GetExecutionArraySize())),
	}

	jobName := tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()
	job := jobStore.Get(jobName)
	// If job isn't currently being monitored (recovering from a restart?), add it to the sync-cache and return
	if job == nil {
		_, err = jobStore.GetOrCreate(jobName, &Job{
			Id:             *currentState.ExternalJobID,
			OwnerReference: tCtx.TaskExecutionMetadata().GetOwnerID(),
			SubJobs:        make([]*Job, currentState.GetExecutionArraySize()),
		})

		if err != nil {
			return nil, err
		}

		return currentState, nil
	}

	for childIdx, existingPhaseIdx := range currentState.GetArrayStatus().Detailed.GetItems() {
		existingPhase := core.Phases[existingPhaseIdx]
		if existingPhase.IsTerminal() {
			// If we get here it means we have already "processed" this terminal phase since we will only persist
			// the phase after all processing is done (e.g. check outputs/errors file, record events... etc.).
			newArrayStatus.Summary.Inc(existingPhase)

			// TODO: collect log links before doing this
			continue
		}

		subJob := job.SubJobs[childIdx]
		originalIndex := calculateOriginalIndex(childIdx, currentState.IndexesToCache)
		logLinks = append(logLinks, &core2.TaskLog{
			Name: fmt.Sprintf("AWS Batch Job #%v", originalIndex),
			Uri:  fmt.Sprintf(JobFormatter, jobStore.GetRegion(), job.Id, job.Id, childIdx),
		})

		for _, attempt := range subJob.Attempts {
			logLinks = append(logLinks, &core2.TaskLog{
				Name: fmt.Sprintf("AWS Batch #%v (%v)", originalIndex, subJob.Status.Phase),
				Uri:  fmt.Sprintf(LogStreamFormatter, jobStore.GetRegion(), attempt.LogStream),
			})
		}

		if subJob.Status.Phase.IsFailure() {
			if len(subJob.Status.Message) > 0 {
				msg.Collect(childIdx, subJob.Status.Message)
			}
		}

		newArrayStatus.Detailed.SetItem(childIdx, bitarray2.Item(subJob.Status.Phase))
		newArrayStatus.Summary.Inc(subJob.Status.Phase)
	}

	newState = newState.SetArrayStatus(newArrayStatus)

	phase := array2.SummaryToPhase(ctx, currentState.OriginalMinSuccesses-currentState.OriginalArraySize-int64(currentState.ExecutionArraySize), newArrayStatus.Summary)
	if phase == array2.PhasePermanentFailure || phase == array2.PhaseRetryableFailure {
		errorMsg := msg.Summary(cfg.MaxErrorStringLength)
		newState = newState.SetReason(errorMsg)
	}

	if phase == array2.PhaseCheckingSubTaskExecutions {
		newPhaseVersion := uint32(0)
		if phase == array2.PhaseCheckingSubTaskExecutions {
			// For now, the only changes to PhaseVersion and PreviousSummary occur for running array jobs.
			for phase, count := range newState.GetArrayStatus().Summary {
				newPhaseVersion += uint32(phase) * uint32(count)
			}
		}

		newState = newState.SetPhase(phase, newPhaseVersion)
	} else {
		newState = newState.SetPhase(phase, core.DefaultPhaseVersion)
	}

	return newState, nil
}

func calculateOriginalIndex(childIdx int, toCache *bitarray2.BitSet) int {
	originalIdx := 0
	targetIdx := -1
	for i := uint(0); i < uint(toCache.Len()) && targetIdx < childIdx; i++ {
		if !toCache.IsSet(i) {
			originalIdx++
		} else {
			targetIdx++
		}
	}

	return originalIdx
}
