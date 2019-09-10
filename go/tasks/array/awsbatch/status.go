/*
 * Copyright (c) 2018 Lyft. All rights reserved.
 */

package awsbatch

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/batch"
	"github.com/lyft/flytedynamicjoboperator/pkg/internal"
	"github.com/lyft/flyteplugins/go/tasks/v1/types"
)

var PhaseManager, _ = internal.NewPhaseManager(
	[]string{
		"",
		batch.JobStatusSubmitted,
		batch.JobStatusRunnable,
		batch.JobStatusPending,
		batch.JobStatusRunning,
		batch.JobStatusFailed,
		batch.JobStatusSucceeded,
	},
	[]string{batch.JobStatusSucceeded},
	[]string{batch.JobStatusFailed})

func ToTaskPhase(status JobPhaseType) types.TaskPhase {
	switch status {
	case batch.JobStatusSubmitted:
		fallthrough
	case batch.JobStatusPending:
		fallthrough
	case batch.JobStatusRunnable:
		fallthrough
	case batch.JobStatusStarting:
		return types.TaskPhaseQueued
	case batch.JobStatusRunning:
		return types.TaskPhaseRunning
	case batch.JobStatusFailed:
		return types.TaskPhasePermanentFailure
	case batch.JobStatusSucceeded:
		return types.TaskPhaseSucceeded
	default:
		// TODO: Should there be an Unknown Status?
		return types.TaskPhaseQueued
	}
}

func ToTaskStatus(jobPhase JobPhaseType, message string) types.TaskStatus {
	p := ToTaskPhase(jobPhase)
	s := types.TaskStatus{
		Phase: p,
	}
	if p.IsRetryableFailure() || p.IsPermanentFailure() {
		s.Err = fmt.Errorf(message)
	}
	return s
}
