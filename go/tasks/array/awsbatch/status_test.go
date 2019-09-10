/*
 * Copyright (c) 2018 Lyft. All rights reserved.
 */

package awsbatch

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/batch"
	"github.com/lyft/flyteplugins/go/tasks/v1/types"
	"github.com/stretchr/testify/assert"
)

func TestToTaskPhase(t *testing.T) {
	assert.Equal(t, types.TaskPhaseQueued, ToTaskPhase(batch.JobStatusSubmitted))
	assert.Equal(t, types.TaskPhaseQueued, ToTaskPhase(batch.JobStatusPending))
	assert.Equal(t, types.TaskPhaseQueued, ToTaskPhase(batch.JobStatusRunnable))
	assert.Equal(t, types.TaskPhaseQueued, ToTaskPhase(batch.JobStatusStarting))
	assert.Equal(t, types.TaskPhaseRunning, ToTaskPhase(batch.JobStatusRunning))
	assert.Equal(t, types.TaskPhaseSucceeded, ToTaskPhase(batch.JobStatusSucceeded))
	assert.Equal(t, types.TaskPhasePermanentFailure, ToTaskPhase(batch.JobStatusFailed))
}
