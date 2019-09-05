/*
 * Copyright (c) 2018 Lyft. All rights reserved.
 */

package arraystatus

import (
	"context"
	"testing"

	"github.com/lyft/flyteplugins/go/tasks/v1/types"
	"github.com/stretchr/testify/assert"
)

func TestArraySummary_MergeFrom(t *testing.T) {
	t.Run("Update when not equal", func(t *testing.T) {
		expected := ArraySummary{
			types.TaskPhaseRunning: 1,
		}

		other := ArraySummary{
			types.TaskPhaseRunning: 1,
			types.TaskPhaseQueued:  0,
		}

		actual := ArraySummary{
			types.TaskPhaseRunning:          2,
			types.TaskPhasePermanentFailure: 2,
			types.TaskPhaseQueued:           10,
		}
		updated := MergeFrom(other)
		assert.True(t, updated)

		assert.Equal(t, expected, actual)
	})

	t.Run("Delete when 0", func(t *testing.T) {
		expected := ArraySummary{
			types.TaskPhaseRunning: 1,
		}

		other := ArraySummary{
			types.TaskPhaseRunning: 1,
			types.TaskPhaseQueued:  0,
		}

		actual := ArraySummary{}
		updated := MergeFrom(other)
		assert.True(t, updated)

		assert.Equal(t, expected, actual)
	})

	t.Run("Delete when other nil", func(t *testing.T) {
		expected := ArraySummary{}

		actual := ArraySummary{
			types.TaskPhaseRunning: 10,
		}

		updated := MergeFrom(nil)
		assert.True(t, updated)
		assert.Equal(t, expected, actual)
	})

	t.Run("Not Updated when equal", func(t *testing.T) {
		expected := ArraySummary{
			types.TaskPhaseRunning: 1,
			types.TaskPhaseQueued:  10,
		}

		other := ArraySummary{
			types.TaskPhaseRunning:          1,
			types.TaskPhaseQueued:           10,
			types.TaskPhaseRetryableFailure: 0,
		}

		actual := ArraySummary{
			types.TaskPhaseRunning: 1,
			types.TaskPhaseQueued:  10,
		}
		updated := MergeFrom(other)
		assert.False(t, updated)

		assert.Equal(t, expected, actual)
	})
}

func TestArraySummary_CopyFrom(t *testing.T) {

	original := ArraySummary{
		types.TaskPhaseRunning:          2,
		types.TaskPhasePermanentFailure: 2,
		types.TaskPhaseQueued:           10,
	}
	copy := CopyArraySummaryFrom(original)

	assert.EqualValues(t, original, copy)

	original[types.TaskPhaseSucceeded] = 1
	assert.NotEqual(t, original, copy)
}

func TestArraySummary_Inc(t *testing.T) {
	original := ArraySummary{
		types.TaskPhaseRunning:          2,
		types.TaskPhasePermanentFailure: 2,
		types.TaskPhaseQueued:           10,
	}

	Inc(types.TaskPhaseRunning)
	Inc(types.TaskPhaseRetryableFailure)

	validatedCount := 0
	for phase, count := range original {
		switch phase {
		case types.TaskPhaseRunning:
			assert.Equal(t, int64(3), count)
			validatedCount++
		case types.TaskPhasePermanentFailure:
			assert.Equal(t, int64(2), count)
			validatedCount++
		case types.TaskPhaseQueued:
			assert.Equal(t, int64(10), count)
			validatedCount++
		case types.TaskPhaseRetryableFailure:
			assert.Equal(t, int64(1), count)
			validatedCount++
		}
	}

	assert.Equal(t, 4, validatedCount)
}

func TestCustomState_AsMap(t *testing.T) {
	input := CustomState{
		JobID: "abc",
		ArrayStatus: &ArrayStatus{
			Summary: ArraySummary{
				types.TaskPhaseRunning: 2,
			},
		},
	}

	actual, err := input.AsMap(context.TODO())
	assert.NoError(t, err)
	assert.NotNil(t, actual)
	assert.Equal(t, 2, len(actual))
	assert.Equal(t, "abc", actual["job-id"])
}

func TestCustomState_MergeFrom(t *testing.T) {
	t.Run("OverridingSummary", func(t *testing.T) {
		input := CustomState{
			JobID: "abc",
			ArrayStatus: &ArrayStatus{
				Summary: ArraySummary{
					types.TaskPhaseRunning: 2,
				},
			},
		}

		other, err := CustomState{
			JobID: "abc",
			ArrayStatus: &ArrayStatus{
				Summary: ArraySummary{
					types.TaskPhaseSucceeded: 2,
				},
			},
		}.AsMap(context.TODO())

		assert.NoError(t, err)
		assert.NotNil(t, other)

		assert.NoError(t, input.MergeFrom(context.TODO(), other))
		assert.Equal(t, 1, len(input.ArrayStatus.Summary))
	})

	t.Run("JobID", func(t *testing.T) {
		input := CustomState{}

		other, err := CustomState{
			JobID: "abc",
			ArrayStatus: &ArrayStatus{
				Summary: ArraySummary{
					types.TaskPhaseSucceeded: 2,
				},
			},
		}.AsMap(context.TODO())

		assert.NoError(t, err)
		assert.NotNil(t, other)

		assert.NoError(t, input.MergeFrom(context.TODO(), other))
		assert.Equal(t, 1, len(input.ArrayStatus.Summary))
		assert.Equal(t, "abc", input.JobID)
	})
}
