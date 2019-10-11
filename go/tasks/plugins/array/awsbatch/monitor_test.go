package awsbatch

import (
	"testing"

	"github.com/lyft/flyteplugins/go/tasks/plugins/array/arraystatus"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/batch"

	arrayCore "github.com/lyft/flyteplugins/go/tasks/plugins/array/core"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	"github.com/lyft/flyteplugins/go/tasks/plugins/array/awsbatch/config"
	batchMocks "github.com/lyft/flyteplugins/go/tasks/plugins/array/awsbatch/mocks"
	"github.com/lyft/flytestdlib/utils"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"k8s.io/apimachinery/pkg/types"

	"github.com/lyft/flytestdlib/bitarray"
)

func Test_calculateOriginalIndex(t *testing.T) {
	inputArr := bitarray.NewBitSet(7)
	inputArr.Set(3)
	inputArr.Set(4)
	inputArr.Set(5)

	tests := []struct {
		name     string
		childIdx int
		want     int
	}{
		{"0", 0, 0},
		{"1", 1, 1},
		{"2", 2, 2},
		{"3", 3, 6},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := calculateOriginalIndex(tt.childIdx, inputArr); got != tt.want {
				t.Errorf("calculateOriginalIndex() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCheckSubTasksState(t *testing.T) {
	ctx := context.Background()
	tID := &mocks.TaskExecutionID{}
	tID.OnGetGeneratedName().Return("generated-name")

	tMeta := &mocks.TaskExecutionMetadata{}
	tMeta.OnGetOwnerID().Return(types.NamespacedName{
		Namespace: "domain",
		Name:      "name",
	})
	tMeta.OnGetTaskExecutionID().Return(tID)

	t.Run("Not in cache", func(t *testing.T) {
		mBatchClient := batchMocks.NewMockAwsBatchClient()
		batchClient := NewCustomBatchClient(mBatchClient, "", "",
			utils.NewRateLimiter("", 10, 20),
			utils.NewRateLimiter("", 10, 20))

		jobStore := newJobsStore(t, batchClient)
		newState, err := CheckSubTasksState(ctx, tMeta, jobStore, &config.Config{}, &State{
			State: &arrayCore.StateImpl{
				CurrentPhase:         arrayCore.PhaseCheckingSubTaskExecutions,
				ExecutionArraySize:   5,
				OriginalArraySize:    10,
				OriginalMinSuccesses: 5,
			},
			ExternalJobID:    refStr("job-id"),
			JobDefinitionArn: "",
		})

		assert.NoError(t, err)
		p, _ := newState.GetPhase()
		assert.Equal(t, arrayCore.PhaseCheckingSubTaskExecutions.String(), p.String())
	})

	t.Run("Succeeded", func(t *testing.T) {
		mBatchClient := batchMocks.NewMockAwsBatchClient()
		mBatchClient.DescribeJobsWithContextCb =
			func(ctx context.Context, input *batch.DescribeJobsInput, opts ...request.Option) (
				output *batch.DescribeJobsOutput, e error) {
				return &batch.DescribeJobsOutput{
					Jobs: []*batch.JobDetail{
						{
							JobId:   refStr("job-id"),
							JobName: refStr(tID.GetGeneratedName()),
							Status:  refStr(batch.JobStatusSucceeded),
						},
					},
				}, nil
			}

		batchClient := NewCustomBatchClient(mBatchClient, "", "",
			utils.NewRateLimiter("", 10, 20),
			utils.NewRateLimiter("", 10, 20))

		jobStore := newJobsStore(t, batchClient)
		_, err := jobStore.GetOrCreate(tID.GetGeneratedName(), &Job{
			ID: "job-id",
			Status: JobStatus{
				Phase: core.PhaseSuccess,
			},
		})

		assert.NoError(t, err)

		newState, err := CheckSubTasksState(ctx, tMeta, jobStore, &config.Config{}, &State{
			State: &arrayCore.StateImpl{
				CurrentPhase:         arrayCore.PhaseCheckingSubTaskExecutions,
				ExecutionArraySize:   5,
				OriginalArraySize:    10,
				OriginalMinSuccesses: 5,
			},
			ExternalJobID:    refStr("job-id"),
			JobDefinitionArn: "",
		})

		assert.NoError(t, err)
		p, _ := newState.GetPhase()
		assert.Equal(t, arrayCore.PhaseAssembleFinalOutput.String(), p.String())
	})

	t.Run("Still running", func(t *testing.T) {
		mBatchClient := batchMocks.NewMockAwsBatchClient()
		batchClient := NewCustomBatchClient(mBatchClient, "", "",
			utils.NewRateLimiter("", 10, 20),
			utils.NewRateLimiter("", 10, 20))

		jobStore := newJobsStore(t, batchClient)
		_, err := jobStore.GetOrCreate(tID.GetGeneratedName(), &Job{
			ID: "job-id",
			Status: JobStatus{
				Phase: core.PhaseRunning,
			},
			SubJobs: []*Job{
				{Status: JobStatus{Phase: core.PhaseRunning}},
				{Status: JobStatus{Phase: core.PhaseSuccess}},
			},
		})

		assert.NoError(t, err)

		newState, err := CheckSubTasksState(ctx, tMeta, jobStore, &config.Config{}, &State{
			State: &arrayCore.StateImpl{
				CurrentPhase:         arrayCore.PhaseCheckingSubTaskExecutions,
				ExecutionArraySize:   2,
				OriginalArraySize:    2,
				OriginalMinSuccesses: 2,
				ArrayStatus: arraystatus.ArrayStatus{
					Detailed: arrayCore.NewPhasesCompactArray(2),
				},
				IndexesToCache: bitarray.NewBitSet(2),
			},
			ExternalJobID:    refStr("job-id"),
			JobDefinitionArn: "",
		})

		assert.NoError(t, err)
		p, _ := newState.GetPhase()
		assert.Equal(t, arrayCore.PhaseCheckingSubTaskExecutions.String(), p.String())

	})
}
