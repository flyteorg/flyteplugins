package awsbatch

import (
	"testing"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/stretchr/testify/assert"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	"github.com/lyft/flyteplugins/go/tasks/plugins/array"
	"github.com/lyft/flyteplugins/go/tasks/plugins/array/awsbatch/config"
	mocks2 "github.com/lyft/flyteplugins/go/tasks/plugins/array/awsbatch/mocks"
	"github.com/lyft/flytestdlib/utils"
	"golang.org/x/net/context"
)

func TestLaunchSubTasks(t *testing.T) {
	ctx := context.Background()
	tCtx := &mocks.TaskExecutionContext{}
	tr := &mocks.TaskReader{}
	tr.OnRead(ctx).Return(&core.TaskTemplate{}, nil)
	tCtx.OnTaskReader().Return(tr)

	batchClient := NewCustomBatchClient(mocks2.NewMockAwsBatchClient(), "", "",
		utils.NewRateLimiter("", 10, 20),
		utils.NewRateLimiter("", 10, 20))

	t.Run("Simple", func(t *testing.T) {
		currentState := &State{
			State: &array.StateImpl{
				CurrentPhase:         array.PhaseLaunch,
				ExecutionArraySize:   5,
				OriginalArraySize:    10,
				OriginalMinSuccesses: 5,
			},
			ExternalJobID:    nil,
			JobDefinitionArn: "",
		}

		newState, err := LaunchSubTasks(context.TODO(), tCtx, batchClient, &config.Config{}, currentState)
		assert.NoError(t, err)
		newPhase, _ := newState.GetPhase()
		assert.Equal(t, array.PhaseCheckingSubTaskExecutions, newPhase)
	})
}
