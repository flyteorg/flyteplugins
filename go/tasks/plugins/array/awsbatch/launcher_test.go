package awsbatch

import (
	"testing"

	mocks3 "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io/mocks"

	v1 "k8s.io/api/core/v1"

	core2 "github.com/lyft/flyteplugins/go/tasks/plugins/array/core"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/stretchr/testify/assert"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	"github.com/lyft/flyteplugins/go/tasks/plugins/array/awsbatch/config"
	mocks2 "github.com/lyft/flyteplugins/go/tasks/plugins/array/awsbatch/mocks"
	"github.com/lyft/flytestdlib/utils"
	"golang.org/x/net/context"
)

func TestLaunchSubTasks(t *testing.T) {
	ctx := context.Background()
	tr := &mocks.TaskReader{}
	tr.OnRead(ctx).Return(&core.TaskTemplate{
		Target: &core.TaskTemplate_Container{
			Container: createSampleContainerTask(),
		},
	}, nil)

	tID := &mocks.TaskExecutionID{}
	tID.OnGetGeneratedName().Return("notfound")
	tID.OnGetID().Return(core.TaskExecutionIdentifier{
		TaskId: &core.Identifier{
			ResourceType: core.ResourceType_TASK,
			Project:      "a",
			Domain:       "d",
			Name:         "n",
			Version:      "abc",
		},
		NodeExecutionId: &core.NodeExecutionIdentifier{
			NodeId: "node1",
			ExecutionId: &core.WorkflowExecutionIdentifier{
				Project: "a",
				Domain:  "d",
				Name:    "exec",
			},
		},
		RetryAttempt: 0,
	})

	overrides := &mocks.TaskOverrides{}
	overrides.OnGetConfig().Return(&v1.ConfigMap{Data: map[string]string{
		DynamicTaskQueueKey: "queue1",
	}})

	tMeta := &mocks.TaskExecutionMetadata{}
	tMeta.OnGetTaskExecutionID().Return(tID)
	tMeta.OnGetOverrides().Return(overrides)

	ow := &mocks3.OutputWriter{}
	ow.OnGetOutputPrefixPath().Return("/prefix/")

	ir := &mocks3.InputReader{}
	ir.OnGetInputPrefixPath().Return("/prefix/")
	ir.OnGetInputPath().Return("/prefix/inputs.pb")

	tCtx := &mocks.TaskExecutionContext{}
	tCtx.OnTaskReader().Return(tr)
	tCtx.OnTaskExecutionMetadata().Return(tMeta)
	tCtx.OnOutputWriter().Return(ow)
	tCtx.OnInputReader().Return(ir)

	batchClient := NewCustomBatchClient(mocks2.NewMockAwsBatchClient(), "", "",
		utils.NewRateLimiter("", 10, 20),
		utils.NewRateLimiter("", 10, 20))

	t.Run("Simple", func(t *testing.T) {
		currentState := &State{
			State: &core2.State{
				CurrentPhase:         core2.PhaseLaunch,
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
		assert.Equal(t, core2.PhaseCheckingSubTaskExecutions, newPhase)
	})
}
