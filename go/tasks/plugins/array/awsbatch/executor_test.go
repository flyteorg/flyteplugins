package awsbatch

import (
	"context"
	"reflect"
	"testing"

	arrayCore "github.com/lyft/flyteplugins/go/tasks/plugins/array/core"

	pluginCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"

	pluginMocks "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	queueMocks "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/workqueue/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/lyft/flyteplugins/go/tasks/plugins/array"
	"github.com/lyft/flyteplugins/go/tasks/plugins/array/awsbatch/definition"
	"github.com/lyft/flyteplugins/go/tasks/plugins/array/awsbatch/mocks"
	cacheMocks "github.com/lyft/flytestdlib/cache/mocks"
	"github.com/lyft/flytestdlib/utils"
)

func TestExecutor_Handle(t *testing.T) {
	batchClient := NewCustomBatchClient(mocks.NewMockAwsBatchClient(), "", "",
		utils.NewRateLimiter("", 10, 20),
		utils.NewRateLimiter("", 10, 20))

	cache := &cacheMocks.AutoRefresh{}

	js := &JobStore{
		Client:      batchClient,
		AutoRefresh: cache,
		started:     true,
	}

	jc := definition.NewCache(10)

	q := &queueMocks.IndexedWorkQueue{}
	q.OnQueue(mock.Anything, mock.Anything).Return(nil).Once()

	oa := array.OutputAssembler{
		IndexedWorkQueue: q,
	}

	e := Executor{
		jobStore:           js,
		jobDefinitionCache: jc,
		outputAssembler:    oa,
		errorAssembler:     oa,
	}

	ctx := context.Background()

	tr := &pluginMocks.TaskReader{}
	tr.OnRead(ctx).Return(&core.TaskTemplate{
		Target: &core.TaskTemplate_Container{
			Container: createSampleContainerTask(),
		},
	}, nil)

	inputState := &State{}

	pluginStateReader := &pluginMocks.PluginStateReader{}
	pluginStateReader.On("Get", mock.AnythingOfType(reflect.TypeOf(&State{}).String())).Return(
		func(v interface{}) uint8 {
			*(v.(*State)) = *inputState
			return 0
		},
		func(v interface{}) error {
			return nil
		})

	pluginStateWriter := &pluginMocks.PluginStateWriter{}
	pluginStateWriter.On("Put", mock.Anything, mock.Anything).Return(
		func(stateVersion uint8, v interface{}) error {
			assert.Equal(t, uint8(0), stateVersion)
			actualState, casted := v.(*State)
			assert.True(t, casted)

			actualPhase, _ := actualState.GetPhase()
			assert.Equal(t, arrayCore.PhasePreLaunch, actualPhase)
			return nil
		})

	tCtx := &pluginMocks.TaskExecutionContext{}
	tCtx.OnPluginStateReader().Return(pluginStateReader)
	tCtx.OnPluginStateWriter().Return(pluginStateWriter)
	tCtx.OnTaskReader().Return(tr)

	transition, err := e.Handle(ctx, tCtx)
	assert.NoError(t, err)
	assert.NotNil(t, transition)
	assert.Equal(t, pluginCore.PhaseRunning.String(), transition.Info().Phase().String())
}
