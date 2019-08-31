package qubole_single

import (
	"context"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/core/mocks"
	mocks2 "github.com/lyft/flyteplugins/go/tasks/v1/k8splugins/mocks"
	"github.com/lyft/flyteplugins/go/tasks/v1/qubole_single/client"
	quboleMocks "github.com/lyft/flyteplugins/go/tasks/v1/qubole_single/client/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"testing"
)

func TestInTerminalState(t *testing.T) {
	var stateTests = []struct {
		phase      ExecutionPhase
		isTerminal bool
	}{
		{phase: PhaseNotStarted, isTerminal: false},
		{phase: PhaseQueued, isTerminal: false},
		{phase: PhaseSubmitted, isTerminal: false},
		{phase: PhaseQuerySucceeded, isTerminal: true},
		{phase: PhaseQueryFailed, isTerminal: true},
	}

	for _, tt := range stateTests {
		t.Run(tt.phase.String(), func(t *testing.T) {
			e := ExecutionState{Phase: tt.phase}
			res := InTerminalState(e)
			assert.Equal(t, tt.isTerminal, res)
		})
	}
}

func TestIsNotYetSubmitted(t *testing.T) {
	var stateTests = []struct {
		phase             ExecutionPhase
		isNotYetSubmitted bool
	}{
		{phase: PhaseNotStarted, isNotYetSubmitted: true},
		{phase: PhaseQueued, isNotYetSubmitted: true},
		{phase: PhaseSubmitted, isNotYetSubmitted: false},
		{phase: PhaseQuerySucceeded, isNotYetSubmitted: false},
		{phase: PhaseQueryFailed, isNotYetSubmitted: false},
	}

	for _, tt := range stateTests {
		t.Run(tt.phase.String(), func(t *testing.T) {
			e := ExecutionState{Phase: tt.phase}
			res := IsNotYetSubmitted(e)
			assert.Equal(t, tt.isNotYetSubmitted, res)
		})
	}

}

func TestCopy(t *testing.T) {
	e0 := ExecutionState{
		Phase:     PhaseQueryFailed,
		CommandId: "234",
	}
	e1 := Copy(e0)
	e0.Phase = PhaseQuerySucceeded
	e0.CommandId = "123"
	assert.Equal(t, "123", e0.CommandId)
	assert.Equal(t, "234", e1.CommandId)
}

func TestGetQueryInfo(t *testing.T) {
	ctx := context.Background()

	taskTemplate := GetSingleHiveQueryTaskTemplate()
	mockTaskReader := &mocks.TaskReader{}
	mockTaskReader.On("Read", mock.Anything).Return(&taskTemplate, nil)

	mockTaskExecutionContext := mocks.TaskExecutionContext{}
	mockTaskExecutionContext.On("TaskReader").Return(mockTaskReader)

	query, cluster, tags, timeout, err := GetQueryInfo(ctx, &mockTaskExecutionContext)
	assert.NoError(t, err)
	assert.Equal(t, "select 'one'", query)
	assert.Equal(t, "default", cluster)
	assert.Equal(t, []string{"flyte_plugin_test"}, tags)
	assert.Equal(t, 500, int(timeout))
}

func TestConstructTaskLog(t *testing.T) {
	taskLog := ConstructTaskLog(ExecutionState{CommandId: "123"})
	assert.Equal(t, "https://api.qubole.com/v2/analyze?command_id=123", taskLog.Uri)
}

func TestConstructTaskInfo(t *testing.T) {
	empty := ConstructTaskInfo(ExecutionState{})
	assert.Nil(t, empty)

	e := ExecutionState{
		Phase:                 PhaseQuerySucceeded,
		CommandId:             "123",
		SyncQuboleApiFailures: 0,
		Id:                    "some_id",
	}
	taskInfo := ConstructTaskInfo(e)
	assert.Equal(t, "https://api.qubole.com/v2/analyze?command_id=123", taskInfo.Logs[0].Uri)
}

func TestMapExecutionStateToPhaseInfo(t *testing.T) {
	t.Run("NotStarted", func(t *testing.T) {
		e := ExecutionState{
			Id:    "test",
			Phase: PhaseNotStarted,
		}
		phaseInfo := MapExecutionStateToPhaseInfo(e)
		assert.Equal(t, core.PhaseNotReady, phaseInfo.Phase())
	})

	t.Run("Queued", func(t *testing.T) {
		e := ExecutionState{
			Id:                        "test",
			Phase:                     PhaseQueued,
			QuboleApiCreationFailures: 0,
		}
		phaseInfo := MapExecutionStateToPhaseInfo(e)
		assert.Equal(t, core.PhaseQueued, phaseInfo.Phase())

		e = ExecutionState{
			Id:                        "test",
			Phase:                     PhaseQueued,
			QuboleApiCreationFailures: 100,
		}
		phaseInfo = MapExecutionStateToPhaseInfo(e)
		assert.Equal(t, core.PhaseRetryableFailure, phaseInfo.Phase())

	})

	t.Run("Submitted", func(t *testing.T) {
		e := ExecutionState{
			Id:    "test",
			Phase: PhaseSubmitted,
		}
		phaseInfo := MapExecutionStateToPhaseInfo(e)
		assert.Equal(t, core.PhaseRunning, phaseInfo.Phase())
	})
}

func TestGetAllocationToken(t *testing.T) {
	ctx := context.Background()

	t.Run("allocation granted", func(t *testing.T) {
		tCtx := GetMockTaskExecutionContext()
		mockResourceManager := tCtx.ResourceManager()
		x := mockResourceManager.(*mocks.ResourceManager)
		x.On("AllocateResource", mock.Anything, mock.Anything, mock.Anything).
			Return(core.AllocationStatusGranted, nil)

		state, err := GetAllocationToken(ctx, tCtx)
		assert.NoError(t, err)
		assert.Equal(t, PhaseQueued, state.Phase)
	})

	t.Run("exhausted", func(t *testing.T) {
		tCtx := GetMockTaskExecutionContext()
		mockResourceManager := tCtx.ResourceManager()
		x := mockResourceManager.(*mocks.ResourceManager)
		x.On("AllocateResource", mock.Anything, mock.Anything, mock.Anything).
			Return(core.AllocationStatusExhausted, nil)

		state, err := GetAllocationToken(ctx, tCtx)
		assert.NoError(t, err)
		assert.Equal(t, PhaseNotStarted, state.Phase)
	})

	t.Run("namespace exhausted", func(t *testing.T) {
		tCtx := GetMockTaskExecutionContext()
		mockResourceManager := tCtx.ResourceManager()
		x := mockResourceManager.(*mocks.ResourceManager)
		x.On("AllocateResource", mock.Anything, mock.Anything, mock.Anything).
			Return(core.AllocationStatusNamespaceQuotaExceeded, nil)

		state, err := GetAllocationToken(ctx, tCtx)
		assert.NoError(t, err)
		assert.Equal(t, PhaseNotStarted, state.Phase)
	})
}

func TestAbort(t *testing.T) {
	ctx := context.Background()

	// TODO: This will need to be replaced with the mock when we switch to using the mock
	dummySecrets := &secretsManager{quboleKey: "fake key"}

	t.Run("Terminate called when not in terminal state", func(t *testing.T) {
		var x = false
		mockQubole := &quboleMocks.QuboleClient{}
		mockQubole.On("KillCommand", mock.Anything, mock.MatchedBy(func(commandId string) bool {
			return commandId == "123456"
		}), mock.Anything).Run(func(_ mock.Arguments) {
			x = true
		}).Return(nil)

		err := Abort(ctx, nil, ExecutionState{Phase: PhaseSubmitted, CommandId: "123456"}, mockQubole, dummySecrets)
		assert.NoError(t, err)
		assert.True(t, x)
	})

	t.Run("Terminate not called when in terminal state", func(t *testing.T) {
		var x = false
		mockQubole := &quboleMocks.QuboleClient{}
		mockQubole.On("KillCommand", mock.Anything, mock.Anything, mock.Anything).Run(func(_ mock.Arguments) {
			x = true
		}).Return(nil)

		err := Abort(ctx, nil, ExecutionState{Phase: PhaseQuerySucceeded, CommandId: "123456"}, mockQubole, dummySecrets)
		assert.NoError(t, err)
		assert.False(t, x)
	})
}

func TestFinalize(t *testing.T) {
	// Test that Finalize releases resources
	ctx := context.Background()
	tCtx := GetMockTaskExecutionContext()
	state := ExecutionState{
		Id: "fjfklasj",
	}
	var called = false
	mockResourceManager := tCtx.ResourceManager()
	x := mockResourceManager.(*mocks.ResourceManager)
	x.On("ReleaseResource", mock.Anything, mock.Anything, mock.MatchedBy(func(id string) bool {
		return id == state.Id
	})).Run(func(_ mock.Arguments) {
		called = true
	}).Return(nil)

	err := Finalize(ctx, tCtx, state)
	assert.NoError(t, err)
	assert.True(t, called)
}

func TestMonitorQuery(t *testing.T) {
	ctx := context.Background()
	tCtx := GetMockTaskExecutionContext()
	state := ExecutionState{
		Id:    "unique-id",
		Phase: PhaseSubmitted,
	}
	var getOrCreateCalled = false
	mockCache := &mocks2.AutoRefreshCache{}
	mockCache.On("GetOrCreate", mock.MatchedBy(func(s ExecutionState) bool {
		return s.Id == state.Id
	})).Run(func(_ mock.Arguments) {
		getOrCreateCalled = true
	}).Return(ExecutionState{Id: state.Id, Phase: PhaseQuerySucceeded}, nil)

	newState, err := MonitorQuery(ctx, tCtx, state, mockCache)
	assert.NoError(t, err)
	assert.True(t, getOrCreateCalled)
	assert.Equal(t, PhaseQuerySucceeded, newState.Phase)
}

func TestKickOffQuery(t *testing.T) {
	ctx := context.Background()
	tCtx := GetMockTaskExecutionContext()

	// TODO: This will need to be replaced with the mock when we switch to using the mock
	dummySecrets := &secretsManager{quboleKey: "fake key"}

	var quboleCalled = false
	quboleCommandDetails := &client.QuboleCommandDetails{
		ID:     int64(453298043),
		Status: client.QuboleStatusWaiting,
	}
	mockQubole := &quboleMocks.QuboleClient{}
	mockQubole.On("ExecuteHiveCommand", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).Run(func(_ mock.Arguments) {
		quboleCalled = true
	}).Return(quboleCommandDetails, nil)

	var getOrCreateCalled = false
	mockCache := &mocks2.AutoRefreshCache{}
	mockCache.On("GetOrCreate", mock.Anything).Run(func(_ mock.Arguments) {
		getOrCreateCalled = true
	}).Return(ExecutionState{}, nil)

	state := ExecutionState{}
	newState, err := KickOffQuery(ctx, tCtx, state, mockQubole, dummySecrets, mockCache)
	assert.NoError(t, err)
	assert.Equal(t, PhaseSubmitted, newState.Phase)
	assert.True(t, getOrCreateCalled)
	assert.True(t, quboleCalled)
}
