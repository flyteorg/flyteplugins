/*
 * Copyright (c) 2018 Lyft. All rights reserved.
 */

package awsbatch

import (
	"context"
	"testing"
	"time"

	"github.com/lyft/flytedynamicjoboperator/pkg/apis/futures/v1alpha1/arraystatus"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types2 "k8s.io/apimachinery/pkg/types"

	mocks2 "github.com/lyft/flyteplugins/go/tasks/v1/types/mocks"
	v12 "k8s.io/api/core/v1"

	"k8s.io/apimachinery/pkg/util/rand"

	"github.com/lyft/flytedynamicjoboperator/internal/mocks"
	"github.com/lyft/flytedynamicjoboperator/pkg/aws/batch/definition"
	"github.com/lyft/flytedynamicjoboperator/pkg/internal"
	"github.com/lyft/flytedynamicjoboperator/pkg/resource/flowcontrol"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/event"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"
	"github.com/lyft/flyteplugins/go/tasks/v1/types"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/storage"
	"github.com/stretchr/testify/assert"
)

type MockEventRecorder struct{}

func (MockEventRecorder) RecordTaskEvent(ctx context.Context, event *event.TaskExecutionEvent) error {
	return nil
}

func newMockExecutor() (Executor, error) {
	dataStore, err := storage.NewDataStore(&storage.Config{Type: storage.TypeMemory}, promutils.NewScope("test_"+rand.String(5)))
	if err != nil {
		return Executor{}, err
	}

	e := Executor{
		definitionCache: definition.NewCache(),
		recorder:        MockEventRecorder{},
		dataStore:       dataStore,
		ownerKind:       "testKind",
	}

	ctx := context.Background()

	jobsStore := NewInMemoryStore(10000, promutils.NewTestScope())
	batchClient := NewCustomBatchClient(mocks.NewMockAwsBatchClient(),
		jobsStore,
		flowcontrol.NewRateLimiter("Get", 15, 20),
		flowcontrol.NewRateLimiter("Default", 15, 20))

	client = batchClient
	jobsStore = jobsStore

	logger.Infof(ctx, "Watching Batch Jobs with SyncPeriod: %v", time.Microsecond)
	runWatcher(ctx, func(ownerId types2.NamespacedName) error { return nil }, time.Microsecond)
	return e, nil
}

func newTaskTemplate() *core.TaskTemplate {
	st, err := internal.ArrayJobToStruct(&plugins.ArrayJob{
		Size:         2,
		MinSuccesses: 2,
	})

	if err != nil {
		return nil
	}

	return &core.TaskTemplate{
		Type:   arrayTaskType,
		Custom: st,
		Target: &core.TaskTemplate_Container{
			Container: &core.Container{
				Image: "img",
				Config: []*core.KeyValuePair{
					{
						Key:   ChildTaskQueueKey,
						Value: "queue_test",
					},
				},
			},
		},
	}
}

func newMockTaskContext(phase types.TaskPhase, state types.CustomState) types.TaskContext {
	taskCtx := &mocks2.TaskContext{}
	taskCtx.On("GetNamespace").Return("ns")
	taskCtx.On("GetAnnotations").Return(map[string]string{"aKey": "aVal"})
	taskCtx.On("GetLabels").Return(map[string]string{"lKey": "lVal"})
	taskCtx.On("GetOwnerReference").Return(v1.OwnerReference{Name: "x"})
	taskCtx.On("GetOutputsFile").Return(storage.DataReference("outputs"))
	taskCtx.On("GetInputsFile").Return(storage.DataReference("inputs"))
	taskCtx.On("GetErrorFile").Return(storage.DataReference("error"))
	taskCtx.On("GetDataDir").Return(storage.DataReference("/node_id/"))
	taskCtx.On("GetOwnerID").Return(types2.NamespacedName{Name: "owner_id"})
	taskCtx.On("GetPhase").Return(phase)
	taskCtx.On("GetPhaseVersion").Return(uint32(1))
	taskCtx.On("GetCustomState").Return(state)

	to := &mocks2.TaskOverrides{}
	to.On("GetConfig").Return(&v12.ConfigMap{})
	taskCtx.On("GetOverrides").Return(to)

	id := &mocks2.TaskExecutionID{}
	id.On("GetGeneratedName").Return("task_test_id")
	id.On("GetID").Return(core.TaskExecutionIdentifier{})
	taskCtx.On("GetTaskExecutionID").Return(id)
	return taskCtx
}

func TestCustomState(t *testing.T) {
	s := arraystatus.CustomState{
		ArrayStatus: &arraystatus.ArrayStatus{
			Summary:  arraystatus.ArraySummary{},
			Detailed: newStatusCompactArray(uint(105)),
		},
	}

	for idx := range s.ArrayStatus.Detailed.GetItems() {
		s.ArrayStatus.Detailed.SetItem(idx, uint64(4))
	}

	marshaled, err := s.AsMap(context.TODO())
	assert.NoError(t, err)

	actual := arraystatus.CustomState{}
	assert.NoError(t, actual.MergeFrom(context.TODO(), marshaled))

	for idx, phase := range actual.ArrayStatus.Detailed.GetItems() {
		t.Logf("Idx: %v, Phase: %v", idx, phase)
	}
}

func TestExecutor_StartTask(t *testing.T) {
	e, err := newMockExecutor()
	assert.NoError(t, err)
	ctx := context.Background()

	status, err := StartTask(ctx, newMockTaskContext(types.TaskPhaseUnknown, nil), newTaskTemplate(), nil)

	assert.NoError(t, err)
	assert.Equal(t, types.TaskPhaseQueued, status.Phase)
	assert.NotNil(t, status.State)
}

func TestExecutor_CheckTaskStatus(t *testing.T) {
	t.Run("Happy path", func(t *testing.T) {
		e, err := newMockExecutor()
		assert.NoError(t, err)
		ctx := context.Background()

		status, err := StartTask(ctx, newMockTaskContext(types.TaskPhaseUnknown, nil), newTaskTemplate(), nil)
		assert.NoError(t, err)

		finalStatus, err := CheckTaskStatus(ctx, newMockTaskContext(status.Phase, status.State), newTaskTemplate())

		assert.NoError(t, err)
		assert.NotNil(t, finalStatus.State)
		assert.Equal(t, types.TaskPhaseQueued, finalStatus.Phase)
	})

	t.Run("Recover after crash", func(t *testing.T) {
		e, err := newMockExecutor()
		assert.NoError(t, err)
		ctx := context.Background()

		taskTmpl := newTaskTemplate()
		status, err := StartTask(ctx, newMockTaskContext(types.TaskPhaseUnknown, nil), taskTmpl, nil)
		assert.NoError(t, err)
		assert.Equal(t, types.TaskPhaseQueued, status.Phase)

		// Simulate crash
		e, err = newMockExecutor()
		assert.NoError(t, err)

		status, err = CheckTaskStatus(ctx, newMockTaskContext(status.Phase, status.State), taskTmpl)

		assert.NoError(t, err)
		assert.NotNil(t, status.State)

	})
}

func TestExecutor_KillTask(t *testing.T) {
	e, err := newMockExecutor()
	assert.NoError(t, err)
	ctx := context.Background()
	s, err := StartTask(ctx, newMockTaskContext(types.TaskPhaseUnknown, nil), newTaskTemplate(), nil)
	assert.NoError(t, err)
	assert.NoError(t, KillTask(ctx, newMockTaskContext(s.Phase, s.State), "test kill"))
}

func TestExecutor_doSync(t *testing.T) {
	e, err := newMockExecutor()
	assert.NoError(t, err)
	ctx := context.Background()
	assert.NoError(t, KillTask(ctx, newMockTaskContext(types.TaskPhaseUnknown, nil), "test kill"))
	_, err = doSync(ctx)
	assert.NoError(t, err)
}

func TestExecutor_Initialize(t *testing.T) {
	e, err := newMockExecutor()
	assert.NoError(t, err)
	assert.NoError(t, Initialize(context.TODO(), types.ExecutorInitializationParameters{
		EventRecorder: MockEventRecorder{},
		DataStore:     dataStore,
		OwnerKind:     "testKind",
		MetricsScope:  promutils.NewTestScope(),
	}))
}
