/*
 * Copyright (c) 2018 Lyft. All rights reserved.
 */

package array

import (
	"context"
	"testing"

	types2 "k8s.io/apimachinery/pkg/types"

	mocks2 "github.com/lyft/flyteplugins/go/tasks/v1/types/mocks"

	"sigs.k8s.io/controller-runtime/pkg/cache/informertest"

	"github.com/lyft/flytedynamicjoboperator/internal/mocks"
	"github.com/lyft/flytedynamicjoboperator/pkg/apis/futures/v1alpha1/arraystatus"
	"github.com/lyft/flytedynamicjoboperator/pkg/internal"
	"github.com/lyft/flytedynamicjoboperator/pkg/internal/bitarray"
	"github.com/lyft/flyteidl/clients/go/coreutils"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/event"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"
	"github.com/lyft/flyteplugins/go/tasks/flytek8s"
	"github.com/lyft/flyteplugins/go/tasks/v1/types"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/storage"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func mustCreateInMemoryStore(t *testing.T) *storage.DataStore {
	dataStore, err := storage.NewDataStore(&storage.Config{Type: storage.TypeMemory}, promutils.NewScope("test_"+rand.String(5)))
	assert.NoError(t, err)

	return dataStore
}

type MockEventRecorder struct{}

func (MockEventRecorder) RecordTaskEvent(ctx context.Context, event *event.TaskExecutionEvent) error {
	return nil
}

func init() {
	flytek8s.InitializeFake()
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
				Image:  "img",
				Config: []*core.KeyValuePair{},
				Resources: &core.Resources{
					Requests: []*core.Resources_ResourceEntry{
						{
							Name:  core.Resources_MEMORY,
							Value: "100M",
						},
					},
					Limits: []*core.Resources_ResourceEntry{},
				},
			},
		},
	}
}

func newMockTaskContext(phase types.TaskPhase, state types.CustomState) types.TaskContext {
	taskCtx := &mocks2.TaskContext{}
	taskCtx.On("GetNamespace").Return("fake-namespace")
	taskCtx.On("GetAnnotations").Return(map[string]string{"aKey": "aVal"})
	taskCtx.On("GetLabels").Return(map[string]string{"lKey": "lVal"})
	taskCtx.On("GetOwnerReference").Return(v1.OwnerReference{Name: "x"})
	taskCtx.On("GetOutputsFile").Return(storage.DataReference("outputs"))
	taskCtx.On("GetInputsFile").Return(storage.DataReference("inputs"))
	taskCtx.On("GetErrorFile").Return(storage.DataReference("error"))
	taskCtx.On("GetDataDir").Return(storage.DataReference("/node_id/"))
	taskCtx.On("GetOwnerID").Return(types2.NamespacedName{Name: "owner_id"})
	taskCtx.On("GetPhase").Return(phase)
	taskCtx.On("GetCustomState").Return(state)
	taskCtx.On("GetPhaseVersion").Return(uint32(1))
	taskCtx.On("GetK8sServiceAccount").Return("")

	res := &mocks2.TaskOverrides{}
	res.On("GetConfig").Return(nil)
	res.On("GetResources").Return(&corev1.ResourceRequirements{})
	taskCtx.On("GetOverrides").Return(res)

	id := &mocks2.TaskExecutionID{}
	id.On("GetGeneratedName").Return("task_test_id")
	id.On("GetID").Return(core.TaskExecutionIdentifier{
		NodeExecutionId: &core.NodeExecutionIdentifier{
			ExecutionId: &core.WorkflowExecutionIdentifier{},
		},
		TaskId: &core.Identifier{},
	})
	taskCtx.On("GetTaskExecutionID").Return(id)
	return taskCtx
}

func TestExecutor_StartTask(t *testing.T) {
	e := newMockExecutor(t, nil, nil)

	ctx := context.Background()
	status, err := StartTask(ctx, newMockTaskContext(types.TaskPhaseUnknown, nil), newTaskTemplate(), nil)

	assert.NoError(t, err)
	assert.NotNil(t, status.State)
	assert.Equal(t, types.TaskPhaseRunning, status.Phase)

	customState := arraystatus.CustomState{}
	err = customState.MergeFrom(ctx, status.State)
	assert.NoError(t, err)

	// attempt to start again, should not result in new objects.
	status, err = StartTask(ctx, newMockTaskContext(types.TaskPhaseUnknown, nil), newTaskTemplate(), nil)

	assert.NoError(t, err)
	assert.NotNil(t, status.State)
}

func newMockExecutor(t *testing.T, runtimeClient client.Client, memStore *storage.DataStore) Executor {
	if memStore == nil {
		memStore = mustCreateInMemoryStore(t)
	}

	e := Executor{
		dataStore:     memStore,
		eventRecorder: MockEventRecorder{},
	}

	if runtimeClient == nil {
		assert.NoError(t, InjectClient(mocks.NewMockRuntimeClient()))
	} else {
		assert.NoError(t, InjectClient(runtimeClient))
	}

	assert.NoError(t, InjectCache(&informertest.FakeInformers{}))

	assert.NoError(t, Initialize(context.TODO(), types.ExecutorInitializationParameters{
		EventRecorder: MockEventRecorder{},
		DataStore:     dataStore,
		OwnerKind:     "testKind",
	}))

	return e
}

func TestExecutor_CheckTaskStatus(t *testing.T) {
	t.Run("Happy path", func(t *testing.T) {
		runtimeClient := mocks.NewMockRuntimeClient()
		memStore := mustCreateInMemoryStore(t)
		e := newMockExecutor(t, runtimeClient, memStore)
		ctx := context.Background()

		status, err := StartTask(ctx, newMockTaskContext(types.TaskPhaseUnknown, nil), newTaskTemplate(), nil)
		assert.NoError(t, err)

		status, err = CheckTaskStatus(ctx, newMockTaskContext(status.Phase, status.State), newTaskTemplate())

		assert.NoError(t, err)
		assert.NotNil(t, status.State)

		status, err = CheckTaskStatus(ctx, newMockTaskContext(status.Phase, status.State), newTaskTemplate())

		assert.NoError(t, err)
		assert.NotNil(t, status.State)

		// Simulate progress... all pods succeeded
		items := corev1.PodList{}
		assert.NoError(t, runtimeClient.List(ctx, &client.ListOptions{}, &items))
		for _, p := range items.Items {
			p.Status.Phase = corev1.PodSucceeded
			assert.NoError(t, runtimeClient.Update(ctx, &p))
		}

		// Write Outputs files
		assert.NoError(t, memStore.WriteProtobuf(ctx, "/node_id/0/outputs.pb", storage.Options{}, &core.LiteralMap{
			Literals: map[string]*core.Literal{
				"x": {},
			},
		}))

		assert.NoError(t, memStore.WriteProtobuf(ctx, "/node_id/1/error.pb", storage.Options{}, &core.ErrorDocument{
			Error: &core.ContainerError{
				Message: "Just thought about failing",
			},
		}))

		status, err = CheckTaskStatus(ctx, newMockTaskContext(status.Phase, status.State), newTaskTemplate())

		assert.NoError(t, err)
		assert.NotNil(t, status.State)
		t.Log(status.State)
	})

	t.Run("Recover after crash", func(t *testing.T) {
		e := newMockExecutor(t, nil, nil)
		ctx := context.Background()

		status, err := StartTask(ctx, newMockTaskContext(types.TaskPhaseUnknown, nil), newTaskTemplate(), nil)
		assert.NoError(t, err)

		// Simulate crash
		e = newMockExecutor(t, nil, nil)
		assert.NoError(t, err)

		status, err = CheckTaskStatus(ctx, newMockTaskContext(status.Phase, status.State), newTaskTemplate())

		assert.NoError(t, err)
		assert.NotNil(t, status.State)

	})
}

func TestExecutor_KillTask(t *testing.T) {
	e := newMockExecutor(t, nil, nil)
	ctx := context.Background()
	_, err := StartTask(ctx, newMockTaskContext(types.TaskPhaseUnknown, nil), newTaskTemplate(), nil)
	assert.NoError(t, err)
	assert.NoError(t, KillTask(ctx, newMockTaskContext(types.TaskPhaseUnknown, nil), "test kill"))
}

func TestExecutor_Initialize(t *testing.T) {
	e := newMockExecutor(t, nil, nil)
	assert.NoError(t, Initialize(context.TODO(), types.ExecutorInitializationParameters{
		EventRecorder: MockEventRecorder{},
		DataStore:     dataStore,
		OwnerKind:     "testKind",
	}))

	assert.NoError(t, LoadPlugin(context.TODO()))
	GetID()
	GetProperties()
}

func TestExecutor_ResolveOutputs(t *testing.T) {
	ctx := context.Background()
	memStore := mustCreateInMemoryStore(t)
	e := newMockExecutor(t, nil, memStore)
	// Write Outputs files
	assert.NoError(t, memStore.WriteProtobuf(ctx, "/node_id/outputs.pb", storage.Options{},
		coreutils.MustMakeLiteral(map[string]interface{}{
			"x": 2,
		}).GetMap()))

	assert.NoError(t, memStore.WriteProtobuf(ctx, "/node_id/1/outputs.pb", storage.Options{},
		coreutils.MustMakeLiteral(map[string]interface{}{
			"x": 3,
		}).GetMap()))

	arrayStatus := arraystatus.CustomState{
		ArrayStatus: &arraystatus.ArrayStatus{
			Detailed: newStatusCompactArray(uint(2)),
		},
	}
	arrayStatus.ArrayStatus.Detailed.SetItem(1, bitarray.Item(PhaseManager.GetPhaseIndex("Succeeded")))
	customState, err := arrayStatus.AsMap(ctx)
	assert.NoError(t, err)

	m, err := ResolveOutputs(ctx, newMockTaskContext(types.TaskPhaseUnknown, customState), "x", "[1].x")
	assert.NoError(t, err)
	assert.Equal(t, coreutils.MustMakeLiteral(map[string]interface{}{
		"x":     2,
		"[1].x": 3,
	}).GetMap().Literals, m)
}
