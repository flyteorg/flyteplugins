package array

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/lyft/flyteplugins/go/tasks/plugins/array/arraystatus"

	arrayCore "github.com/lyft/flyteplugins/go/tasks/plugins/array/core"

	mocks3 "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core/mocks"

	"github.com/lyft/flytestdlib/contextutils"
	"github.com/lyft/flytestdlib/promutils/labeled"

	"github.com/lyft/flyteidl/clients/go/coreutils"

	"github.com/lyft/flytestdlib/bitarray"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
	mocks2 "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io/mocks"

	"github.com/lyft/flytestdlib/storage"
	"github.com/stretchr/testify/assert"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/workqueue/mocks"
	"github.com/stretchr/testify/mock"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	pluginCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/workqueue"
	"github.com/lyft/flytestdlib/promutils"
)

func TestOutputAssembler_Queue(t *testing.T) {
	type args struct {
		id   workqueue.WorkItemID
		item *outputAssembleItem
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"nil item", args{"id", nil}, false},
		{"valid", args{"id", &outputAssembleItem{}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &mocks.IndexedWorkQueue{}
			q.OnQueue(mock.Anything, mock.Anything).Return(nil).Once()

			o := OutputAssembler{
				IndexedWorkQueue: q,
			}
			if err := o.Queue(tt.args.id, tt.args.item); (err != nil) != tt.wantErr {
				t.Errorf("OutputAssembler.Queue() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func init() {
	labeled.SetMetricKeys(contextutils.NamespaceKey)
}

func Test_assembleOutputsWorker_Process(t *testing.T) {
	ctx := context.Background()

	memStore, err := storage.NewDataStore(&storage.Config{
		Type: storage.TypeMemory,
	}, promutils.NewTestScope())
	assert.NoError(t, err)

	// Write data to 1st and 3rd tasks only. Simulate a failed 2nd and 4th tasks.
	l := coreutils.MustMakeLiteral(map[string]interface{}{
		"var1": 5,
		"var2": "hello world",
	})
	assert.NoError(t, memStore.WriteProtobuf(ctx, "/bucket/prefix/0/outputs.pb", storage.Options{}, l.GetMap()))
	assert.NoError(t, memStore.WriteProtobuf(ctx, "/bucket/prefix/2/outputs.pb", storage.Options{}, l.GetMap()))

	// Setup the expected data to be written to outputWriter.
	ow := &mocks2.OutputWriter{}
	ow.On("Put", mock.Anything, mock.Anything).Return(func(ctx context.Context, reader io.OutputReader) error {
		// Since 2nd and 4th tasks failed, there should be nil literals in their expected places.
		expected := coreutils.MustMakeLiteral(map[string]interface{}{
			"var1": []interface{}{5, nil, 5, nil},
			"var2": []interface{}{"hello world", nil, "hello world", nil},
		}).GetMap()

		final, ee, err := reader.Read(ctx)
		assert.NoError(t, err)
		assert.Nil(t, ee)

		assert.True(t, reflect.DeepEqual(final, expected))
		return nil
	}).Once()

	// Setup the input phases that inform outputs worker about which tasks failed/succeeded.
	phases := arrayCore.NewPhasesCompactArray(4)
	phases.SetItem(0, bitarray.Item(pluginCore.PhaseSuccess))
	phases.SetItem(1, bitarray.Item(pluginCore.PhasePermanentFailure))
	phases.SetItem(2, bitarray.Item(pluginCore.PhaseSuccess))
	phases.SetItem(3, bitarray.Item(pluginCore.PhasePermanentFailure))

	item := &outputAssembleItem{
		outputPrefix: "/bucket/prefix",
		varNames:     []string{"var1", "var2"},
		finalPhases:  phases,
		outputWriter: ow,
		dataStore:    memStore,
	}

	w := assembleOutputsWorker{}
	actual, err := w.Process(ctx, item)
	assert.NoError(t, err)
	assert.Equal(t, workqueue.WorkStatusSucceeded, actual)
}

func Test_appendSubTaskOutput(t *testing.T) {
	type args struct {
		outputs       map[string]interface{}
		subTaskOutput *core.LiteralMap
		expectedSize  int64
	}

	nativeMap := map[string]interface{}{
		"var1": 5,
		"var2": "hello",
	}
	validOutputs := coreutils.MustMakeLiteral(nativeMap).GetMap()

	t.Run("append to empty", func(t *testing.T) {
		expected := map[string]interface{}{
			"var1": []interface{}{coreutils.MustMakeLiteral(5)},
			"var2": []interface{}{coreutils.MustMakeLiteral("hello")},
		}

		actual := map[string]interface{}{}
		appendSubTaskOutput(actual, validOutputs, 1)
		assert.Equal(t, actual, expected)
	})

	t.Run("append to existing", func(t *testing.T) {
		expected := map[string]interface{}{
			"var1": []interface{}{coreutils.MustMakeLiteral(nil), coreutils.MustMakeLiteral(5)},
			"var2": []interface{}{coreutils.MustMakeLiteral(nil), coreutils.MustMakeLiteral("hello")},
		}

		actual := map[string]interface{}{
			"var1": []interface{}{coreutils.MustMakeLiteral(nil)},
			"var2": []interface{}{coreutils.MustMakeLiteral(nil)},
		}

		appendSubTaskOutput(actual, validOutputs, 1)
		assert.Equal(t, actual, expected)
	})
}

func TestAssembleFinalOutputs(t *testing.T) {
	ctx := context.Background()
	q := &mocks.IndexedWorkQueue{}
	q.On("Queue", mock.Anything, mock.Anything).Return(
		func(id workqueue.WorkItemID, workItem workqueue.WorkItem) error {
			return nil
		})
	assemblyQueue := OutputAssembler{
		IndexedWorkQueue: q,
	}

	t.Run("Found succeeded", func(t *testing.T) {
		info := &mocks.WorkItemInfo{}
		info.OnStatus().Return(workqueue.WorkStatusSucceeded)
		q.OnGet("found").Return(info, true, nil)

		s := &arrayCore.State{}

		tID := &mocks3.TaskExecutionID{}
		tID.OnGetGeneratedName().Return("found")

		tMeta := &mocks3.TaskExecutionMetadata{}
		tMeta.OnGetTaskExecutionID().Return(tID)

		tCtx := &mocks3.TaskExecutionContext{}
		tCtx.OnTaskExecutionMetadata().Return(tMeta)

		_, err := AssembleFinalOutputs(ctx, assemblyQueue, tCtx, s)
		assert.NoError(t, err)
		assert.Equal(t, arrayCore.PhaseSuccess, s.CurrentPhase)
	})

	t.Run("Found failed", func(t *testing.T) {
		info := &mocks.WorkItemInfo{}
		info.OnStatus().Return(workqueue.WorkStatusFailed)
		info.OnError().Return(fmt.Errorf("expected error"))

		q.OnGet("found_failed").Return(info, true, nil)

		s := &arrayCore.State{}

		tID := &mocks3.TaskExecutionID{}
		tID.OnGetGeneratedName().Return("found_failed")

		tMeta := &mocks3.TaskExecutionMetadata{}
		tMeta.OnGetTaskExecutionID().Return(tID)

		tCtx := &mocks3.TaskExecutionContext{}
		tCtx.OnTaskExecutionMetadata().Return(tMeta)

		_, err := AssembleFinalOutputs(ctx, assemblyQueue, tCtx, s)
		assert.NoError(t, err)
		assert.Equal(t, arrayCore.PhaseRetryableFailure, s.CurrentPhase)
	})

	t.Run("Not Found Queued then Succeeded", func(t *testing.T) {
		info := &mocks.WorkItemInfo{}
		info.OnStatus().Return(workqueue.WorkStatusSucceeded)
		q.OnGet("notfound").Return(nil, false, nil).Once()
		q.OnGet("notfound").Return(info, true, nil).Once()

		detailedStatus := arrayCore.NewPhasesCompactArray(2)
		detailedStatus.SetItem(0, bitarray.Item(pluginCore.PhaseSuccess))
		detailedStatus.SetItem(1, bitarray.Item(pluginCore.PhaseSuccess))

		s := &arrayCore.State{
			ArrayStatus: arraystatus.ArrayStatus{
				Detailed: detailedStatus,
			},
		}

		tID := &mocks3.TaskExecutionID{}
		tID.OnGetGeneratedName().Return("notfound")

		tMeta := &mocks3.TaskExecutionMetadata{}
		tMeta.OnGetTaskExecutionID().Return(tID)

		tReader := &mocks3.TaskReader{}
		tReader.OnReadMatch(mock.Anything).Return(&core.TaskTemplate{
			Interface: &core.TypedInterface{
				Outputs: &core.VariableMap{
					Variables: map[string]*core.Variable{"var1": {Type: &core.LiteralType{Type: &core.LiteralType_Simple{Simple: core.SimpleType_INTEGER}}}},
				},
			},
		}, nil)

		ow := &mocks2.OutputWriter{}
		ow.OnGetOutputPrefixPath().Return("/prefix/")
		ow.On("Put", mock.Anything, mock.Anything).Return(func(ctx context.Context, or io.OutputReader) {
			m, ee, err := or.Read(ctx)
			assert.NoError(t, err)
			assert.Nil(t, ee)
			assert.NotNil(t, m)
		})

		ds, err := storage.NewDataStore(&storage.Config{Type: storage.TypeMemory}, promutils.NewTestScope())
		assert.NoError(t, err)

		tCtx := &mocks3.TaskExecutionContext{}
		tCtx.OnTaskExecutionMetadata().Return(tMeta)
		tCtx.OnTaskReader().Return(tReader)
		tCtx.OnOutputWriter().Return(ow)
		tCtx.OnDataStore().Return(ds)

		_, err = AssembleFinalOutputs(ctx, assemblyQueue, tCtx, s)
		assert.NoError(t, err)
		assert.Equal(t, arrayCore.PhaseSuccess, s.CurrentPhase)
	})
}

func Test_assembleErrorsWorker_Process(t *testing.T) {
	ctx := context.Background()

	memStore, err := storage.NewDataStore(&storage.Config{
		Type: storage.TypeMemory,
	}, promutils.NewTestScope())
	assert.NoError(t, err)

	// Write data to 1st and 3rd tasks only. Simulate a failed 2nd and 4th tasks.
	l := coreutils.MustMakeLiteral(map[string]interface{}{
		"var1": 5,
		"var2": "hello world",
	})

	ee := &core.ErrorDocument{
		Error: &core.ContainerError{
			Message: "Expected error",
		},
	}

	assert.NoError(t, memStore.WriteProtobuf(ctx, "/bucket/prefix/0/outputs.pb", storage.Options{}, l.GetMap()))
	assert.NoError(t, memStore.WriteProtobuf(ctx, "/bucket/prefix/1/error.pb", storage.Options{}, ee))
	assert.NoError(t, memStore.WriteProtobuf(ctx, "/bucket/prefix/2/outputs.pb", storage.Options{}, l.GetMap()))
	assert.NoError(t, memStore.WriteProtobuf(ctx, "/bucket/prefix/3/error.pb", storage.Options{}, ee))

	// Setup the expected data to be written to outputWriter.
	ow := &mocks2.OutputWriter{}
	ow.On("Put", mock.Anything, mock.Anything).Return(func(ctx context.Context, reader io.OutputReader) error {
		// Since 2nd and 4th tasks failed, there should be nil literals in their expected places.

		final, ee, err := reader.Read(ctx)
		assert.NoError(t, err)
		assert.Nil(t, final)
		assert.NotNil(t, ee)
		assert.Equal(t, `[1][3]: message:"Expected error" 
`, ee.Message)

		return nil
	}).Once()

	// Setup the input phases that inform outputs worker about which tasks failed/succeeded.
	phases := arrayCore.NewPhasesCompactArray(4)
	phases.SetItem(0, bitarray.Item(pluginCore.PhaseSuccess))
	phases.SetItem(1, bitarray.Item(pluginCore.PhasePermanentFailure))
	phases.SetItem(2, bitarray.Item(pluginCore.PhaseSuccess))
	phases.SetItem(3, bitarray.Item(pluginCore.PhasePermanentFailure))

	item := &outputAssembleItem{
		outputPrefix: "/bucket/prefix",
		varNames:     []string{"var1", "var2"},
		finalPhases:  phases,
		outputWriter: ow,
		dataStore:    memStore,
	}

	w := assembleErrorsWorker{
		maxErrorMessageLength: 1000,
	}
	actual, err := w.Process(ctx, item)
	assert.NoError(t, err)
	assert.Equal(t, workqueue.WorkStatusSucceeded, actual)
}

func TestNewOutputAssembler(t *testing.T) {
	t.Run("Invalid Config", func(t *testing.T) {
		_, err := NewOutputAssembler(workqueue.Config{
			Workers: 1,
		}, promutils.NewTestScope())
		assert.Error(t, err)
	})

	t.Run("Valid Config", func(t *testing.T) {
		o, err := NewOutputAssembler(workqueue.Config{
			Workers:            1,
			IndexCacheMaxItems: 10,
		}, promutils.NewTestScope())
		assert.NoError(t, err)
		assert.NotNil(t, o)
	})
}

func TestNewErrorAssembler(t *testing.T) {
	t.Run("Invalid Config", func(t *testing.T) {
		_, err := NewErrorAssembler(0, workqueue.Config{
			Workers: 1,
		}, promutils.NewTestScope())
		assert.Error(t, err)
	})

	t.Run("Valid Config", func(t *testing.T) {
		o, err := NewErrorAssembler(0, workqueue.Config{
			Workers:            1,
			IndexCacheMaxItems: 10,
		}, promutils.NewTestScope())
		assert.NoError(t, err)
		assert.NotNil(t, o)
	})
}
