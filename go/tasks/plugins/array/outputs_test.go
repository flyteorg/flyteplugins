package array

import (
	"context"
	"reflect"
	"testing"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	pluginCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/workqueue"
	"github.com/lyft/flytestdlib/promutils"
)

func TestOutputAssembler_Queue(t *testing.T) {
	type fields struct {
		IndexedWorkQueue workqueue.IndexedWorkQueue
	}
	type args struct {
		id   workqueue.WorkItemID
		item *outputAssembleItem
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := OutputAssembler{
				IndexedWorkQueue: tt.fields.IndexedWorkQueue,
			}
			if err := o.Queue(tt.args.id, tt.args.item); (err != nil) != tt.wantErr {
				t.Errorf("OutputAssembler.Queue() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_assembleOutputsWorker_Process(t *testing.T) {
	type args struct {
		ctx      context.Context
		workItem workqueue.WorkItem
	}
	tests := []struct {
		name    string
		w       assembleOutputsWorker
		args    args
		want    workqueue.WorkStatus
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := assembleOutputsWorker{}
			got, err := w.Process(tt.args.ctx, tt.args.workItem)
			if (err != nil) != tt.wantErr {
				t.Errorf("assembleOutputsWorker.Process() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("assembleOutputsWorker.Process() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_appendSubTaskOutput(t *testing.T) {
	type args struct {
		outputs       map[string]interface{}
		subTaskOutput *core.LiteralMap
		expectedSize  int64
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			appendSubTaskOutput(tt.args.outputs, tt.args.subTaskOutput, tt.args.expectedSize)
		})
	}
}

func Test_appendEmptyOutputs(t *testing.T) {
	type args struct {
		vars    []string
		outputs map[string]interface{}
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			appendEmptyOutputs(tt.args.vars, tt.args.outputs)
		})
	}
}

func TestAssembleFinalOutputs(t *testing.T) {
	type args struct {
		ctx           context.Context
		assemblyQueue OutputAssembler
		tCtx          pluginCore.TaskExecutionContext
		state         State
	}
	tests := []struct {
		name    string
		args    args
		want    State
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := AssembleFinalOutputs(tt.args.ctx, tt.args.assemblyQueue, tt.args.tCtx, tt.args.state)
			if (err != nil) != tt.wantErr {
				t.Errorf("AssembleFinalOutputs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AssembleFinalOutputs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_assembleErrorsWorker_Process(t *testing.T) {
	type fields struct {
		maxErrorMessageLength int
	}
	type args struct {
		ctx      context.Context
		workItem workqueue.WorkItem
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    workqueue.WorkStatus
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assembleErrorsWorker{
				maxErrorMessageLength: tt.fields.maxErrorMessageLength,
			}
			got, err := a.Process(tt.args.ctx, tt.args.workItem)
			if (err != nil) != tt.wantErr {
				t.Errorf("assembleErrorsWorker.Process() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("assembleErrorsWorker.Process() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewOutputAssembler(t *testing.T) {
	type args struct {
		workQueueConfig workqueue.Config
		scope           promutils.Scope
	}
	tests := []struct {
		name    string
		args    args
		want    OutputAssembler
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewOutputAssembler(tt.args.workQueueConfig, tt.args.scope)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewOutputAssembler() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewOutputAssembler() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewErrorAssembler(t *testing.T) {
	type args struct {
		maxErrorMessageLength int
		workQueueConfig       workqueue.Config
		scope                 promutils.Scope
	}
	tests := []struct {
		name    string
		args    args
		want    OutputAssembler
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewErrorAssembler(tt.args.maxErrorMessageLength, tt.args.workQueueConfig, tt.args.scope)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewErrorAssembler() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewErrorAssembler() = %v, want %v", got, tt.want)
			}
		})
	}
}
