package catalog

import (
	"context"
	"reflect"
	"testing"

	mocks2 "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io/mocks"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/workqueue/mocks"
	"github.com/lyft/flytestdlib/bitarray"
	"github.com/stretchr/testify/mock"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/workqueue"
)

func TestAsyncClientImpl_Download(t *testing.T) {
	ctx := context.Background()

	q := &mocks.IndexedWorkQueue{}
	info := &mocks.WorkItemInfo{}
	info.OnItem().Return(NewReaderWorkItem(Key{}, &mocks2.OutputWriter{}))
	info.OnStatus().Return(workqueue.WorkStatusSucceeded)
	q.OnGet("{UNSPECIFIED     {} [] 0}:-0").Return(info, true, nil)
	q.OnQueueMatch(mock.Anything, mock.Anything).Return(nil)

	tests := []struct {
		name             string
		reader           workqueue.IndexedWorkQueue
		requests         []DownloadRequest
		wantOutputFuture DownloadFuture
		wantErr          bool
	}{
		{"DownloadQueued", q, []DownloadRequest{{}}, newDownloadFuture(ResponseStatusReady, nil, bitarray.NewBitSet(1), 1, 0), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := AsyncClientImpl{
				Reader: tt.reader,
			}
			gotOutputFuture, err := c.Download(ctx, tt.requests...)
			if (err != nil) != tt.wantErr {
				t.Errorf("AsyncClientImpl.Download() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotOutputFuture, tt.wantOutputFuture) {
				t.Errorf("AsyncClientImpl.Download() = %v, want %v", gotOutputFuture, tt.wantOutputFuture)
			}
		})
	}
}

func TestAsyncClientImpl_Upload(t *testing.T) {
	type fields struct {
		Reader workqueue.IndexedWorkQueue
		Writer workqueue.IndexedWorkQueue
	}
	type args struct {
		ctx      context.Context
		requests []UploadRequest
	}
	tests := []struct {
		name          string
		fields        fields
		args          args
		wantPutFuture UploadFuture
		wantErr       bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := AsyncClientImpl{
				Reader: tt.fields.Reader,
				Writer: tt.fields.Writer,
			}
			gotPutFuture, err := c.Upload(tt.args.ctx, tt.args.requests...)
			if (err != nil) != tt.wantErr {
				t.Errorf("AsyncClientImpl.Upload() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotPutFuture, tt.wantPutFuture) {
				t.Errorf("AsyncClientImpl.Upload() = %v, want %v", gotPutFuture, tt.wantPutFuture)
			}
		})
	}
}

func TestAsyncClientImpl_Start(t *testing.T) {
	type fields struct {
		Reader workqueue.IndexedWorkQueue
		Writer workqueue.IndexedWorkQueue
	}
	type args struct {
		ctx context.Context
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
			c := AsyncClientImpl{
				Reader: tt.fields.Reader,
				Writer: tt.fields.Writer,
			}
			if err := c.Start(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("AsyncClientImpl.Start() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
