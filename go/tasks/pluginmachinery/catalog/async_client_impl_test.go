package catalog

import (
	"context"
	"testing"

	"github.com/go-test/deep"

	"github.com/lyft/flytestdlib/contextutils"
	"github.com/lyft/flytestdlib/promutils/labeled"

	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/storage"
	"github.com/stretchr/testify/assert"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"

	mocks2 "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io/mocks"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/workqueue/mocks"
	"github.com/lyft/flytestdlib/bitarray"
	"github.com/stretchr/testify/mock"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/workqueue"
)

func init() {
	labeled.SetMetricKeys(contextutils.NamespaceKey)
}

func invertBitSet(input *bitarray.BitSet, limit uint) *bitarray.BitSet {
	output := bitarray.NewBitSet(limit)
	for i := uint(0); i < limit; i++ {
		if !input.IsSet(i) {
			output.Set(i)
		}
	}

	return output
}

func TestAsyncClientImpl_Download(t *testing.T) {
	ctx := context.Background()

	q := &mocks.IndexedWorkQueue{}
	info := &mocks.WorkItemInfo{}
	info.OnItem().Return(NewArrayReaderWorkItem(DownloadArrayRequest{}))
	info.OnStatus().Return(workqueue.WorkStatusSucceeded)
	q.OnGetMatch(mock.Anything).Return(info, true, nil)
	q.OnQueueMatch(mock.Anything, mock.Anything, mock.Anything).Return(nil)

	ow := &mocks2.OutputWriter{}
	ow.OnGetOutputPrefixPath().Return("/prefix/")
	ow.OnGetOutputPath().Return("/prefix/outputs.pb")

	ir := &mocks2.InputReader{}
	ir.OnGetInputPrefixPath().Return("/prefix/")

	ds, err := storage.NewDataStore(&storage.Config{Type: storage.TypeMemory}, promutils.NewTestScope())
	assert.NoError(t, err)

	tests := []struct {
		name             string
		reader           workqueue.IndexedWorkQueue
		request          DownloadArrayRequest
		wantOutputFuture DownloadFuture
		wantErr          bool
	}{
		{"DownloadQueued", q, DownloadArrayRequest{
			Identifier:      core.Identifier{},
			CacheVersion:    "",
			TypedInterface:  core.TypedInterface{},
			dataStore:       ds,
			BaseInputReader: ir,
			BaseTarget:      ow,
			Indexes:         invertBitSet(bitarray.NewBitSet(1), 1),
			Count:           1,
		}, newDownloadFuture(ResponseStatusReady, nil, bitarray.NewBitSet(1), 1), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := AsyncClientImpl{
				ArrayReader: tt.reader,
			}

			gotOutputFuture, err := c.DownloadArray(ctx, tt.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("AsyncClientImpl.Download() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if diff := deep.Equal(tt.wantOutputFuture, gotOutputFuture); diff != nil {
				t.Errorf("expected != actual. Diff: %v", diff)
			}
		})
	}
}

func TestAsyncClientImpl_Upload(t *testing.T) {
	ctx := context.Background()

	q := &mocks.IndexedWorkQueue{}
	info := &mocks.WorkItemInfo{}
	info.OnItem().Return(NewArrayReaderWorkItem(DownloadArrayRequest{}))
	info.OnStatus().Return(workqueue.WorkStatusSucceeded)
	q.OnGet("cfqacua").Return(info, true, nil)
	q.OnQueueMatch(mock.Anything, mock.Anything, mock.Anything).Return(nil)

	ir := &mocks2.InputReader{}
	ir.OnGetInputPrefixPath().Return("/prefix/")

	ow := &mocks2.OutputWriter{}
	ow.OnGetOutputPrefixPath().Return("/prefix/")
	ow.OnGetOutputPath().Return("/prefix/outputs.pb")

	ds, err := storage.NewDataStore(&storage.Config{Type: storage.TypeMemory}, promutils.NewTestScope())
	assert.NoError(t, err)

	tests := []struct {
		name          string
		request       UploadArrayRequest
		wantPutFuture UploadFuture
		wantErr       bool
	}{
		{"UploadSucceeded", UploadArrayRequest{
			Identifier:       core.Identifier{},
			CacheVersion:     "",
			TypedInterface:   core.TypedInterface{},
			ArtifactMetadata: Metadata{},
			dataStore:        ds,
			BaseInputReader:  ir,
			BaseArtifactData: ow,
			Indexes:          invertBitSet(bitarray.NewBitSet(1), 1),
			Count:            1,
		}, newUploadFuture(ResponseStatusReady, nil), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := AsyncClientImpl{
				ArrayWriter: q,
			}

			gotPutFuture, err := c.UploadArray(ctx, tt.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("AsyncClientImpl.Upload() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if diff := deep.Equal(tt.wantPutFuture, gotPutFuture); diff != nil {
				t.Errorf("expected != actual. Diff: %v", diff)
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
				ArrayReader: tt.fields.Reader,
				ArrayWriter: tt.fields.Writer,
			}
			if err := c.Start(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("AsyncClientImpl.Start() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
