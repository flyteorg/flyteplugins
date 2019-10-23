package array

import (
	"context"
	"testing"

	"github.com/lyft/flytestdlib/bitarray"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/catalog"

	catalogMocks "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/catalog/mocks"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
	ioMocks "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io/mocks"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"

	pluginMocks "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core/mocks"

	arrayCore "github.com/lyft/flyteplugins/go/tasks/plugins/array/core"
)

func TestDetermineDiscoverability(t *testing.T) {
	ctx := context.Background()

	tr := &pluginMocks.TaskReader{}
	tr.OnRead(ctx).Return(&core.TaskTemplate{
		Id: &core.Identifier{
			ResourceType: core.ResourceType_TASK,
			Project:      "p",
			Domain:       "d",
			Name:         "n",
			Version:      "1",
		},
		Interface: &core.TypedInterface{
			Inputs:  &core.VariableMap{Variables: map[string]*core.Variable{}},
			Outputs: &core.VariableMap{Variables: map[string]*core.Variable{}},
		},
		Metadata: &core.TaskMetadata{
			Discoverable:     true,
			DiscoveryVersion: "1",
		},
		Target: &core.TaskTemplate_Container{
			Container: &core.Container{
				Command: []string{"cmd"},
				Args:    []string{"{{$inputPrefix}}"},
				Image:   "img1",
			},
		},
	}, nil)

	ds, err := storage.NewDataStore(&storage.Config{Type: storage.TypeMemory}, promutils.NewTestScope())
	assert.NoError(t, err)

	download := &catalogMocks.DownloadResponse{}
	download.OnGetCachedCount().Return(0)
	download.OnGetCachedResults().Return(bitarray.NewBitSet(1))
	download.OnGetResultsSize().Return(1)

	f := &catalogMocks.DownloadFuture{}
	f.OnGetResponseStatus().Return(catalog.ResponseStatusReady)
	f.OnGetResponseError().Return(nil)
	f.OnGetResponse().Return(download, nil)

	cat := &catalogMocks.AsyncClient{}
	cat.OnDownloadMatch(mock.Anything, mock.Anything).Return(f, nil)

	ir := &ioMocks.InputReader{}
	ir.OnGetInputPrefixPath().Return("/prefix/")

	ow := &ioMocks.OutputWriter{}
	ow.OnGetOutputPrefixPath().Return("/prefix/")
	ow.OnGetOutputPath().Return("/prefix/outputs.pb")
	ow.On("Put", mock.Anything, mock.Anything).Return(func(ctx context.Context, or io.OutputReader) error {
		m, ee, err := or.Read(ctx)
		assert.NoError(t, err)
		assert.Nil(t, ee)
		assert.NotNil(t, m)

		assert.NoError(t, ds.WriteProtobuf(ctx, "/prefix/outputs.pb", storage.Options{}, m))

		return err
	})

	tCtx := &pluginMocks.TaskExecutionContext{}
	tCtx.OnTaskReader().Return(tr)
	tCtx.OnInputReader().Return(ir)
	tCtx.OnDataStore().Return(ds)
	tCtx.OnCatalog().Return(cat)
	tCtx.OnOutputWriter().Return(ow)
	tCtx.OnTaskRefreshIndicator().Return(func(ctx context.Context) {
		t.Log("Refresh called")
	})

	state := &arrayCore.State{
		CurrentPhase: arrayCore.PhaseStart,
	}

	got, err := DetermineDiscoverability(ctx, tCtx, state)
	assert.NoError(t, err)
	assert.Equal(t, arrayCore.PhasePreLaunch, got.CurrentPhase)
}
