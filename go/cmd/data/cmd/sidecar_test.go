package cmd

import (
	"context"
	"encoding/base64"
	"io/ioutil"
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/storage"
	"github.com/stretchr/testify/assert"

	"github.com/lyft/flyteplugins/go/cmd/data/cmd/containerwatcher"
)

func TestUploadOptions_Upload(t *testing.T) {
	tmpFolderLocation := ""
	tmpPrefix := "upload_test"
	outputPath := "output"

	ctx := context.TODO()
	uopts := UploadOptions{
		remoteOutputsPrefix: outputPath,
		metadataFormat:      core.DataLoadingConfig_JSON.String(),
		uploadMode:          core.IOStrategy_UPLOAD_ON_EXIT.String(),
		startWatcherType:    containerwatcher.WatcherTypeFile,
	}

	t.Run("uploadNoOutputs", func(t *testing.T) {
		tmpDir, err := ioutil.TempDir(tmpFolderLocation, tmpPrefix)
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, os.RemoveAll(tmpDir))
		}()
		uopts.localDirectoryPath = tmpDir

		uopts.outputInterface = nil
		s := promutils.NewTestScope()
		store, err := storage.NewDataStore(&storage.Config{Type: storage.TypeMemory}, s.NewSubScope("storage"))
		assert.NoError(t, err)
		uopts.RootOptions = &RootOptions{
			Scope: s,
			Store: store,
		}

		assert.NoError(t, uopts.Upload(ctx))
	})

	t.Run("uploadBlobType-FileNotFound", func(t *testing.T) {
		vmap := &core.VariableMap{
			Variables: map[string]*core.Variable{
				"x": {
					Type:        &core.LiteralType{Type: &core.LiteralType_Blob{Blob: &core.BlobType{Dimensionality: core.BlobType_SINGLE}}},
					Description: "example",
				},
			},
		}
		d, err := proto.Marshal(vmap)
		assert.NoError(t, err)
		uopts.outputInterface = []byte(base64.StdEncoding.EncodeToString(d))
		s := promutils.NewTestScope()
		store, err := storage.NewDataStore(&storage.Config{Type: storage.TypeMemory}, s.NewSubScope("storage"))
		assert.NoError(t, err)
		uopts.RootOptions = &RootOptions{
			Scope: s,
			Store: store,
		}

		assert.NoError(t, uopts.Upload(ctx))
	})
}
