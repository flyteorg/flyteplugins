package ioutils

import (
	"context"

	"github.com/lyft/flytestdlib/errors"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/storage"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/io"
)

const (
	ErrFailedRead string = "READ_FAILED"
)

var (
	// Ensure we get an early build break if interface changes and these classes do not conform.
	_ io.InputFilePaths = SimpleInputFilePath{}
	_ io.InputReader    = RemoteFileInputReader{}
)

type RemoteFileInputReader struct {
	inputPath      io.InputFilePaths
	store          storage.ComposedProtobufStore
	maxPayloadSize int64
}

func (r RemoteFileInputReader) GetInputPath() storage.DataReference {
	return r.inputPath.GetInputPath()
}

func (r RemoteFileInputReader) Get(ctx context.Context) (*core.LiteralMap, error) {
	d := &core.LiteralMap{}
	if err := r.store.ReadProtobuf(ctx, r.inputPath.GetInputPath(), d); err != nil {
		// TODO change flytestdlib to return protobuf unmarshal errors separately. As this can indicate malformed output and we should catch that
		return nil, errors.Wrapf(ErrFailedRead, err, "failed to read data from dataDir [%v].", r.inputPath.GetInputPath())
	}

	return d, nil

}

func NewRemoteFileInputReader(_ context.Context, store storage.ComposedProtobufStore, inputPaths io.InputFilePaths, maxDatasetSize int64) RemoteFileInputReader {
	return RemoteFileInputReader{
		inputPath:      inputPaths,
		store:          store,
		maxPayloadSize: maxDatasetSize,
	}
}

type SimpleInputFilePath struct {
	filePath storage.DataReference
}

func (s SimpleInputFilePath) GetInputPath() storage.DataReference {
	return s.filePath
}

func NewInputFilePaths(_ context.Context, inputPath storage.DataReference) SimpleInputFilePath {
	return SimpleInputFilePath{filePath: inputPath}
}
