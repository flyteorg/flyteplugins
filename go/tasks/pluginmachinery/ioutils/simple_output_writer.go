package ioutils

import (
	"context"
	"fmt"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
	"github.com/lyft/flytestdlib/storage"
)

type SimpleOutputWriter struct {
	io.OutputFilePaths
	store storage.ProtobufStore
}

type SimpleOutputFilePaths struct {
	outputPrefix storage.DataReference
	store        storage.ReferenceConstructor
}

var (
	_ io.OutputWriter    = SimpleOutputWriter{}
	_ io.OutputFilePaths = SimpleOutputFilePaths{}
)

func (w SimpleOutputFilePaths) GetOutputPrefixPath() storage.DataReference {
	return w.outputPrefix
}

func (w SimpleOutputFilePaths) GetOutputPath() storage.DataReference {
	return constructPath(w.store, w.outputPrefix, OutputsSuffix)
}

func (w SimpleOutputFilePaths) GetErrorPath() storage.DataReference {
	return constructPath(w.store, w.outputPrefix, ErrorsSuffix)
}

func (w SimpleOutputFilePaths) GetFuturesPath() storage.DataReference {
	return constructPath(w.store, w.outputPrefix, FuturesSuffix)
}

func (w SimpleOutputWriter) Put(ctx context.Context, reader io.OutputReader) error {
	literals, executionErr, err := reader.Read(ctx)
	if err != nil {
		return err
	}

	if executionErr != nil {
		return w.store.WriteProtobuf(ctx, w.GetErrorPath(), storage.Options{}, executionErr)
	}

	if literals != nil {
		return w.store.WriteProtobuf(ctx, w.GetOutputPath(), storage.Options{}, literals)
	}

	return fmt.Errorf("no data found to write")
}

func NewSimpleOutputFilePaths(_ context.Context, store storage.ReferenceConstructor, outputPrefix storage.DataReference) SimpleOutputFilePaths {
	return SimpleOutputFilePaths{
		store:        store,
		outputPrefix: outputPrefix,
	}
}

func NewSimpleOutputWriter(_ context.Context, store storage.ProtobufStore, outputFilePaths io.OutputFilePaths) SimpleOutputWriter {
	return SimpleOutputWriter{
		OutputFilePaths: outputFilePaths,
		store:           store,
	}
}
