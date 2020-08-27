package sagemaker

import (
	"context"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/ioutils"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/storage"
)

type JobOutputPaths struct {
	io.OutputFilePaths
	jobName      string
	store        storage.ReferenceConstructor
	outputPrefix storage.DataReference
}

func constructPath(store storage.ReferenceConstructor, base storage.DataReference, suffix string) storage.DataReference {
	res, err := store.ConstructReference(context.Background(), base, suffix)
	if err != nil {
		logger.Error(context.Background(), "Failed to construct path. Base[%v] Error: %v", base, err)
	}

	return res
}

func (s JobOutputPaths) GetOutputPrefixPath() storage.DataReference {
	return constructPath(s.store, s.GetOutputPrefixPath(), s.jobName)
}

func (s JobOutputPaths) GetOutputPath() storage.DataReference {
	return constructPath(s.store, s.GetOutputPrefixPath(), ioutils.OutputsSuffix)
}

func (s JobOutputPaths) GetErrorPath() storage.DataReference {
	return constructPath(s.store, s.GetOutputPrefixPath(), ioutils.ErrorsSuffix)
}

func NewJobOutputPaths(_ context.Context, store storage.ReferenceConstructor, outputPrefix storage.DataReference, jobName string) JobOutputPaths {
	return JobOutputPaths{
		jobName:      jobName,
		store:        store,
		outputPrefix: outputPrefix,
	}
}
