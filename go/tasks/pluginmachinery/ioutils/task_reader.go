package ioutils

import (
	"context"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flytestdlib/atomic"
	"github.com/flyteorg/flytestdlib/storage"
	"github.com/pkg/errors"
)

// lazyUploadingTaskReader provides a lazy interface that uploads the core.TaskTemplate to a configured location,
// only if the location is accessed. This reduces the potential overhead of writing the template
type lazyUploadingTaskReader struct {
	core.TaskReader
	uploaded   atomic.Bool
	store      storage.ProtobufStore
	remotePath storage.DataReference
}

func (r *lazyUploadingTaskReader) Path(ctx context.Context) (storage.DataReference, error) {
	// We are using atomic because it is ok to re-upload in some cases. We know that most of the plugins are
	// executed in a single go-routine, so chances of a race condition are minimal.
	if !r.uploaded.Load() {
		t, err := r.TaskReader.Read(ctx)
		if err != nil {
			return "", err
		}
		err = r.store.WriteProtobuf(ctx, r.remotePath, storage.Options{}, t)
		if err != nil {
			return "", errors.Wrapf(err, "failed to store task template to remote path [%s]", r.remotePath)
		}
		r.uploaded.Store(true)
	}
	return r.remotePath, nil
}

// NewLazyUploadingTaskReader decorates an existing TaskReader and adds a functionality to allow lazily uploading the task template to
// a remote location, only when the location information is accessed
func NewLazyUploadingTaskReader(baseTaskReader core.TaskReader, remotePath storage.DataReference, store storage.ProtobufStore) core.TaskReader {
	return &lazyUploadingTaskReader{
		TaskReader: baseTaskReader,
		uploaded:   atomic.NewBool(false),
		store:      store,
		remotePath: remotePath,
	}
}
