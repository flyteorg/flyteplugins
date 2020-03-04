package ioutils

import (
	"context"
	"crypto/md5"

	"github.com/lyft/flytestdlib/storage"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
)

type randomPrefixShardedOutputSandbox struct {
	path storage.DataReference
}

func (r randomPrefixShardedOutputSandbox) GetOutputDataSandboxPath() storage.DataReference {
	return r.path
}

// Creates a deterministic OutputSandbox whose path is distributed based on the ShardSelector passed in.
// Determinism depends on the outputMetadataPath
// Potential performance problem, as creating anew randomprefixShardedOutput Sandbox may be expensive as it hashes the outputMetadataPath
// the final OutputSandbox is created in the shard selected by the sharder at the basePath and then appended by a hashed value of the outputMetadata
func NewRandomPrefixShardedOutputSandbox(ctx context.Context, sharder ShardSelector, basePath, outputMetadataPath storage.DataReference, store storage.ReferenceConstructor) (io.OutputDataSandbox, error) {
	o := []byte(outputMetadataPath)
	prefix, err := sharder.GetShardPrefix(ctx, o)
	if err != nil {
		return nil, err
	}
	m := md5.New()
	m.Write(o)
	path, err := store.ConstructReference(ctx, basePath, prefix, string(m.Sum(nil)))
	if err != nil {
		return nil, err
	}
	return randomPrefixShardedOutputSandbox{
		path: path,
	}, nil
}
