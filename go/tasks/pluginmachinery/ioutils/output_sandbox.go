package ioutils

import (
	"context"
	"crypto/md5"
	"encoding/hex"

	"github.com/lyft/flytestdlib/storage"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
)

type precomputedOutputSandbox struct {
	path storage.DataReference
}

func (r precomputedOutputSandbox) GetOutputDataSandboxPath() storage.DataReference {
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
	path, err := store.ConstructReference(ctx, basePath, prefix, hex.EncodeToString(m.Sum(nil)))
	if err != nil {
		return nil, err
	}
	return precomputedOutputSandbox{
		path: path,
	}, nil
}

// A simple Output sandbox at a given path
func NewOutputSandbox(_ context.Context, outputSandboxPath storage.DataReference) io.OutputDataSandbox {
	return precomputedOutputSandbox{path: outputSandboxPath}
}
