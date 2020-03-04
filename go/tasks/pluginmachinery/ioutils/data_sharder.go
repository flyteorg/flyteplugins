package ioutils

import "context"

// This interface allows shard selection for OutputSandbox. The API required the Initialize method be invoked before
// invoking the Shard string
type ShardSelector interface {
	Initialize(ctx context.Context) error
	GetShardPrefix(ctx context.Context, s []byte) (string, error)
}
