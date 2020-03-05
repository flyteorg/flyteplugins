package ioutils

import (
	"context"
	"testing"

	"github.com/lyft/flytestdlib/storage"
	"github.com/stretchr/testify/assert"
)

func TestNewOutputSandbox(t *testing.T) {
	assert.Equal(t, NewOutputSandbox(context.TODO(), "x").GetOutputDataSandboxPath(), storage.DataReference("x"))
}

func TestNewRandomPrefixShardedOutputSandbox(t *testing.T) {
	ctx := context.TODO()

	t.Run("success-path", func(t *testing.T) {
		ss := NewConstantShardSelector([]string{"x"})
		sd, err := NewRandomPrefixShardedOutputSandbox(ctx, ss, "s3://bucket", "m", storage.URLPathConstructor{})
		assert.NoError(t, err)
		assert.Equal(t, storage.DataReference("s3://bucket/x/6f8f57715090da2632453988d9a1501b"), sd.GetOutputDataSandboxPath())
	})

	t.Run("error", func(t *testing.T) {
		ss := NewConstantShardSelector([]string{"x"})
		_, err := NewRandomPrefixShardedOutputSandbox(ctx, ss, "#/	", "m", storage.URLPathConstructor{})
		assert.Error(t, err)
	})
}
