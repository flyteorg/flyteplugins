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
		assert.Equal(t, storage.DataReference("s3://bucket/x/6b0d31c0d563223024da45691584643ac78c96e8"), sd.GetOutputDataSandboxPath())
	})

	t.Run("error", func(t *testing.T) {
		ss := NewConstantShardSelector([]string{"s3:// abc"})
		sd, err := NewRandomPrefixShardedOutputSandbox(ctx, ss, "s3://bucket", "m", storage.URLPathConstructor{})
		assert.Error(t, err, "%s", sd)
	})
}

func TestNewShardedOutputSandbox(t *testing.T) {
	ctx := context.TODO()
	t.Run("", func(t *testing.T) {
		ss := NewConstantShardSelector([]string{"x"})
		sd, err := NewShardedOutputSandbox(ctx, ss, "s3://flyte", "unique", storage.URLPathConstructor{})
		assert.NoError(t, err)
		assert.Equal(t, storage.DataReference("s3://flyte/x/unique"), sd.GetOutputDataSandboxPath())
	})

	t.Run("error", func(t *testing.T) {
		ss := NewConstantShardSelector([]string{"s3:// abc"})
		sd, err := NewShardedOutputSandbox(ctx, ss, "s3://bucket", "m", storage.URLPathConstructor{})
		assert.Error(t, err, "%s", sd)
	})
}
