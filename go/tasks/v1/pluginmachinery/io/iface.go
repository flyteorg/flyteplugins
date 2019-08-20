package io

import (
	"context"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/storage"
)

//go:generate mockery -all -case=underscore

type InputFilePaths interface {
	GetInputPath() storage.DataReference
}

type InputReader interface {
	InputFilePaths
	Get(ctx context.Context) (*core.LiteralMap, error)
}

type OutputReader interface {
	IsError(ctx context.Context) (bool, error)
	ReadError(ctx context.Context) (ExecutionError, error)
	IsFile(ctx context.Context) bool
	Exists(ctx context.Context) (bool, error)
	Read(ctx context.Context) (*core.LiteralMap, *ExecutionError, error)
}

type OutputFilePaths interface {
	GetOutputPrefixPath() storage.DataReference
	GetOutputPath() storage.DataReference
	GetErrorPath() storage.DataReference
}

type OutputWriter interface {
	OutputFilePaths
	Put(ctx context.Context, reader OutputReader) error
}

type ExecutionError struct {
	*core.ExecutionError
	IsRecoverable bool
}
