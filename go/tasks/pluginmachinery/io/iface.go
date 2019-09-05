package io

import (
	"context"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/storage"
)

//go:generate mockery -all -case=underscore

// If using Files for IO with tasks, then the input will be written to this path
// All the files are always created in a sandbox per execution
type InputFilePaths interface {
	// The inputs file path, minus the protobuf file name.
	GetInputPrefixPath() storage.DataReference
	// Gets a path for where the protobuf encoded inputs of type `core.LiteralMap` can be found. The returned value is an URN in the configured storage backend
	GetInputPath() storage.DataReference
}

// InputReader provides a method to access the inputs for a task execution within the plugin's Task Context
type InputReader interface {
	InputFilePaths
	// Get the inputs for this task as a literal map, an error is returned only in case of systemic errors.
	// No outputs or void is indicated using *core.LiteralMap -> nil
	Get(ctx context.Context) (*core.LiteralMap, error)
}

// OutputReader
type OutputReader interface {
	// Returns true if an error was detected when reading the output and false if no error was detected
	IsError(ctx context.Context) (bool, error)
	// Returns the error as type ExecutionError
	ReadError(ctx context.Context) (ExecutionError, error)
	// Returns true if the outputs are using the OutputFilePaths specified files. If so it allows the system to
	// optimize the reads of the files
	IsFile(ctx context.Context) bool
	// Returns true if the output exists false otherwise
	Exists(ctx context.Context) (bool, error)
	// Returns the output -> *core.LiteralMap (nil if void), *ExecutionError if user error when reading the output and error to indicate system problems
	Read(ctx context.Context) (*core.LiteralMap, *ExecutionError, error)
}

// All paths where various outputs produced by the task can be placed, such that the framework can directly access them.
// All paths are reperesented using storage.DataReference -> an URN for the configured storage backend
type OutputFilePaths interface {
	// A path to a directory or prefix that contains all execution metadata for this execution
	GetOutputPrefixPath() storage.DataReference
	// A fully qualified path (URN) to where the framework expects the output to exist in the configured storage backend
	GetOutputPath() storage.DataReference
	// A Fully qualified path (URN) where the error information should be placed as a protobuf core.ErrorDocument. It is not directly
	// used by the framework, but could be used in the future
	GetErrorPath() storage.DataReference
}

// Framework Output writing interface.
type OutputWriter interface {
	OutputFilePaths
	// Once the task completes, use this method to indicate the output accessor to the framework
	Put(ctx context.Context, reader OutputReader) error
}

// Indicates any error in executing the task
type ExecutionError struct {
	// Core error structure
	*core.ExecutionError
	// Indicates if this error is recoverable
	IsRecoverable bool
}
