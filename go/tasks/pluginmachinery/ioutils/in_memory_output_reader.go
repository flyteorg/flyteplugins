package ioutils

import (
	"context"
	"fmt"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
)

type InMemoryOutputReader struct {
	literals *core.LiteralMap
	err      *io.ExecutionError
}

var _ io.OutputReader = InMemoryOutputReader{}

func (r InMemoryOutputReader) IsError(ctx context.Context) (bool, error) {
	return r.err != nil, nil
}

func (r InMemoryOutputReader) ReadError(ctx context.Context) (io.ExecutionError, error) {
	if r.err != nil {
		return *r.err, nil
	}

	return io.ExecutionError{}, fmt.Errorf("no execution error specified")
}

func (r InMemoryOutputReader) IsFile(ctx context.Context) bool {
	return false
}

func (r InMemoryOutputReader) Exists(ctx context.Context) (bool, error) {
	// TODO: should this return true if there is an error?
	return r.literals != nil, nil
}

func (r InMemoryOutputReader) Read(ctx context.Context) (*core.LiteralMap, *io.ExecutionError, error) {
	return r.literals, r.err, nil
}

func NewInMemoryOutputReader(literals *core.LiteralMap, err *io.ExecutionError) InMemoryOutputReader {
	return InMemoryOutputReader{
		literals: literals,
		err:      err,
	}
}
