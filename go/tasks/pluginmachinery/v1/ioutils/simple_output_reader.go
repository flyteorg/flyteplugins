package ioutils

import (
	"context"
	"fmt"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/io"
)

type simpleOutputReader struct {
	literals *core.LiteralMap
	err      *io.ExecutionError
}

func (r simpleOutputReader) IsError(ctx context.Context) (bool, error) {
	return r.err != nil, nil
}

func (r simpleOutputReader) ReadError(ctx context.Context) (io.ExecutionError, error) {
	if r.err != nil {
		return *r.err, nil
	}

	return io.ExecutionError{}, fmt.Errorf("no execution error specified")
}

func (r simpleOutputReader) IsFile(ctx context.Context) bool {
	return false
}

func (r simpleOutputReader) Exists(ctx context.Context) (bool, error) {
	// TODO: should this return true if there is an error?
	return r.literals != nil, nil
}

func (r simpleOutputReader) Read(ctx context.Context) (*core.LiteralMap, *io.ExecutionError, error) {
	return r.literals, r.err, nil
}

func NewSimpleOutputReader(literals *core.LiteralMap, err *io.ExecutionError) io.OutputReader {
	return simpleOutputReader{
		literals: literals,
		err:      err,
	}
}
