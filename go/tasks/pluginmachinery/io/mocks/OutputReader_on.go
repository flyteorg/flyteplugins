package mocks

import (
	"context"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
	"github.com/stretchr/testify/mock"
)

type ExistsCall struct {
	*mock.Call
}

func (c *ExistsCall) Return(
	boolvalue bool,
	errorvalue error,
) *ExistsCall {
	return &ExistsCall{Call: c.Call.Return(
		boolvalue,
		errorvalue,
	)}
}

func (_m *OutputReader) OnExistsMatch(matchers ...interface{}) *ExistsCall {
	c := _m.On("Exists", matchers)
	return &ExistsCall{Call: c}
}

func (_m *OutputReader) OnExists(
	ctx context.Context,
) *ExistsCall {
	c := _m.On("Exists",
		ctx,
	)
	return &ExistsCall{Call: c}
}

type IsErrorCall struct {
	*mock.Call
}

func (c *IsErrorCall) Return(
	boolvalue bool,
	errorvalue error,
) *IsErrorCall {
	return &IsErrorCall{Call: c.Call.Return(
		boolvalue,
		errorvalue,
	)}
}

func (_m *OutputReader) OnIsErrorMatch(matchers ...interface{}) *IsErrorCall {
	c := _m.On("IsError", matchers)
	return &IsErrorCall{Call: c}
}

func (_m *OutputReader) OnIsError(
	ctx context.Context,
) *IsErrorCall {
	c := _m.On("IsError",
		ctx,
	)
	return &IsErrorCall{Call: c}
}

type IsFileCall struct {
	*mock.Call
}

func (c *IsFileCall) Return(
	boolvalue bool,
) *IsFileCall {
	return &IsFileCall{Call: c.Call.Return(
		boolvalue,
	)}
}

func (_m *OutputReader) OnIsFileMatch(matchers ...interface{}) *IsFileCall {
	c := _m.On("IsFile", matchers)
	return &IsFileCall{Call: c}
}

func (_m *OutputReader) OnIsFile(
	ctx context.Context,
) *IsFileCall {
	c := _m.On("IsFile",
		ctx,
	)
	return &IsFileCall{Call: c}
}

type ReadCall struct {
	*mock.Call
}

func (c *ReadCall) Return(
	LiteralMapvalue core.LiteralMap,
	ExecutionErrorvalue io.ExecutionError,
	errorvalue error,
) *ReadCall {
	return &ReadCall{Call: c.Call.Return(
		LiteralMapvalue,
		ExecutionErrorvalue,
		errorvalue,
	)}
}

func (_m *OutputReader) OnReadMatch(matchers ...interface{}) *ReadCall {
	c := _m.On("Read", matchers)
	return &ReadCall{Call: c}
}

func (_m *OutputReader) OnRead(
	ctx context.Context,
) *ReadCall {
	c := _m.On("Read",
		ctx,
	)
	return &ReadCall{Call: c}
}

type ReadErrorCall struct {
	*mock.Call
}

func (c *ReadErrorCall) Return(
	ExecutionErrorvalue io.ExecutionError,
	errorvalue error,
) *ReadErrorCall {
	return &ReadErrorCall{Call: c.Call.Return(
		ExecutionErrorvalue,
		errorvalue,
	)}
}

func (_m *OutputReader) OnReadErrorMatch(matchers ...interface{}) *ReadErrorCall {
	c := _m.On("ReadError", matchers)
	return &ReadErrorCall{Call: c}
}

func (_m *OutputReader) OnReadError(
	ctx context.Context,
) *ReadErrorCall {
	c := _m.On("ReadError",
		ctx,
	)
	return &ReadErrorCall{Call: c}
}
