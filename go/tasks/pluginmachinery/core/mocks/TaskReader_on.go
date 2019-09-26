package mocks

import (
	"context"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/stretchr/testify/mock"
)

type ReadCall struct {
	*mock.Call
}

func (c *ReadCall) Return(
	TaskTemplatevalue *core.TaskTemplate,
	errorvalue error,
) *ReadCall {
	return &ReadCall{Call: c.Call.Return(
		TaskTemplatevalue,
		errorvalue,
	)}
}

func (_m *TaskReader) OnReadMatch(matchers ...interface{}) *ReadCall {
	c := _m.On("Read", matchers...)
	return &ReadCall{Call: c}
}

func (_m *TaskReader) OnRead(
	ctx context.Context,
) *ReadCall {
	c := _m.On("Read",
		ctx,
	)
	return &ReadCall{Call: c}
}
