package mocks

import (
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/stretchr/testify/mock"
)

type GetGeneratedNameCall struct {
	*mock.Call
}

func (c *GetGeneratedNameCall) Return(
	stringvalue string,
) *GetGeneratedNameCall {
	return &GetGeneratedNameCall{Call: c.Call.Return(
		stringvalue,
	)}
}

func (_m *TaskExecutionID) OnGetGeneratedNameMatch(matchers ...interface{}) *GetGeneratedNameCall {
	c := _m.On("GetGeneratedName", matchers)
	return &GetGeneratedNameCall{Call: c}
}

func (_m *TaskExecutionID) OnGetGeneratedName() *GetGeneratedNameCall {
	c := _m.On("GetGeneratedName")
	return &GetGeneratedNameCall{Call: c}
}

type GetIDCall struct {
	*mock.Call
}

func (c *GetIDCall) Return(
	TaskExecutionIdentifiervalue core.TaskExecutionIdentifier,
) *GetIDCall {
	return &GetIDCall{Call: c.Call.Return(
		TaskExecutionIdentifiervalue,
	)}
}

func (_m *TaskExecutionID) OnGetIDMatch(matchers ...interface{}) *GetIDCall {
	c := _m.On("GetID", matchers)
	return &GetIDCall{Call: c}
}

func (_m *TaskExecutionID) OnGetID() *GetIDCall {
	c := _m.On("GetID")
	return &GetIDCall{Call: c}
}
