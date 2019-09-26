package mocks

import (
	"context"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/storage"
	"github.com/stretchr/testify/mock"
)

type GetCall struct {
	*mock.Call
}

func (c *GetCall) Return(
	LiteralMapvalue core.LiteralMap,
	errorvalue error,
) *GetCall {
	return &GetCall{Call: c.Call.Return(
		LiteralMapvalue,
		errorvalue,
	)}
}

func (_m *InputReader) OnGetMatch(matchers ...interface{}) *GetCall {
	c := _m.On("Get", matchers)
	return &GetCall{Call: c}
}

func (_m *InputReader) OnGet(
	ctx context.Context,
) *GetCall {
	c := _m.On("Get",
		ctx,
	)
	return &GetCall{Call: c}
}

type GetInputPathCall struct {
	*mock.Call
}

func (c *GetInputPathCall) Return(
	DataReferencevalue storage.DataReference,
) *GetInputPathCall {
	return &GetInputPathCall{Call: c.Call.Return(
		DataReferencevalue,
	)}
}

func (_m *InputReader) OnGetInputPathMatch(matchers ...interface{}) *GetInputPathCall {
	c := _m.On("GetInputPath", matchers)
	return &GetInputPathCall{Call: c}
}

func (_m *InputReader) OnGetInputPath() *GetInputPathCall {
	c := _m.On("GetInputPath")
	return &GetInputPathCall{Call: c}
}

type GetInputPrefixPathCall struct {
	*mock.Call
}

func (c *GetInputPrefixPathCall) Return(
	DataReferencevalue storage.DataReference,
) *GetInputPrefixPathCall {
	return &GetInputPrefixPathCall{Call: c.Call.Return(
		DataReferencevalue,
	)}
}

func (_m *InputReader) OnGetInputPrefixPathMatch(matchers ...interface{}) *GetInputPrefixPathCall {
	c := _m.On("GetInputPrefixPath", matchers)
	return &GetInputPrefixPathCall{Call: c}
}

func (_m *InputReader) OnGetInputPrefixPath() *GetInputPrefixPathCall {
	c := _m.On("GetInputPrefixPath")
	return &GetInputPrefixPathCall{Call: c}
}
