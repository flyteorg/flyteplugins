package mocks

import (
	"context"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
	"github.com/lyft/flytestdlib/storage"
	"github.com/stretchr/testify/mock"
)

type GetErrorPathCall struct {
	*mock.Call
}

func (c *GetErrorPathCall) Return(
	DataReferencevalue storage.DataReference,
) *GetErrorPathCall {
	return &GetErrorPathCall{Call: c.Call.Return(
		DataReferencevalue,
	)}
}

func (_m *OutputWriter) OnGetErrorPathMatch(matchers ...interface{}) *GetErrorPathCall {
	c := _m.On("GetErrorPath", matchers)
	return &GetErrorPathCall{Call: c}
}

func (_m *OutputWriter) OnGetErrorPath() *GetErrorPathCall {
	c := _m.On("GetErrorPath")
	return &GetErrorPathCall{Call: c}
}

type GetOutputPathCall struct {
	*mock.Call
}

func (c *GetOutputPathCall) Return(
	DataReferencevalue storage.DataReference,
) *GetOutputPathCall {
	return &GetOutputPathCall{Call: c.Call.Return(
		DataReferencevalue,
	)}
}

func (_m *OutputWriter) OnGetOutputPathMatch(matchers ...interface{}) *GetOutputPathCall {
	c := _m.On("GetOutputPath", matchers)
	return &GetOutputPathCall{Call: c}
}

func (_m *OutputWriter) OnGetOutputPath() *GetOutputPathCall {
	c := _m.On("GetOutputPath")
	return &GetOutputPathCall{Call: c}
}

type GetOutputPrefixPathCall struct {
	*mock.Call
}

func (c *GetOutputPrefixPathCall) Return(
	DataReferencevalue storage.DataReference,
) *GetOutputPrefixPathCall {
	return &GetOutputPrefixPathCall{Call: c.Call.Return(
		DataReferencevalue,
	)}
}

func (_m *OutputWriter) OnGetOutputPrefixPathMatch(matchers ...interface{}) *GetOutputPrefixPathCall {
	c := _m.On("GetOutputPrefixPath", matchers)
	return &GetOutputPrefixPathCall{Call: c}
}

func (_m *OutputWriter) OnGetOutputPrefixPath() *GetOutputPrefixPathCall {
	c := _m.On("GetOutputPrefixPath")
	return &GetOutputPrefixPathCall{Call: c}
}

type PutCall struct {
	*mock.Call
}

func (c *PutCall) Return(
	errorvalue error,
) *PutCall {
	return &PutCall{Call: c.Call.Return(
		errorvalue,
	)}
}

func (_m *OutputWriter) OnPutMatch(matchers ...interface{}) *PutCall {
	c := _m.On("Put", matchers)
	return &PutCall{Call: c}
}

func (_m *OutputWriter) OnPut(
	ctx context.Context,
	reader io.OutputReader,
) *PutCall {
	c := _m.On("Put",
		ctx,
		reader,
	)
	return &PutCall{Call: c}
}
