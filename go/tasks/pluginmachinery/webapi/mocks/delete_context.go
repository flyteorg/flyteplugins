// Code generated by mockery v1.0.1. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// DeleteContext is an autogenerated mock type for the DeleteContext type
type DeleteContext struct {
	mock.Mock
}

type DeleteContext_Reason struct {
	*mock.Call
}

func (_m DeleteContext_Reason) Return(_a0 string) *DeleteContext_Reason {
	return &DeleteContext_Reason{Call: _m.Call.Return(_a0)}
}

func (_m *DeleteContext) OnReason() *DeleteContext_Reason {
	c := _m.On("Reason")
	return &DeleteContext_Reason{Call: c}
}

func (_m *DeleteContext) OnReasonMatch(matchers ...interface{}) *DeleteContext_Reason {
	c := _m.On("Reason", matchers...)
	return &DeleteContext_Reason{Call: c}
}

// Reason provides a mock function with given fields:
func (_m *DeleteContext) Reason() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

type DeleteContext_ResourceMeta struct {
	*mock.Call
}

func (_m DeleteContext_ResourceMeta) Return(_a0 interface{}) *DeleteContext_ResourceMeta {
	return &DeleteContext_ResourceMeta{Call: _m.Call.Return(_a0)}
}

func (_m *DeleteContext) OnResourceMeta() *DeleteContext_ResourceMeta {
	c := _m.On("ResourceMeta")
	return &DeleteContext_ResourceMeta{Call: c}
}

func (_m *DeleteContext) OnResourceMetaMatch(matchers ...interface{}) *DeleteContext_ResourceMeta {
	c := _m.On("ResourceMeta", matchers...)
	return &DeleteContext_ResourceMeta{Call: c}
}

// ResourceMeta provides a mock function with given fields:
func (_m *DeleteContext) ResourceMeta() interface{} {
	ret := _m.Called()

	var r0 interface{}
	if rf, ok := ret.Get(0).(func() interface{}); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(interface{})
		}
	}

	return r0
}