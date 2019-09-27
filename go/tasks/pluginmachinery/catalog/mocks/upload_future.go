// Code generated by mockery v1.0.0. DO NOT EDIT.

package mocks

import catalog "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/catalog"
import mock "github.com/stretchr/testify/mock"

// UploadFuture is an autogenerated mock type for the UploadFuture type
type UploadFuture struct {
	mock.Mock
}

// GetResponseStatus provides a mock function with given fields:
func (_m *UploadFuture) GetResponseStatus() catalog.ResponseStatus {
	ret := _m.Called()

	var r0 catalog.ResponseStatus
	if rf, ok := ret.Get(0).(func() catalog.ResponseStatus); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(catalog.ResponseStatus)
	}

	return r0
}
