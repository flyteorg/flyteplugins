// Code generated by mockery v1.0.0. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"
import storage "github.com/lyft/flytestdlib/storage"

// InputFilePaths is an autogenerated mock type for the InputFilePaths type
type InputFilePaths struct {
	mock.Mock
}

// GetInputPath provides a mock function with given fields:
func (_m *InputFilePaths) GetInputPath() storage.DataReference {
	ret := _m.Called()

	var r0 storage.DataReference
	if rf, ok := ret.Get(0).(func() storage.DataReference); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(storage.DataReference)
	}

	return r0
}

// GetInputPrefixPath provides a mock function with given fields:
func (_m *InputFilePaths) GetInputPrefixPath() storage.DataReference {
	ret := _m.Called()

	var r0 storage.DataReference
	if rf, ok := ret.Get(0).(func() storage.DataReference); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(storage.DataReference)
	}

	return r0
}
