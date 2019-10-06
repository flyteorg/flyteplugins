// Code generated by mockery v1.0.0. DO NOT EDIT.

package mocks

import catalog "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/catalog"
import mock "github.com/stretchr/testify/mock"

// DownloadFuture is an autogenerated mock type for the DownloadFuture type
type DownloadFuture struct {
	mock.Mock
}

type DownloadFuture_GetResponse struct {
	*mock.Call
}

func (_m DownloadFuture_GetResponse) Return(_a0 catalog.DownloadResponse, _a1 error) *DownloadFuture_GetResponse {
	return &DownloadFuture_GetResponse{Call: _m.Call.Return(_a0, _a1)}
}

func (_m *DownloadFuture) OnGetResponse() *DownloadFuture_GetResponse {
	c := _m.On("GetResponse")
	return &DownloadFuture_GetResponse{Call: c}
}
func (_m *DownloadFuture) OnGetResponseMatch(matchers ...interface{}) *DownloadFuture_GetResponse {
	c := _m.On("GetResponse", matchers...)
	return &DownloadFuture_GetResponse{Call: c}
}

// GetResponse provides a mock function with given fields:
func (_m *DownloadFuture) GetResponse() (catalog.DownloadResponse, error) {
	ret := _m.Called()

	var r0 catalog.DownloadResponse
	if rf, ok := ret.Get(0).(func() catalog.DownloadResponse); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(catalog.DownloadResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

type DownloadFuture_GetResponseStatus struct {
	*mock.Call
}

func (_m DownloadFuture_GetResponseStatus) Return(_a0 catalog.ResponseStatus) *DownloadFuture_GetResponseStatus {
	return &DownloadFuture_GetResponseStatus{Call: _m.Call.Return(_a0)}
}

func (_m *DownloadFuture) OnGetResponseStatus() *DownloadFuture_GetResponseStatus {
	c := _m.On("GetResponseStatus")
	return &DownloadFuture_GetResponseStatus{Call: c}
}
func (_m *DownloadFuture) OnGetResponseStatusMatch(matchers ...interface{}) *DownloadFuture_GetResponseStatus {
	c := _m.On("GetResponseStatus", matchers...)
	return &DownloadFuture_GetResponseStatus{Call: c}
}

// GetResponseStatus provides a mock function with given fields:
func (_m *DownloadFuture) GetResponseStatus() catalog.ResponseStatus {
	ret := _m.Called()

	var r0 catalog.ResponseStatus
	if rf, ok := ret.Get(0).(func() catalog.ResponseStatus); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(catalog.ResponseStatus)
	}

	return r0
}
