// Code generated by mockery v1.0.0. DO NOT EDIT.

package mocks

import core "github.com/lyft/flyteplugins/go/tasks/v1/pluginmachinery/core"
import mock "github.com/stretchr/testify/mock"
import promutils "github.com/lyft/flytestdlib/promutils"

// SetupContext is an autogenerated mock type for the SetupContext type
type SetupContext struct {
	mock.Mock
}

// EnqueueOwner provides a mock function with given fields:
func (_m *SetupContext) EnqueueOwner() core.EnqueueOwner {
	ret := _m.Called()

	var r0 core.EnqueueOwner
	if rf, ok := ret.Get(0).(func() core.EnqueueOwner); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(core.EnqueueOwner)
		}
	}

	return r0
}

// KubeClient provides a mock function with given fields:
func (_m *SetupContext) KubeClient() core.KubeClient {
	ret := _m.Called()

	var r0 core.KubeClient
	if rf, ok := ret.Get(0).(func() core.KubeClient); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(core.KubeClient)
		}
	}

	return r0
}

// MetricsScope provides a mock function with given fields:
func (_m *SetupContext) MetricsScope() promutils.Scope {
	ret := _m.Called()

	var r0 promutils.Scope
	if rf, ok := ret.Get(0).(func() promutils.Scope); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(promutils.Scope)
		}
	}

	return r0
}

// OwnerKind provides a mock function with given fields:
func (_m *SetupContext) OwnerKind() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}
