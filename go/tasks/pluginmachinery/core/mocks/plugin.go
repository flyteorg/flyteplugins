// Code generated by mockery v1.0.0. DO NOT EDIT.

package mocks

import context "context"
import core "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
import mock "github.com/stretchr/testify/mock"

// Plugin is an autogenerated mock type for the Plugin type
type Plugin struct {
	mock.Mock
}

// Abort provides a mock function with given fields: ctx, tCtx
func (_m *Plugin) Abort(ctx context.Context, tCtx core.TaskExecutionContext) error {
	ret := _m.Called(ctx, tCtx)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, core.TaskExecutionContext) error); ok {
		r0 = rf(ctx, tCtx)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Finalize provides a mock function with given fields: ctx, tCtx
func (_m *Plugin) Finalize(ctx context.Context, tCtx core.TaskExecutionContext) error {
	ret := _m.Called(ctx, tCtx)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, core.TaskExecutionContext) error); ok {
		r0 = rf(ctx, tCtx)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetID provides a mock function with given fields:
func (_m *Plugin) GetID() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// GetProperties provides a mock function with given fields:
func (_m *Plugin) GetProperties() core.PluginProperties {
	ret := _m.Called()

	var r0 core.PluginProperties
	if rf, ok := ret.Get(0).(func() core.PluginProperties); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(core.PluginProperties)
	}

	return r0
}

// Handle provides a mock function with given fields: ctx, tCtx
func (_m *Plugin) Handle(ctx context.Context, tCtx core.TaskExecutionContext) (core.Transition, error) {
	ret := _m.Called(ctx, tCtx)

	var r0 core.Transition
	if rf, ok := ret.Get(0).(func(context.Context, core.TaskExecutionContext) core.Transition); ok {
		r0 = rf(ctx, tCtx)
	} else {
		r0 = ret.Get(0).(core.Transition)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, core.TaskExecutionContext) error); ok {
		r1 = rf(ctx, tCtx)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
