// Code generated by mockery v1.0.0. DO NOT EDIT.

package mocks

import core "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
import mock "github.com/stretchr/testify/mock"
import types "k8s.io/apimachinery/pkg/types"
import v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// TaskExecutionMetadata is an autogenerated mock type for the TaskExecutionMetadata type
type TaskExecutionMetadata struct {
	mock.Mock
}

type TaskExecutionMetadata_GetAnnotations struct {
	*mock.Call
}

func (_m TaskExecutionMetadata_GetAnnotations) Return(_a0 map[string]string) *TaskExecutionMetadata_GetAnnotations {
	return &TaskExecutionMetadata_GetAnnotations{Call: _m.Call.Return(_a0)}
}

func (_m *TaskExecutionMetadata) OnGetAnnotations() *TaskExecutionMetadata_GetAnnotations {
	c := _m.On("GetAnnotations")
	return &TaskExecutionMetadata_GetAnnotations{Call: c}
}
func (_m *TaskExecutionMetadata) OnGetAnnotationsMatch(matchers ...interface{}) *TaskExecutionMetadata_GetAnnotations {
	c := _m.On("GetAnnotations", matchers...)
	return &TaskExecutionMetadata_GetAnnotations{Call: c}
}

// GetAnnotations provides a mock function with given fields:
func (_m *TaskExecutionMetadata) GetAnnotations() map[string]string {
	ret := _m.Called()

	var r0 map[string]string
	if rf, ok := ret.Get(0).(func() map[string]string); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]string)
		}
	}

	return r0
}

type TaskExecutionMetadata_GetK8sServiceAccount struct {
	*mock.Call
}

func (_m TaskExecutionMetadata_GetK8sServiceAccount) Return(_a0 string) *TaskExecutionMetadata_GetK8sServiceAccount {
	return &TaskExecutionMetadata_GetK8sServiceAccount{Call: _m.Call.Return(_a0)}
}

func (_m *TaskExecutionMetadata) OnGetK8sServiceAccount() *TaskExecutionMetadata_GetK8sServiceAccount {
	c := _m.On("GetK8sServiceAccount")
	return &TaskExecutionMetadata_GetK8sServiceAccount{Call: c}
}
func (_m *TaskExecutionMetadata) OnGetK8sServiceAccountMatch(matchers ...interface{}) *TaskExecutionMetadata_GetK8sServiceAccount {
	c := _m.On("GetK8sServiceAccount", matchers...)
	return &TaskExecutionMetadata_GetK8sServiceAccount{Call: c}
}

// GetK8sServiceAccount provides a mock function with given fields:
func (_m *TaskExecutionMetadata) GetK8sServiceAccount() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

type TaskExecutionMetadata_GetLabels struct {
	*mock.Call
}

func (_m TaskExecutionMetadata_GetLabels) Return(_a0 map[string]string) *TaskExecutionMetadata_GetLabels {
	return &TaskExecutionMetadata_GetLabels{Call: _m.Call.Return(_a0)}
}

func (_m *TaskExecutionMetadata) OnGetLabels() *TaskExecutionMetadata_GetLabels {
	c := _m.On("GetLabels")
	return &TaskExecutionMetadata_GetLabels{Call: c}
}
func (_m *TaskExecutionMetadata) OnGetLabelsMatch(matchers ...interface{}) *TaskExecutionMetadata_GetLabels {
	c := _m.On("GetLabels", matchers...)
	return &TaskExecutionMetadata_GetLabels{Call: c}
}

// GetLabels provides a mock function with given fields:
func (_m *TaskExecutionMetadata) GetLabels() map[string]string {
	ret := _m.Called()

	var r0 map[string]string
	if rf, ok := ret.Get(0).(func() map[string]string); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]string)
		}
	}

	return r0
}

type TaskExecutionMetadata_GetNamespace struct {
	*mock.Call
}

func (_m TaskExecutionMetadata_GetNamespace) Return(_a0 string) *TaskExecutionMetadata_GetNamespace {
	return &TaskExecutionMetadata_GetNamespace{Call: _m.Call.Return(_a0)}
}

func (_m *TaskExecutionMetadata) OnGetNamespace() *TaskExecutionMetadata_GetNamespace {
	c := _m.On("GetNamespace")
	return &TaskExecutionMetadata_GetNamespace{Call: c}
}
func (_m *TaskExecutionMetadata) OnGetNamespaceMatch(matchers ...interface{}) *TaskExecutionMetadata_GetNamespace {
	c := _m.On("GetNamespace", matchers...)
	return &TaskExecutionMetadata_GetNamespace{Call: c}
}

// GetNamespace provides a mock function with given fields:
func (_m *TaskExecutionMetadata) GetNamespace() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

type TaskExecutionMetadata_GetOverrides struct {
	*mock.Call
}

func (_m TaskExecutionMetadata_GetOverrides) Return(_a0 core.TaskOverrides) *TaskExecutionMetadata_GetOverrides {
	return &TaskExecutionMetadata_GetOverrides{Call: _m.Call.Return(_a0)}
}

func (_m *TaskExecutionMetadata) OnGetOverrides() *TaskExecutionMetadata_GetOverrides {
	c := _m.On("GetOverrides")
	return &TaskExecutionMetadata_GetOverrides{Call: c}
}
func (_m *TaskExecutionMetadata) OnGetOverridesMatch(matchers ...interface{}) *TaskExecutionMetadata_GetOverrides {
	c := _m.On("GetOverrides", matchers...)
	return &TaskExecutionMetadata_GetOverrides{Call: c}
}

// GetOverrides provides a mock function with given fields:
func (_m *TaskExecutionMetadata) GetOverrides() core.TaskOverrides {
	ret := _m.Called()

	var r0 core.TaskOverrides
	if rf, ok := ret.Get(0).(func() core.TaskOverrides); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(core.TaskOverrides)
		}
	}

	return r0
}

type TaskExecutionMetadata_GetOwnerID struct {
	*mock.Call
}

func (_m TaskExecutionMetadata_GetOwnerID) Return(_a0 types.NamespacedName) *TaskExecutionMetadata_GetOwnerID {
	return &TaskExecutionMetadata_GetOwnerID{Call: _m.Call.Return(_a0)}
}

func (_m *TaskExecutionMetadata) OnGetOwnerID() *TaskExecutionMetadata_GetOwnerID {
	c := _m.On("GetOwnerID")
	return &TaskExecutionMetadata_GetOwnerID{Call: c}
}
func (_m *TaskExecutionMetadata) OnGetOwnerIDMatch(matchers ...interface{}) *TaskExecutionMetadata_GetOwnerID {
	c := _m.On("GetOwnerID", matchers...)
	return &TaskExecutionMetadata_GetOwnerID{Call: c}
}

// GetOwnerID provides a mock function with given fields:
func (_m *TaskExecutionMetadata) GetOwnerID() types.NamespacedName {
	ret := _m.Called()

	var r0 types.NamespacedName
	if rf, ok := ret.Get(0).(func() types.NamespacedName); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(types.NamespacedName)
	}

	return r0
}

type TaskExecutionMetadata_GetOwnerReference struct {
	*mock.Call
}

func (_m TaskExecutionMetadata_GetOwnerReference) Return(_a0 v1.OwnerReference) *TaskExecutionMetadata_GetOwnerReference {
	return &TaskExecutionMetadata_GetOwnerReference{Call: _m.Call.Return(_a0)}
}

func (_m *TaskExecutionMetadata) OnGetOwnerReference() *TaskExecutionMetadata_GetOwnerReference {
	c := _m.On("GetOwnerReference")
	return &TaskExecutionMetadata_GetOwnerReference{Call: c}
}
func (_m *TaskExecutionMetadata) OnGetOwnerReferenceMatch(matchers ...interface{}) *TaskExecutionMetadata_GetOwnerReference {
	c := _m.On("GetOwnerReference", matchers...)
	return &TaskExecutionMetadata_GetOwnerReference{Call: c}
}

// GetOwnerReference provides a mock function with given fields:
func (_m *TaskExecutionMetadata) GetOwnerReference() v1.OwnerReference {
	ret := _m.Called()

	var r0 v1.OwnerReference
	if rf, ok := ret.Get(0).(func() v1.OwnerReference); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(v1.OwnerReference)
	}

	return r0
}

type TaskExecutionMetadata_GetTaskExecutionID struct {
	*mock.Call
}

func (_m TaskExecutionMetadata_GetTaskExecutionID) Return(_a0 core.TaskExecutionID) *TaskExecutionMetadata_GetTaskExecutionID {
	return &TaskExecutionMetadata_GetTaskExecutionID{Call: _m.Call.Return(_a0)}
}

func (_m *TaskExecutionMetadata) OnGetTaskExecutionID() *TaskExecutionMetadata_GetTaskExecutionID {
	c := _m.On("GetTaskExecutionID")
	return &TaskExecutionMetadata_GetTaskExecutionID{Call: c}
}
func (_m *TaskExecutionMetadata) OnGetTaskExecutionIDMatch(matchers ...interface{}) *TaskExecutionMetadata_GetTaskExecutionID {
	c := _m.On("GetTaskExecutionID", matchers...)
	return &TaskExecutionMetadata_GetTaskExecutionID{Call: c}
}

// GetTaskExecutionID provides a mock function with given fields:
func (_m *TaskExecutionMetadata) GetTaskExecutionID() core.TaskExecutionID {
	ret := _m.Called()

	var r0 core.TaskExecutionID
	if rf, ok := ret.Get(0).(func() core.TaskExecutionID); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(core.TaskExecutionID)
		}
	}

	return r0
}
