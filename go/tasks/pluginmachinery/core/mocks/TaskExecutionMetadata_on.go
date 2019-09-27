package mocks

import (
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/stretchr/testify/mock"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type GetAnnotationsCall struct {
	*mock.Call
}

func (c *GetAnnotationsCall) Return(
	mapVal map[string]string,
) *GetAnnotationsCall {
	return &GetAnnotationsCall{Call: c.Call.Return(
		mapVal,
	)}
}

func (_m *TaskExecutionMetadata) OnGetAnnotationsMatch(matchers ...interface{}) *GetAnnotationsCall {
	c := _m.On("GetAnnotations", matchers)
	return &GetAnnotationsCall{Call: c}
}

func (_m *TaskExecutionMetadata) OnGetAnnotations() *GetAnnotationsCall {
	c := _m.On("GetAnnotations")
	return &GetAnnotationsCall{Call: c}
}

type GetK8sServiceAccountCall struct {
	*mock.Call
}

func (c *GetK8sServiceAccountCall) Return(
	stringvalue string,
) *GetK8sServiceAccountCall {
	return &GetK8sServiceAccountCall{Call: c.Call.Return(
		stringvalue,
	)}
}

func (_m *TaskExecutionMetadata) OnGetK8sServiceAccountMatch(matchers ...interface{}) *GetK8sServiceAccountCall {
	c := _m.On("GetK8sServiceAccount", matchers)
	return &GetK8sServiceAccountCall{Call: c}
}

func (_m *TaskExecutionMetadata) OnGetK8sServiceAccount() *GetK8sServiceAccountCall {
	c := _m.On("GetK8sServiceAccount")
	return &GetK8sServiceAccountCall{Call: c}
}

type GetLabelsCall struct {
	*mock.Call
}

func (c *GetLabelsCall) Return(
	mapVal map[string]string,
) *GetLabelsCall {
	return &GetLabelsCall{Call: c.Call.Return(
		mapVal,
	)}
}

func (_m *TaskExecutionMetadata) OnGetLabelsMatch(matchers ...interface{}) *GetLabelsCall {
	c := _m.On("GetLabels", matchers)
	return &GetLabelsCall{Call: c}
}

func (_m *TaskExecutionMetadata) OnGetLabels() *GetLabelsCall {
	c := _m.On("GetLabels")
	return &GetLabelsCall{Call: c}
}

type GetNamespaceCall struct {
	*mock.Call
}

func (c *GetNamespaceCall) Return(
	stringvalue string,
) *GetNamespaceCall {
	return &GetNamespaceCall{Call: c.Call.Return(
		stringvalue,
	)}
}

func (_m *TaskExecutionMetadata) OnGetNamespaceMatch(matchers ...interface{}) *GetNamespaceCall {
	c := _m.On("GetNamespace", matchers)
	return &GetNamespaceCall{Call: c}
}

func (_m *TaskExecutionMetadata) OnGetNamespace() *GetNamespaceCall {
	c := _m.On("GetNamespace")
	return &GetNamespaceCall{Call: c}
}

type GetOverridesCall struct {
	*mock.Call
}

func (c *GetOverridesCall) Return(
	TaskOverridesvalue core.TaskOverrides,
) *GetOverridesCall {
	return &GetOverridesCall{Call: c.Call.Return(
		TaskOverridesvalue,
	)}
}

func (_m *TaskExecutionMetadata) OnGetOverridesMatch(matchers ...interface{}) *GetOverridesCall {
	c := _m.On("GetOverrides", matchers)
	return &GetOverridesCall{Call: c}
}

func (_m *TaskExecutionMetadata) OnGetOverrides() *GetOverridesCall {
	c := _m.On("GetOverrides")
	return &GetOverridesCall{Call: c}
}

type GetOwnerIDCall struct {
	*mock.Call
}

func (c *GetOwnerIDCall) Return(
	NamespacedNamevalue types.NamespacedName,
) *GetOwnerIDCall {
	return &GetOwnerIDCall{Call: c.Call.Return(
		NamespacedNamevalue,
	)}
}

func (_m *TaskExecutionMetadata) OnGetOwnerIDMatch(matchers ...interface{}) *GetOwnerIDCall {
	c := _m.On("GetOwnerID", matchers)
	return &GetOwnerIDCall{Call: c}
}

func (_m *TaskExecutionMetadata) OnGetOwnerID() *GetOwnerIDCall {
	c := _m.On("GetOwnerID")
	return &GetOwnerIDCall{Call: c}
}

type GetOwnerReferenceCall struct {
	*mock.Call
}

func (c *GetOwnerReferenceCall) Return(
	OwnerReferencevalue v1.OwnerReference,
) *GetOwnerReferenceCall {
	return &GetOwnerReferenceCall{Call: c.Call.Return(
		OwnerReferencevalue,
	)}
}

func (_m *TaskExecutionMetadata) OnGetOwnerReferenceMatch(matchers ...interface{}) *GetOwnerReferenceCall {
	c := _m.On("GetOwnerReference", matchers)
	return &GetOwnerReferenceCall{Call: c}
}

func (_m *TaskExecutionMetadata) OnGetOwnerReference() *GetOwnerReferenceCall {
	c := _m.On("GetOwnerReference")
	return &GetOwnerReferenceCall{Call: c}
}

type GetTaskExecutionIDCall struct {
	*mock.Call
}

func (c *GetTaskExecutionIDCall) Return(
	TaskExecutionIDvalue core.TaskExecutionID,
) *GetTaskExecutionIDCall {
	return &GetTaskExecutionIDCall{Call: c.Call.Return(
		TaskExecutionIDvalue,
	)}
}

func (_m *TaskExecutionMetadata) OnGetTaskExecutionIDMatch(matchers ...interface{}) *GetTaskExecutionIDCall {
	c := _m.On("GetTaskExecutionID", matchers)
	return &GetTaskExecutionIDCall{Call: c}
}

func (_m *TaskExecutionMetadata) OnGetTaskExecutionID() *GetTaskExecutionIDCall {
	c := _m.On("GetTaskExecutionID")
	return &GetTaskExecutionIDCall{Call: c}
}
