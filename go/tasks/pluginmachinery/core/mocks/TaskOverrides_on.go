package mocks

import (
	"github.com/stretchr/testify/mock"
	v1 "k8s.io/api/core/v1"
)

type GetConfigCall struct {
	*mock.Call
}

func (c *GetConfigCall) Return(
	ConfigMapvalue *v1.ConfigMap,
) *GetConfigCall {
	return &GetConfigCall{Call: c.Call.Return(
		ConfigMapvalue,
	)}
}

func (_m *TaskOverrides) OnGetConfigMatch(matchers ...interface{}) *GetConfigCall {
	c := _m.On("GetConfig", matchers)
	return &GetConfigCall{Call: c}
}

func (_m *TaskOverrides) OnGetConfig() *GetConfigCall {
	c := _m.On("GetConfig")
	return &GetConfigCall{Call: c}
}

type GetResourcesCall struct {
	*mock.Call
}

func (c *GetResourcesCall) Return(
	ResourceRequirementsvalue v1.ResourceRequirements,
) *GetResourcesCall {
	return &GetResourcesCall{Call: c.Call.Return(
		ResourceRequirementsvalue,
	)}
}

func (_m *TaskOverrides) OnGetResourcesMatch(matchers ...interface{}) *GetResourcesCall {
	c := _m.On("GetResources", matchers)
	return &GetResourcesCall{Call: c}
}

func (_m *TaskOverrides) OnGetResources() *GetResourcesCall {
	c := _m.On("GetResources")
	return &GetResourcesCall{Call: c}
}
