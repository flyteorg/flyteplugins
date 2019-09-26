package flytek8s

import (
	"context"
	"github.com/golang/protobuf/proto"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/storage"
	"github.com/stretchr/testify/mock"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	pluginsCoreMock "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	pluginsIOMock "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io/mocks"
)

func TestApplyResourceOverrides_OverrideCpu(t *testing.T) {
	cpuRequest := resource.MustParse("1")
	overrides := ApplyResourceOverrides(context.Background(), v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU: cpuRequest,
		},
	})
	assert.EqualValues(t, cpuRequest, overrides.Requests[v1.ResourceCPU])
	assert.EqualValues(t, cpuRequest, overrides.Limits[v1.ResourceCPU])

	cpuLimit := resource.MustParse("2")
	overrides = ApplyResourceOverrides(context.Background(), v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU: cpuRequest,
		},
		Limits: v1.ResourceList{
			v1.ResourceCPU: cpuLimit,
		},
	})
	assert.EqualValues(t, cpuRequest, overrides.Requests[v1.ResourceCPU])
	assert.EqualValues(t, cpuLimit, overrides.Limits[v1.ResourceCPU])
}

func TestApplyResourceOverrides_OverrideMemory(t *testing.T) {
	memoryRequest := resource.MustParse("1")
	overrides := ApplyResourceOverrides(context.Background(), v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceMemory: memoryRequest,
		},
	})
	assert.EqualValues(t, memoryRequest, overrides.Requests[v1.ResourceMemory])
	assert.EqualValues(t, memoryRequest, overrides.Limits[v1.ResourceMemory])

	memoryLimit := resource.MustParse("2")
	overrides = ApplyResourceOverrides(context.Background(), v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceMemory: memoryRequest,
		},
		Limits: v1.ResourceList{
			v1.ResourceMemory: memoryLimit,
		},
	})
	assert.EqualValues(t, memoryRequest, overrides.Requests[v1.ResourceMemory])
	assert.EqualValues(t, memoryLimit, overrides.Limits[v1.ResourceMemory])
}

func TestApplyResourceOverrides_RemoveStorage(t *testing.T) {
	requestedResourceQuantity := resource.MustParse("1")
	overrides := ApplyResourceOverrides(context.Background(), v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceStorage:          requestedResourceQuantity,
			v1.ResourceMemory:           requestedResourceQuantity,
			v1.ResourceCPU:              requestedResourceQuantity,
			v1.ResourceEphemeralStorage: requestedResourceQuantity,
		},
		Limits: v1.ResourceList{
			v1.ResourceStorage:          requestedResourceQuantity,
			v1.ResourceMemory:           requestedResourceQuantity,
			v1.ResourceEphemeralStorage: requestedResourceQuantity,
		},
	})
	assert.EqualValues(t, v1.ResourceList{
		v1.ResourceMemory: requestedResourceQuantity,
		v1.ResourceCPU: requestedResourceQuantity,
	}, overrides.Requests)

	assert.EqualValues(t, v1.ResourceList{
		v1.ResourceMemory: requestedResourceQuantity,
		v1.ResourceCPU: requestedResourceQuantity,
	}, overrides.Limits)
}

func TestApplyResourceOverrides_OverrideGpu(t *testing.T) {
	gpuRequest := resource.MustParse("1")
	overrides := ApplyResourceOverrides(context.Background(), v1.ResourceRequirements{
		Requests: v1.ResourceList{
			resourceGPU: gpuRequest,
		},
	})
	assert.EqualValues(t, gpuRequest, overrides.Requests[ResourceNvidiaGPU])

	overrides = ApplyResourceOverrides(context.Background(), v1.ResourceRequirements{
		Limits: v1.ResourceList{
			resourceGPU: gpuRequest,
		},
	})
	assert.EqualValues(t, gpuRequest, overrides.Limits[ResourceNvidiaGPU])
}

func TestToK8sContainer(t *testing.T) {
	mockTaskCtx := pluginsCoreMock.TaskExecutionMetadata{}
	taskExecutionId := pluginsCoreMock.TaskExecutionID{}
	taskName := "taskname"
	taskExecutionId.On("GetGeneratedName").Return(taskName)
	mockTaskCtx.On("GetTaskExecutionID").Return(&taskExecutionId)

	taskContainer := core.Container{
		Image:                "image",
		Command:              []string{"command1", "command2"},
		Args:                 []string{"arg1", "arg2", "arg3"},
		Env:                  []*core.KeyValuePair{
			{
				Key: "FOO",
				Value: "bar",
			},
		},
	}
	container, err := ToK8sContainer(&mockTaskCtx, &taskContainer)
	assert.Nil(t, err)
	assert.Equal(t, taskName, container.Name)
	assert.Equal(t, taskContainer.Image, container.Image)
	assert.Equal(t, taskContainer.Command, container.Command)
	assert.Equal(t, taskContainer.Args, container.Args)
	assert.EqualValues(t, []v1.EnvVar{
		{
			Name: "FOO",
			Value: "bar",
		},
	}, container.Env)
}

func TestToK8sContainer_InvalidTaskName(t *testing.T) {
	mockTaskCtx := pluginsCoreMock.TaskExecutionMetadata{}
	taskExecutionId := pluginsCoreMock.TaskExecutionID{}
	taskName := "%invalid+!!!"
	taskExecutionId.On("GetGeneratedName").Return(taskName)
	mockTaskCtx.On("GetTaskExecutionID").Return(&taskExecutionId)
	container, err := ToK8sContainer(&mockTaskCtx, &core.Container{})
	assert.Nil(t, err)
	assert.Len(t, container.Name, 4)

	taskName = "way2long-way2long-way2long-way2long-way2long-way2long-way2long-way2long"
	taskExecutionId.On("GetGeneratedName").Return(taskName)
	mockTaskCtx.On("GetTaskExecutionID").Return(&taskExecutionId)
	container, err = ToK8sContainer(&mockTaskCtx, &core.Container{})
	assert.Nil(t, err)
	assert.Len(t, container.Name, 4)
}

func TestAddFlyteModificationsForContainer(t *testing.T) {
	taskCtx := &pluginsCoreMock.TaskExecutionContext{}
	inputReader := &pluginsIOMock.InputReader{}
	inputReader.On("GetInputPath").Return(storage.DataReference("/data/inputs.pb"))
	inputReader.On("Get", mock.Anything).Return(&core.LiteralMap{}, nil)
	taskCtx.On("InputReader").Return(inputReader)

	outputReader := &pluginsIOMock.OutputWriter{}
	outputReader.On("GetOutputPath").Return(storage.DataReference("/data/outputs.pb"))
	outputReader.On("GetOutputPrefixPath").Return(storage.DataReference("/data/"))
	taskCtx.On("OutputWriter").Return(outputReader)

	taskExecutionMetadata := &pluginsCoreMock.TaskExecutionMetadata{}
	overrides := &pluginsCoreMock.TaskOverrides{}
	requestedResourceQuantity := resource.MustParse("1")
	overrides.On("GetResources").Return(&v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceStorage:          requestedResourceQuantity,
			v1.ResourceMemory:           requestedResourceQuantity,
			v1.ResourceCPU:              requestedResourceQuantity,
			v1.ResourceEphemeralStorage: requestedResourceQuantity,
		},
		Limits: v1.ResourceList{
			v1.ResourceStorage:          requestedResourceQuantity,
			v1.ResourceMemory:           requestedResourceQuantity,
			v1.ResourceEphemeralStorage: requestedResourceQuantity,
		},
	})
	taskExecutionMetadata.On("GetOverrides").Return(overrides)

	taskExecutionId := pluginsCoreMock.TaskExecutionID{}
	taskExecutionId.On("GetID").Return(core.TaskExecutionIdentifier{
		NodeExecutionId: &core.NodeExecutionIdentifier{
			ExecutionId: &core.WorkflowExecutionIdentifier{
				Project:              "proj",
				Domain:               "domain",
				Name:                 "name",
			},
		},
		TaskId: &core.Identifier{
			Project:              "proj",
			Domain:               "domain",
		},
	})
	taskExecutionMetadata.On("GetTaskExecutionID").Return(&taskExecutionId)

	taskCtx.On("TaskExecutionMetadata").Return(taskExecutionMetadata)

	overriddenResourceQuantity := resource.MustParse("3")
	container := v1.Container{
		Command:                  []string{"{{$input}}"},
		Args:                     []string{"{{ .Input }}"},
		Env:                      []v1.EnvVar{
			{
				Name: "FOO",
				Value: "bar",
			},
		},
		Resources: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				v1.ResourceStorage:          overriddenResourceQuantity,
				v1.ResourceMemory:           overriddenResourceQuantity,
				v1.ResourceCPU:              overriddenResourceQuantity,
				v1.ResourceEphemeralStorage: overriddenResourceQuantity,
			},
			Limits: v1.ResourceList{
				v1.ResourceStorage:          overriddenResourceQuantity,
				v1.ResourceMemory:           overriddenResourceQuantity,
				v1.ResourceEphemeralStorage: overriddenResourceQuantity,
			},
		},
	}
	err := AddFlyteModificationsForContainer(context.TODO(), taskCtx, &container)
	assert.Nil(t, err)
	assert.EqualValues(t, []string{"/data/inputs.pb"}, container.Args)
	assert.EqualValues(t, []string{"/data/inputs.pb"}, container.Command)
	assert.True(t, proto.Equal(&v1.EnvVar{
			Name: "FOO",
			Value: "bar",
		}, &container.Env[0]))
}