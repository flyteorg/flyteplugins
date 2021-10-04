package flytek8s

import (
	"context"
	"testing"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core/template"
	mocks2 "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/io/mocks"
	"github.com/flyteorg/flytestdlib/storage"
	"github.com/stretchr/testify/mock"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

var zeroQuantity = resource.MustParse("0")

func TestAssignResource(t *testing.T) {
	t.Run("Leave valid requests and limits unchanged", func(t *testing.T) {
		res := assignResource(
			resource.MustParse("1"), resource.MustParse("2"),
			resource.MustParse("10"), resource.MustParse("20"))
		assert.True(t, res.request.Equal(resource.MustParse("1")))
		assert.True(t, res.limit.Equal(resource.MustParse("2")))
	})
	t.Run("Assign unset request from limit", func(t *testing.T) {
		res := assignResource(
			zeroQuantity, resource.MustParse("2"),
			resource.MustParse("10"), resource.MustParse("20"))
		assert.True(t, res.request.Equal(resource.MustParse("2")))
		assert.True(t, res.limit.Equal(resource.MustParse("2")))
	})
	t.Run("Assign unset limit from request", func(t *testing.T) {
		res := assignResource(
			resource.MustParse("2"), zeroQuantity,
			resource.MustParse("10"), resource.MustParse("20"))
		assert.True(t, res.request.Equal(resource.MustParse("2")))
		assert.True(t, res.limit.Equal(resource.MustParse("2")))
	})
	t.Run("Assign from platform defaults", func(t *testing.T) {
		res := assignResource(
			zeroQuantity, zeroQuantity,
			resource.MustParse("10"), resource.MustParse("20"))
		assert.True(t, res.request.Equal(resource.MustParse("10")))
		assert.True(t, res.limit.Equal(resource.MustParse("10")))
	})
	t.Run("Adjust limit when request > limit", func(t *testing.T) {
		res := assignResource(
			resource.MustParse("10"), resource.MustParse("2"),
			resource.MustParse("10"), resource.MustParse("20"))
		assert.True(t, res.request.Equal(resource.MustParse("10")))
		assert.True(t, res.limit.Equal(resource.MustParse("10")))
	})
	t.Run("Adjust limit > platformLimit", func(t *testing.T) {
		res := assignResource(
			resource.MustParse("1"), resource.MustParse("40"),
			resource.MustParse("10"), resource.MustParse("20"))
		assert.True(t, res.request.Equal(resource.MustParse("1")))
		assert.True(t, res.limit.Equal(resource.MustParse("20")))
	})
	t.Run("Adjust request, limit > platformLimit", func(t *testing.T) {
		res := assignResource(
			resource.MustParse("40"), resource.MustParse("50"),
			resource.MustParse("10"), resource.MustParse("20"))
		assert.True(t, res.request.Equal(resource.MustParse("20")))
		assert.True(t, res.limit.Equal(resource.MustParse("20")))
	})
}

func TestApplyResourceOverrides_OverrideCpu(t *testing.T) {
	platformRequirements := v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU: resource.MustParse("3"),
		},
		Limits: v1.ResourceList{
			v1.ResourceCPU: resource.MustParse("10"),
		},
	}
	cpuRequest := resource.MustParse("1")
	overrides := ApplyResourceOverrides(v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU: cpuRequest,
		},
	}, platformRequirements, assignIfUnset)
	assert.EqualValues(t, cpuRequest, overrides.Requests[v1.ResourceCPU])
	assert.EqualValues(t, cpuRequest, overrides.Limits[v1.ResourceCPU])

	cpuLimit := resource.MustParse("2")
	overrides = ApplyResourceOverrides(v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU: cpuRequest,
		},
		Limits: v1.ResourceList{
			v1.ResourceCPU: cpuLimit,
		},
	}, platformRequirements, assignIfUnset)
	assert.EqualValues(t, cpuRequest, overrides.Requests[v1.ResourceCPU])
	assert.EqualValues(t, cpuLimit, overrides.Limits[v1.ResourceCPU])

	// request equals limit if not set
	overrides = ApplyResourceOverrides(v1.ResourceRequirements{
		Limits: v1.ResourceList{
			v1.ResourceCPU: cpuLimit,
		},
	}, platformRequirements, assignIfUnset)
	assert.EqualValues(t, cpuLimit, overrides.Requests[v1.ResourceCPU])
	assert.EqualValues(t, cpuLimit, overrides.Limits[v1.ResourceCPU])
}

func TestApplyResourceOverrides_OverrideMemory(t *testing.T) {
	memoryRequest := resource.MustParse("1")
	platformRequirements := v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceMemory: resource.MustParse("3"),
		},
		Limits: v1.ResourceList{
			v1.ResourceMemory: resource.MustParse("10"),
		},
	}
	overrides := ApplyResourceOverrides(v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceMemory: memoryRequest,
		},
	}, platformRequirements, assignIfUnset)
	assert.EqualValues(t, memoryRequest, overrides.Requests[v1.ResourceMemory])
	assert.EqualValues(t, memoryRequest, overrides.Limits[v1.ResourceMemory])

	memoryLimit := resource.MustParse("2")
	overrides = ApplyResourceOverrides(v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceMemory: memoryRequest,
		},
		Limits: v1.ResourceList{
			v1.ResourceMemory: memoryLimit,
		},
	}, platformRequirements, assignIfUnset)
	assert.EqualValues(t, memoryRequest, overrides.Requests[v1.ResourceMemory])
	assert.EqualValues(t, memoryLimit, overrides.Limits[v1.ResourceMemory])

	// request equals limit if not set
	overrides = ApplyResourceOverrides(v1.ResourceRequirements{
		Limits: v1.ResourceList{
			v1.ResourceMemory: memoryLimit,
		},
	}, platformRequirements, assignIfUnset)
	assert.EqualValues(t, memoryLimit, overrides.Requests[v1.ResourceMemory])
	assert.EqualValues(t, memoryLimit, overrides.Limits[v1.ResourceMemory])
}

func TestApplyResourceOverrides_OverrideEphemeralStorage(t *testing.T) {
	ephemeralStorageRequest := resource.MustParse("1")
	overrides := ApplyResourceOverrides(v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceEphemeralStorage: ephemeralStorageRequest,
		},
	}, v1.ResourceRequirements{}, assignIfUnset)
	assert.EqualValues(t, ephemeralStorageRequest, overrides.Requests[v1.ResourceEphemeralStorage])
	assert.EqualValues(t, ephemeralStorageRequest, overrides.Limits[v1.ResourceEphemeralStorage])

	ephemeralStorageLimit := resource.MustParse("2")
	overrides = ApplyResourceOverrides(v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceEphemeralStorage: ephemeralStorageRequest,
		},
		Limits: v1.ResourceList{
			v1.ResourceEphemeralStorage: ephemeralStorageLimit,
		},
	}, v1.ResourceRequirements{}, assignIfUnset)
	assert.EqualValues(t, ephemeralStorageRequest, overrides.Requests[v1.ResourceEphemeralStorage])
	assert.EqualValues(t, ephemeralStorageLimit, overrides.Limits[v1.ResourceEphemeralStorage])

	// request equals limit if not set
	overrides = ApplyResourceOverrides(v1.ResourceRequirements{
		Limits: v1.ResourceList{
			v1.ResourceEphemeralStorage: ephemeralStorageLimit,
		},
	}, v1.ResourceRequirements{}, assignIfUnset)
	assert.EqualValues(t, ephemeralStorageLimit, overrides.Requests[v1.ResourceEphemeralStorage])
}

func TestApplyResourceOverrides_RemoveStorage(t *testing.T) {
	requestedResourceQuantity := resource.MustParse("1")
	overrides := ApplyResourceOverrides(v1.ResourceRequirements{
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
	}, v1.ResourceRequirements{}, assignIfUnset)
	assert.EqualValues(t, v1.ResourceList{
		v1.ResourceMemory:           requestedResourceQuantity,
		v1.ResourceCPU:              requestedResourceQuantity,
		v1.ResourceEphemeralStorage: requestedResourceQuantity,
	}, overrides.Requests)

	assert.EqualValues(t, v1.ResourceList{
		v1.ResourceMemory:           requestedResourceQuantity,
		v1.ResourceCPU:              requestedResourceQuantity,
		v1.ResourceEphemeralStorage: requestedResourceQuantity,
	}, overrides.Limits)
}

func TestApplyResourceOverrides_OverrideGpu(t *testing.T) {
	gpuRequest := resource.MustParse("1")
	overrides := ApplyResourceOverrides(v1.ResourceRequirements{
		Requests: v1.ResourceList{
			resourceGPU: gpuRequest,
		},
	}, v1.ResourceRequirements{}, assignIfUnset)
	assert.EqualValues(t, gpuRequest, overrides.Requests[ResourceNvidiaGPU])

	overrides = ApplyResourceOverrides(v1.ResourceRequirements{
		Limits: v1.ResourceList{
			resourceGPU: gpuRequest,
		},
	}, v1.ResourceRequirements{}, assignIfUnset)
	assert.EqualValues(t, gpuRequest, overrides.Limits[ResourceNvidiaGPU])
}

func TestMergeResources_EmptyIn(t *testing.T) {
	requestedResourceQuantity := resource.MustParse("1")
	expectedResources := v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceMemory:           requestedResourceQuantity,
			v1.ResourceCPU:              requestedResourceQuantity,
			v1.ResourceEphemeralStorage: requestedResourceQuantity,
		},
		Limits: v1.ResourceList{
			v1.ResourceStorage:          requestedResourceQuantity,
			v1.ResourceMemory:           requestedResourceQuantity,
			v1.ResourceEphemeralStorage: requestedResourceQuantity,
		},
	}
	outResources := expectedResources.DeepCopy()
	MergeResources(v1.ResourceRequirements{}, outResources)
	assert.EqualValues(t, *outResources, expectedResources)
}

func TestMergeResources_EmptyOut(t *testing.T) {
	requestedResourceQuantity := resource.MustParse("1")
	expectedResources := v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceMemory:           requestedResourceQuantity,
			v1.ResourceCPU:              requestedResourceQuantity,
			v1.ResourceEphemeralStorage: requestedResourceQuantity,
		},
		Limits: v1.ResourceList{
			v1.ResourceStorage:          requestedResourceQuantity,
			v1.ResourceMemory:           requestedResourceQuantity,
			v1.ResourceEphemeralStorage: requestedResourceQuantity,
		},
	}
	outResources := v1.ResourceRequirements{}
	MergeResources(expectedResources, &outResources)
	assert.EqualValues(t, outResources, expectedResources)
}

func TestMergeResources_PartialRequirements(t *testing.T) {
	requestedResourceQuantity := resource.MustParse("1")
	resourceList := v1.ResourceList{
		v1.ResourceMemory:           requestedResourceQuantity,
		v1.ResourceCPU:              requestedResourceQuantity,
		v1.ResourceEphemeralStorage: requestedResourceQuantity,
	}
	inResources := v1.ResourceRequirements{Requests: resourceList}
	outResources := v1.ResourceRequirements{Limits: resourceList}
	MergeResources(inResources, &outResources)
	assert.EqualValues(t, outResources, v1.ResourceRequirements{
		Requests: resourceList,
		Limits:   resourceList,
	})
}

func TestMergeResources_PartialResourceKeys(t *testing.T) {
	requestedResourceQuantity := resource.MustParse("1")
	resourceList1 := v1.ResourceList{
		v1.ResourceMemory:           requestedResourceQuantity,
		v1.ResourceEphemeralStorage: requestedResourceQuantity,
	}
	resourceList2 := v1.ResourceList{v1.ResourceCPU: requestedResourceQuantity}
	expectedResourceList := v1.ResourceList{
		v1.ResourceCPU:              requestedResourceQuantity,
		v1.ResourceMemory:           requestedResourceQuantity,
		v1.ResourceEphemeralStorage: requestedResourceQuantity,
	}
	inResources := v1.ResourceRequirements{
		Requests: resourceList1,
		Limits:   resourceList2,
	}
	outResources := v1.ResourceRequirements{
		Requests: resourceList2,
		Limits:   resourceList1,
	}
	MergeResources(inResources, &outResources)
	assert.EqualValues(t, outResources, v1.ResourceRequirements{
		Requests: expectedResourceList,
		Limits:   expectedResourceList,
	})
}

func TestToK8sContainer(t *testing.T) {
	taskContainer := &core.Container{
		Image: "myimage",
		Args: []string{
			"arg1",
			"arg2",
			"arg3",
		},
		Command: []string{
			"com1",
			"com2",
			"com3",
		},
		Env: []*core.KeyValuePair{
			{
				Key:   "k",
				Value: "v",
			},
		},
	}

	mockTaskExecMetadata := mocks.TaskExecutionMetadata{}
	mockTaskOverrides := mocks.TaskOverrides{}
	mockTaskOverrides.OnGetResources().Return(&v1.ResourceRequirements{
		Limits: v1.ResourceList{
			v1.ResourceEphemeralStorage: resource.MustParse("1024Mi"),
		},
	})
	mockTaskExecMetadata.OnGetOverrides().Return(&mockTaskOverrides)
	mockTaskExecutionID := mocks.TaskExecutionID{}
	mockTaskExecutionID.OnGetGeneratedName().Return("gen_name")
	mockTaskExecMetadata.OnGetTaskExecutionID().Return(&mockTaskExecutionID)

	templateParameters := template.Parameters{
		TaskExecMetadata: &mockTaskExecMetadata,
	}

	container, err := ToK8sContainer(context.TODO(), taskContainer, nil, templateParameters)
	assert.NoError(t, err)
	assert.Equal(t, container.Image, "myimage")
	assert.EqualValues(t, []string{
		"arg1",
		"arg2",
		"arg3",
	}, container.Args)
	assert.EqualValues(t, []string{
		"com1",
		"com2",
		"com3",
	}, container.Command)
	assert.EqualValues(t, []v1.EnvVar{
		{
			Name:  "k",
			Value: "v",
		},
	}, container.Env)
	errs := validation.IsDNS1123Label(container.Name)
	assert.Nil(t, errs)
}

func getTemplateParametersForTest(resourceRequirements, platformResources *v1.ResourceRequirements) template.Parameters {
	mockTaskExecMetadata := mocks.TaskExecutionMetadata{}
	mockTaskExecutionID := mocks.TaskExecutionID{}
	mockTaskExecutionID.OnGetGeneratedName().Return("gen_name")
	mockTaskExecutionID.OnGetID().Return(core.TaskExecutionIdentifier{
		TaskId: &core.Identifier{
			ResourceType: core.ResourceType_TASK,
			Project:      "p1",
			Domain:       "d1",
			Name:         "task_name",
			Version:      "v1",
		},
		NodeExecutionId: &core.NodeExecutionIdentifier{
			NodeId: "node_id",
			ExecutionId: &core.WorkflowExecutionIdentifier{
				Project: "p2",
				Domain:  "d2",
				Name:    "n2",
			},
		},
		RetryAttempt: 1,
	})
	mockTaskExecMetadata.OnGetTaskExecutionID().Return(&mockTaskExecutionID)

	mockOverrides := mocks.TaskOverrides{}
	mockOverrides.OnGetResources().Return(resourceRequirements)
	mockTaskExecMetadata.OnGetOverrides().Return(&mockOverrides)
	mockTaskExecMetadata.OnGetPlatformResources().Return(platformResources)

	mockInputReader := mocks2.InputReader{}
	mockInputPath := storage.DataReference("s3://input/path")
	mockInputReader.OnGetInputPath().Return(mockInputPath)
	mockInputReader.OnGetInputPrefixPath().Return(mockInputPath)
	mockInputReader.On("Get", mock.Anything).Return(nil, nil)

	mockOutputPath := mocks2.OutputFilePaths{}
	mockOutputPathPrefix := storage.DataReference("s3://output/path")
	mockOutputPath.OnGetRawOutputPrefix().Return(mockOutputPathPrefix)
	mockOutputPath.OnGetOutputPrefixPath().Return(mockOutputPathPrefix)

	return template.Parameters{
		TaskExecMetadata: &mockTaskExecMetadata,
		Inputs:           &mockInputReader,
		OutputPath:       &mockOutputPath,
	}
}

func TestAddFlyteCustomizationsToContainer(t *testing.T) {
	templateParameters := getTemplateParametersForTest(&v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceEphemeralStorage: resource.MustParse("1024Mi"),
		},
		Limits: v1.ResourceList{
			v1.ResourceEphemeralStorage: resource.MustParse("2048Mi"),
		},
	}, nil)
	container := &v1.Container{
		Command: []string{
			"{{ .Input }}",
		},
		Args: []string{
			"{{ .OutputPrefix }}",
		},
	}
	err := AddFlyteCustomizationsToContainer(context.TODO(), templateParameters, AssignResources, container)
	assert.NoError(t, err)
	assert.EqualValues(t, container.Args, []string{"s3://output/path"})
	assert.EqualValues(t, container.Command, []string{"s3://input/path"})
	assert.Len(t, container.Resources.Limits, 3)
	assert.Len(t, container.Resources.Requests, 3)
	assert.Len(t, container.Env, 12)
}

func TestAddFlyteCustomizationsToContainer_Resources(t *testing.T) {
	container := &v1.Container{
		Command: []string{
			"{{ .Input }}",
		},
		Args: []string{
			"{{ .OutputPrefix }}",
		},
		Resources: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("1"),
			},
			Limits: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("10"),
			},
		},
	}

	t.Run("merge requests/limits for pod tasks - primary container", func(t *testing.T) {
		templateParameters := getTemplateParametersForTest(&v1.ResourceRequirements{
			Requests: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("2"),
			},
		}, &v1.ResourceRequirements{
			Requests: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("2"),
			},
			Limits: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("20"),
			},
		})
		err := AddFlyteCustomizationsToContainer(context.TODO(), templateParameters, MergeExistingResources, container)
		assert.NoError(t, err)
		assert.True(t, container.Resources.Requests.Cpu().Equal(resource.MustParse("1")))
		assert.True(t, container.Resources.Limits.Cpu().Equal(resource.MustParse("10")))
		assert.True(t, container.Resources.Requests.Memory().Equal(resource.MustParse("2")))
		assert.True(t, container.Resources.Limits.Memory().Equal(resource.MustParse("2")))
	})
	t.Run("enforce merge requests/limits for pod tasks - values from task overrides", func(t *testing.T) {
		templateParameters := getTemplateParametersForTest(&v1.ResourceRequirements{
			Requests: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("2"),
			},
			Limits: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("200"),
			},
		}, &v1.ResourceRequirements{
			Requests: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("2"),
			},
			Limits: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("20"),
			},
		})
		err := AddFlyteCustomizationsToContainer(context.TODO(), templateParameters, MergeExistingResources, container)
		assert.NoError(t, err)
		assert.True(t, container.Resources.Requests.Cpu().Equal(resource.MustParse("1")))
		assert.True(t, container.Resources.Limits.Cpu().Equal(resource.MustParse("10")))
		assert.True(t, container.Resources.Requests.Memory().Equal(resource.MustParse("2")))
		assert.True(t, container.Resources.Limits.Memory().Equal(resource.MustParse("20")))
	})
	t.Run("enforce requests/limits for pod tasks - values from container", func(t *testing.T) {
		container := &v1.Container{
			Command: []string{
				"{{ .Input }}",
			},
			Args: []string{
				"{{ .OutputPrefix }}",
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("100"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("100"),
				},
			},
		}

		templateParameters := getTemplateParametersForTest(&v1.ResourceRequirements{}, &v1.ResourceRequirements{
			Requests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("1"),
				v1.ResourceMemory: resource.MustParse("2"),
			},
			Limits: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("10"),
				v1.ResourceMemory: resource.MustParse("20"),
			},
		})
		err := AddFlyteCustomizationsToContainer(context.TODO(), templateParameters, MergeExistingResources, container)
		assert.NoError(t, err)
		assert.True(t, container.Resources.Requests.Cpu().Equal(resource.MustParse("10")))
		assert.True(t, container.Resources.Limits.Cpu().Equal(resource.MustParse("10")))
		assert.True(t, container.Resources.Requests.Memory().Equal(resource.MustParse("2")))
		assert.True(t, container.Resources.Limits.Memory().Equal(resource.MustParse("2")))
	})
}

func TestAddFlyteCustomizationsToContainer_ValidateExistingResources(t *testing.T) {
	container := &v1.Container{
		Command: []string{
			"{{ .Input }}",
		},
		Args: []string{
			"{{ .OutputPrefix }}",
		},
		Resources: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("100"),
			},
			Limits: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("200"),
			},
		},
	}
	templateParameters := getTemplateParametersForTest(&v1.ResourceRequirements{}, &v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU: resource.MustParse("1"),
			v1.ResourceMemory: resource.MustParse("2"),
		},
		Limits: v1.ResourceList{
			v1.ResourceCPU: resource.MustParse("10"),
			v1.ResourceMemory: resource.MustParse("20"),
		},
	})
	err := AddFlyteCustomizationsToContainer(context.TODO(), templateParameters, ValidateExistingResources, container)
	assert.NoError(t, err)

	assert.True(t, container.Resources.Requests.Cpu().Equal(resource.MustParse("10")))
	assert.True(t, container.Resources.Limits.Cpu().Equal(resource.MustParse("10")))
}
