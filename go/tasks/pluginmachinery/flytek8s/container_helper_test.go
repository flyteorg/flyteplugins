package flytek8s

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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

	// request equals limit if not set
	overrides = ApplyResourceOverrides(context.Background(), v1.ResourceRequirements{
		Limits: v1.ResourceList{
			v1.ResourceCPU: cpuLimit,
		},
	})
	assert.EqualValues(t, cpuLimit, overrides.Limits[v1.ResourceCPU])

	// request equals limit if not set
	overrides = ApplyResourceOverrides(context.Background(), v1.ResourceRequirements{
		Limits: v1.ResourceList{
			v1.ResourceCPU: cpuLimit,
		},
	})
	assert.EqualValues(t, cpuLimit, overrides.Requests[v1.ResourceCPU])
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

	// request equals limit if not set
	cpuLimit := resource.MustParse("2")
	overrides = ApplyResourceOverrides(context.Background(), v1.ResourceRequirements{
		Limits: v1.ResourceList{
			v1.ResourceMemory: memoryLimit,
			v1.ResourceCPU:    cpuLimit,
		},
	})
	assert.EqualValues(t, memoryLimit, overrides.Requests[v1.ResourceMemory])
	assert.EqualValues(t, cpuLimit, overrides.Requests[v1.ResourceCPU])
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
		v1.ResourceCPU:    requestedResourceQuantity,
	}, overrides.Requests)

	assert.EqualValues(t, v1.ResourceList{
		v1.ResourceMemory: requestedResourceQuantity,
		v1.ResourceCPU:    requestedResourceQuantity,
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
