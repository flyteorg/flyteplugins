package flytek8s

import (
	"context"

	"github.com/flyteorg/flytestdlib/logger"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core/template"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/rand"

	"github.com/flyteorg/flyteplugins/go/tasks/errors"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/flytek8s/config"
)

const resourceGPU = "gpu"

// ResourceNvidiaGPU is the name of the Nvidia GPU resource.
// Copied from: k8s.io/autoscaler/cluster-autoscaler/utils/gpu/gpu.go
const ResourceNvidiaGPU = "nvidia.com/gpu"

// Specifies whether resource resolution should assign unset resource requests or limits from platform defaults
// or existing container values.
const assignIfUnset = true

func MergeResources(in v1.ResourceRequirements, out *v1.ResourceRequirements) {
	if out.Limits == nil {
		out.Limits = in.Limits
	} else if in.Limits != nil {
		for key, val := range in.Limits {
			out.Limits[key] = val
		}
	}
	if out.Requests == nil {
		out.Requests = in.Requests
	} else if in.Requests != nil {
		for key, val := range in.Requests {
			out.Requests[key] = val
		}
	}
}

type AssignedResource struct {
	Request resource.Quantity
	Limit   resource.Quantity
}

func resolvePlatformDefaults(platformResources v1.ResourceRequirements, configCPU, configMemory resource.Quantity) v1.ResourceRequirements {
	if len(platformResources.Requests) == 0 {
		platformResources.Requests = make(v1.ResourceList)
	}
	if _, ok := platformResources.Requests[v1.ResourceCPU]; !ok {
		platformResources.Requests[v1.ResourceCPU] = configCPU
	}
	if _, ok := platformResources.Requests[v1.ResourceMemory]; !ok {
		platformResources.Requests[v1.ResourceMemory] = configMemory
	}

	if len(platformResources.Limits) == 0 {
		platformResources.Limits = make(v1.ResourceList)
	}
	return platformResources
}

// Validates resources conform to platform limits and assigns defaults for Request and Limit values by:
// using the Request when the Limit is unset, and vice versa.
func AssignResource(request, limit, platformRequest, platformLimit resource.Quantity) AssignedResource {
	if request.IsZero() {
		if !limit.IsZero() {
			request = limit
		} else {
			request = platformRequest
		}
	}
	if !platformLimit.IsZero() && request.Cmp(platformLimit) == 1 {
		// Adjust the Request downwards to not exceed the max Limit if it's set.
		request = platformLimit
	}

	if limit.IsZero() {
		limit = request
	}

	if !platformLimit.IsZero() && limit.Cmp(platformLimit) == 1 {
		// Adjust the Limit downwards to not exceed the max Limit if it's set.
		limit = platformLimit
	}

	if request.Cmp(limit) == 1 {
		// The Limit should always be greater than or equal to the Request
		limit = request
	}

	return AssignedResource{
		Request: request,
		Limit:   limit,
	}
}

// Doesn't assign resources unless they  need to be adjusted downwards
func validateResource(request, limit, platformLimit resource.Quantity) AssignedResource {
	if !request.IsZero() && !platformLimit.IsZero() && request.Cmp(platformLimit) == 1 {
		request = platformLimit
	}

	if !limit.IsZero() && !platformLimit.IsZero() && limit.Cmp(platformLimit) == 1 {
		limit = platformLimit
	}

	if request.Cmp(limit) == 1 {
		request = limit
	}

	return AssignedResource{
		Request: request,
		Limit:   limit,
	}
}

// This function handles resource resolution, allocation and validation. Primarily, it ensures that container resources
// do not exceed defined platformResource limits and in the case of assignIfUnset, ensures that limits and requests are
// sensibly set for resources of all types.
// Furthermore, this function handles some clean-up such as converting GPU resources to the recognized Nvidia gpu
// resource name and deleting unsupported Storage-type resources.
func ApplyResourceOverrides(resources, platformResources v1.ResourceRequirements, assignIfUnset bool) v1.ResourceRequirements {
	if len(resources.Requests) == 0 {
		resources.Requests = make(v1.ResourceList)
	}

	if len(resources.Limits) == 0 {
		resources.Limits = make(v1.ResourceList)
	}

	// As a fallback, in the case the Flyte workflow object does not have platformResource defaults set, the defaults
	// come from the plugin config.
	platformResources = resolvePlatformDefaults(platformResources, config.GetK8sPluginConfig().DefaultCPURequest,
		config.GetK8sPluginConfig().DefaultMemoryRequest)

	var cpu AssignedResource
	if assignIfUnset {
		cpu = AssignResource(resources.Requests[v1.ResourceCPU], resources.Limits[v1.ResourceCPU],
			platformResources.Requests[v1.ResourceCPU], platformResources.Limits[v1.ResourceCPU])
	} else {
		cpu = validateResource(resources.Requests[v1.ResourceCPU], resources.Limits[v1.ResourceCPU],
			platformResources.Limits[v1.ResourceCPU])
	}
	resources.Requests[v1.ResourceCPU] = cpu.Request
	resources.Limits[v1.ResourceCPU] = cpu.Limit

	var memory AssignedResource
	if assignIfUnset {
		memory = AssignResource(resources.Requests[v1.ResourceMemory], resources.Limits[v1.ResourceMemory],
			platformResources.Requests[v1.ResourceMemory], platformResources.Limits[v1.ResourceMemory])
	} else {
		memory = validateResource(resources.Requests[v1.ResourceMemory], resources.Limits[v1.ResourceMemory],
			platformResources.Limits[v1.ResourceMemory])
	}
	resources.Requests[v1.ResourceMemory] = memory.Request
	resources.Limits[v1.ResourceMemory] = memory.Limit

	_, ephemeralStorageRequested := resources.Requests[v1.ResourceEphemeralStorage]
	_, ephemeralStorageLimited := resources.Limits[v1.ResourceEphemeralStorage]

	if ephemeralStorageRequested || ephemeralStorageLimited {
		var ephemeralStorage AssignedResource
		if assignIfUnset {
			ephemeralStorage = AssignResource(resources.Requests[v1.ResourceEphemeralStorage], resources.Limits[v1.ResourceEphemeralStorage],
				platformResources.Requests[v1.ResourceEphemeralStorage], platformResources.Limits[v1.ResourceEphemeralStorage])
		} else {
			ephemeralStorage = validateResource(resources.Requests[v1.ResourceEphemeralStorage], resources.Limits[v1.ResourceEphemeralStorage],
				platformResources.Limits[v1.ResourceEphemeralStorage])
		}

		resources.Requests[v1.ResourceEphemeralStorage] = ephemeralStorage.Request
		resources.Limits[v1.ResourceEphemeralStorage] = ephemeralStorage.Limit
	}

	// TODO: Make configurable. 1/15/2019 Flyte Cluster doesn't support setting storage requests/limits.
	// https://github.com/kubernetes/enhancements/issues/362
	delete(resources.Requests, v1.ResourceStorage)
	delete(resources.Limits, v1.ResourceStorage)

	shouldAdjustGPU := false
	_, gpuRequested := resources.Requests[ResourceNvidiaGPU]
	_, gpuLimited := resources.Limits[ResourceNvidiaGPU]
	if gpuRequested || gpuLimited {
		shouldAdjustGPU = true
	}
	// Override GPU
	if res, found := resources.Requests[resourceGPU]; found {
		resources.Requests[ResourceNvidiaGPU] = res
		delete(resources.Requests, resourceGPU)
		shouldAdjustGPU = true
	}
	if res, found := resources.Limits[resourceGPU]; found {
		resources.Limits[ResourceNvidiaGPU] = res
		delete(resources.Limits, resourceGPU)
		shouldAdjustGPU = true
	}
	if shouldAdjustGPU {
		var gpu AssignedResource
		if assignIfUnset {
			gpu = AssignResource(resources.Requests[ResourceNvidiaGPU], resources.Limits[ResourceNvidiaGPU],
				platformResources.Requests[resourceGPU], platformResources.Limits[resourceGPU])
		} else {
			gpu = validateResource(resources.Requests[ResourceNvidiaGPU], resources.Limits[ResourceNvidiaGPU],
				platformResources.Limits[resourceGPU])
		}

		resources.Requests[ResourceNvidiaGPU] = gpu.Request
		resources.Limits[ResourceNvidiaGPU] = gpu.Limit
	}

	return resources
}

// Transforms a task template target of type core.Container into a bare-bones kubernetes container, which can be further
// modified with flyte-specific customizations specified by various static and run-time attributes.
func ToK8sContainer(ctx context.Context, taskContainer *core.Container, iFace *core.TypedInterface, parameters template.Parameters) (*v1.Container, error) {
	// Perform preliminary validations
	if parameters.TaskExecMetadata.GetOverrides() == nil {
		return nil, errors.Errorf(errors.BadTaskSpecification, "platform/compiler error, overrides not set for task")
	}
	if parameters.TaskExecMetadata.GetOverrides() == nil || parameters.TaskExecMetadata.GetOverrides().GetResources() == nil {
		return nil, errors.Errorf(errors.BadTaskSpecification, "resource requirements not found for container task, required!")
	}
	// Make the container name the same as the pod name, unless it violates K8s naming conventions
	// Container names are subject to the DNS-1123 standard
	containerName := parameters.TaskExecMetadata.GetTaskExecutionID().GetGeneratedName()
	if errs := validation.IsDNS1123Label(containerName); len(errs) > 0 {
		containerName = rand.String(4)
	}
	container := &v1.Container{
		Name:                     containerName,
		Image:                    taskContainer.GetImage(),
		Args:                     taskContainer.GetArgs(),
		Command:                  taskContainer.GetCommand(),
		Env:                      ToK8sEnvVar(taskContainer.GetEnv()),
		TerminationMessagePolicy: v1.TerminationMessageFallbackToLogsOnError,
	}
	if err := AddCoPilotToContainer(ctx, config.GetK8sPluginConfig().CoPilot, container, iFace, taskContainer.DataConfig); err != nil {
		return nil, err
	}
	return container, nil
}

type ResourceCustomizationMode int

const (
	// Used for container tasks where resources are validated and assigned if necessary.
	AssignResources ResourceCustomizationMode = iota
	// Used for primary containers in pod tasks where container requests and limits are merged, validated and assigned
	// if necessary.
	MergeExistingResources
	// Used for secondary containers in pod tasks where requests and limits are only validated.
	ValidateExistingResources
)

// Takes a container definition which specifies how to run a Flyte task and fills in templated command and argument
// values, updates resources and decorates environment variables with platform and task-specific customizations.
func AddFlyteCustomizationsToContainer(ctx context.Context, parameters template.Parameters,
	mode ResourceCustomizationMode, container *v1.Container) error {
	modifiedCommand, err := template.Render(ctx, container.Command, parameters)
	if err != nil {
		return err
	}
	container.Command = modifiedCommand

	modifiedArgs, err := template.Render(ctx, container.Args, parameters)
	if err != nil {
		return err
	}
	container.Args = modifiedArgs

	container.Env = DecorateEnvVars(ctx, container.Env, parameters.TaskExecMetadata.GetTaskExecutionID())

	if parameters.TaskExecMetadata.GetOverrides() != nil && parameters.TaskExecMetadata.GetOverrides().GetResources() != nil {
		res := parameters.TaskExecMetadata.GetOverrides().GetResources()
		platformResources := parameters.TaskExecMetadata.GetPlatformResources()
		if platformResources == nil {
			platformResources = &v1.ResourceRequirements{}
		}
		switch mode {
		case AssignResources:
			logger.Warnf(ctx, "Applying resource overrides, resources [%+v]/[%+v]", res.Requests.Cpu(), res.Limits.Cpu())
			logger.Warnf(ctx, "and platform resources [%+v]/[%+v]", platformResources.Requests.Cpu(), platformResources.Limits.Cpu())
			container.Resources = ApplyResourceOverrides(*res, *platformResources, assignIfUnset)
		case MergeExistingResources:
			MergeResources(*res, &container.Resources)
			container.Resources = ApplyResourceOverrides(container.Resources, *platformResources, assignIfUnset)
		case ValidateExistingResources:
			container.Resources = ApplyResourceOverrides(container.Resources, *platformResources, !assignIfUnset)
		}
	}
	return nil
}
