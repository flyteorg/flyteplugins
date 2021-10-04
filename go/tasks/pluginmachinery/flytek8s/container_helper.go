package flytek8s

import (
	"context"
	"fmt"
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

type assignedResource struct {
	request resource.Quantity
	limit   resource.Quantity
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

func assignResource(request, limit, platformRequest, platformLimit resource.Quantity) assignedResource {
	if request.IsZero() {
		if !limit.IsZero() {
			request = limit
		} else {
			request = platformRequest
		}
	}
	if request.Cmp(platformLimit) == 1 && !platformLimit.IsZero() {
		// Adjust the request downwards to not exceed the max limit if it's set.
		request = platformLimit
	}

	if limit.IsZero() {
		limit = request
	}

	if limit.Cmp(platformLimit) == 1 && !platformLimit.IsZero() {
		println(fmt.Sprintf("platform limit [%+v]", platformLimit.String()))
		// Adjust the limit downwards to not exceed the max limit if it's set.
		limit = platformLimit
	}

	if request.Cmp(limit) == 1 {
		// The limit should always be greater than or equal to the request
		limit = request
	}

	return assignedResource{
		request: request,
		limit:   limit,
	}
}

func ApplyResourceOverrides(resources, platformResources v1.ResourceRequirements) *v1.ResourceRequirements {
	var finalizedResources = &v1.ResourceRequirements{
		Requests: make(v1.ResourceList),
		Limits:   make(v1.ResourceList),
	}

	if len(resources.Requests) == 0 {
		resources.Requests = make(v1.ResourceList)
	}

	if len(resources.Limits) == 0 {
		resources.Limits = make(v1.ResourceList)
	}

	platformResources = resolvePlatformDefaults(platformResources, config.GetK8sPluginConfig().DefaultCPURequest,
		config.GetK8sPluginConfig().DefaultMemoryRequest)

	cpu := assignResource(resources.Requests[v1.ResourceCPU], resources.Limits[v1.ResourceCPU],
		platformResources.Requests[v1.ResourceCPU], platformResources.Limits[v1.ResourceCPU])
	finalizedResources.Requests[v1.ResourceCPU] = cpu.request
	finalizedResources.Limits[v1.ResourceCPU] = cpu.limit

	memory := assignResource(resources.Requests[v1.ResourceMemory], resources.Limits[v1.ResourceMemory],
		platformResources.Requests[v1.ResourceMemory], platformResources.Limits[v1.ResourceMemory])
	finalizedResources.Requests[v1.ResourceMemory] = memory.request
	finalizedResources.Limits[v1.ResourceMemory] = memory.limit

	_, ephemeralStorageRequested := resources.Requests[v1.ResourceEphemeralStorage]
	_, ephemeralStorageLimited := resources.Limits[v1.ResourceEphemeralStorage]

	if ephemeralStorageRequested || ephemeralStorageLimited {
		ephemeralStorage := assignResource(resources.Requests[v1.ResourceEphemeralStorage], resources.Limits[v1.ResourceEphemeralStorage],
			platformResources.Requests[v1.ResourceEphemeralStorage], platformResources.Limits[v1.ResourceEphemeralStorage])
		finalizedResources.Requests[v1.ResourceEphemeralStorage] = ephemeralStorage.request
		finalizedResources.Limits[v1.ResourceEphemeralStorage] = ephemeralStorage.limit
	}

	// TODO: Make configurable. 1/15/2019 Flyte Cluster doesn't support setting storage requests/limits.
	// https://github.com/kubernetes/enhancements/issues/362

	// Override GPU
	if res, found := resources.Requests[resourceGPU]; found {
		finalizedResources.Requests[ResourceNvidiaGPU] = res
	} else if res, found := resources.Requests[ResourceNvidiaGPU]; found {
		finalizedResources.Requests[ResourceNvidiaGPU] = res
	}
	if res, found := resources.Limits[resourceGPU]; found {
		finalizedResources.Limits[ResourceNvidiaGPU] = res
	} else if res, found := resources.Limits[ResourceNvidiaGPU]; found {
		finalizedResources.Limits[ResourceNvidiaGPU] = res
	}

	return finalizedResources
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
	AssignResources ResourceCustomizationMode = iota
	MergeExistingResources
	LeaveResourcesUnmodified
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
		logger.Warnf(ctx, "Using mode [%+v]", mode)
		switch mode {
		case AssignResources:
			if res = ApplyResourceOverrides(*res, *platformResources); res != nil {
				container.Resources = *res
			}
		case MergeExistingResources:
			logger.Warnf(ctx, "merging resources [%+v] and [%+v]", *res, container.Resources)
			MergeResources(*res, &container.Resources)
			logger.Warnf(ctx, "merged resources [%+v]", container.Resources)
			logger.Warnf(ctx, "platform resources (memory) request: [%+v], limit [%+v]", platformResources.Requests.Cpu().String(), platformResources.Limits.Cpu().String())
			container.Resources = *ApplyResourceOverrides(container.Resources, *platformResources)
			logger.Warnf(ctx, "overridden resources [%+v]", container.Resources)
		case LeaveResourcesUnmodified:
		}
	}
	return nil
}
