package flytek8s

import (
	"context"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/logger"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/validation"

	pluginsCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/utils"
	"github.com/lyft/flyteplugins/go/tasks/v1/errors"
	"github.com/lyft/flyteplugins/go/tasks/v1/flytek8s/config"
)

const resourceGPU = "GPU"

// ResourceNvidiaGPU is the name of the Nvidia GPU resource.
// Copied from: k8s.io/autoscaler/cluster-autoscaler/utils/gpu/gpu.go
const ResourceNvidiaGPU = "nvidia.com/gpu"

func ApplyResourceOverrides(ctx context.Context, resources v1.ResourceRequirements) *v1.ResourceRequirements {
	// set memory and cpu to default if not provided by user.
	if len(resources.Requests) == 0 {
		resources.Requests = make(v1.ResourceList)
	}
	if _, found := resources.Requests[v1.ResourceCPU]; !found {
		resources.Requests[v1.ResourceCPU] = resource.MustParse(config.GetK8sPluginConfig().DefaultCpuRequest)
	}
	if _, found := resources.Requests[v1.ResourceMemory]; !found {
		resources.Requests[v1.ResourceMemory] = resource.MustParse(config.GetK8sPluginConfig().DefaultMemoryRequest)
	}

	if len(resources.Limits) == 0 {
		resources.Limits = make(v1.ResourceList)
	}
	if len(resources.Requests) == 0 {
		resources.Requests = make(v1.ResourceList)
	}
	if _, found := resources.Limits[v1.ResourceCPU]; !found {
		logger.Infof(ctx, "found cpu limit missing, setting limit to the requested value %v", resources.Requests[v1.ResourceCPU])
		resources.Limits[v1.ResourceCPU] = resources.Requests[v1.ResourceCPU]
	}
	if _, found := resources.Limits[v1.ResourceMemory]; !found {
		logger.Infof(ctx, "found memory limit missing, setting limit to the requested value %v", resources.Requests[v1.ResourceMemory])
		resources.Limits[v1.ResourceMemory] = resources.Requests[v1.ResourceMemory]
	}

	// TODO: Make configurable. 1/15/2019 Flyte Cluster doesn't support setting storage requests/limits.
	// https://github.com/kubernetes/enhancements/issues/362
	delete(resources.Requests, v1.ResourceStorage)
	delete(resources.Requests, v1.ResourceEphemeralStorage)

	delete(resources.Limits, v1.ResourceStorage)
	delete(resources.Limits, v1.ResourceEphemeralStorage)

	// Override GPU
	if res, found := resources.Requests[resourceGPU]; found {
		resources.Requests[ResourceNvidiaGPU] = res
		delete(resources.Requests, resourceGPU)
	}
	if res, found := resources.Limits[resourceGPU]; found {
		resources.Limits[ResourceNvidiaGPU] = res
		delete(resources.Requests, resourceGPU)
	}

	return &resources
}

// Transforms the task container definition to a core kubernetes container.
func ToK8sContainer(taskCtx pluginsCore.TaskExecutionMetadata, taskContainer *core.Container) (
	*v1.Container, error) {
	// Make the container name the same as the pod name, unless it violates K8s naming conventions
	// Container names are subject to the DNS-1123 standard
	containerName := taskCtx.GetTaskExecutionID().GetGeneratedName()
	if errs := validation.IsDNS1123Label(containerName); len(errs) > 0 {
		containerName = rand.String(4)
	}
	return &v1.Container{
		Name:    containerName,
		Image:   taskContainer.GetImage(),
		Args:    taskContainer.GetArgs(),
		Command: taskContainer.GetCommand(),
		Env:     ToK8sEnvVar(taskContainer.GetEnv()),
	}, nil
}

func mergeResourceLists(a, b v1.ResourceList) v1.ResourceList {
	if a == nil {
		a = make(v1.ResourceList)
	}
	if b != nil {
		for name, quantity := range b {
			a[name] = quantity
		}
	}
	return a
}

// Takes a raw kubernetes container and modifies it so that it is suitable to run on Flyte.
// These changes include substituting command line args, modifying resources according to Flyte platform constraints
// and updating env vars.
func AddFlyteModificationsForContainer(ctx context.Context, taskCtx pluginsCore.TaskExecutionContext,
	container *v1.Container) error {
	inputs, err := taskCtx.InputReader().Get(ctx)
	if err != nil {
		return err
	}

	modifiedCommand, err := utils.ReplaceTemplateCommandArgs(ctx,
		container.Command,
		utils.CommandLineTemplateArgs{
			Input:        taskCtx.InputReader().GetInputPath().String(),
			OutputPrefix: taskCtx.OutputWriter().GetOutputPrefixPath().String(),
			Inputs:       utils.LiteralMapToTemplateArgs(ctx, inputs),
		})

	if err != nil {
		return err
	}
	container.Command = modifiedCommand

	modifiedArgs, err := utils.ReplaceTemplateCommandArgs(ctx,
		container.Args,
		utils.CommandLineTemplateArgs{
			Input:        taskCtx.InputReader().GetInputPath().String(),
			OutputPrefix: taskCtx.OutputWriter().GetOutputPrefixPath().String(),
			Inputs:       utils.LiteralMapToTemplateArgs(ctx, inputs),
		})

	if err != nil {
		return err
	}
	container.Args = modifiedArgs

	if taskCtx.TaskExecutionMetadata().GetOverrides() == nil || taskCtx.TaskExecutionMetadata().GetOverrides().GetResources() == nil {
		return errors.Errorf(errors.BadTaskSpecification, "resource requirements not found for container task, required!")
	}
	resourceRequirements := ApplyResourceOverrides(ctx, *(taskCtx.TaskExecutionMetadata().GetOverrides().GetResources()))
	container.Resources.Limits = mergeResourceLists(container.Resources.Limits, resourceRequirements.Limits)
	container.Resources.Requests = mergeResourceLists(container.Resources.Requests, resourceRequirements.Requests)

	container.Env = DecorateEnvVars(ctx, container.Env, taskCtx.TaskExecutionMetadata().GetTaskExecutionID())
	return nil
}