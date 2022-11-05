package dask

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/scheme"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flyteplugins/go/tasks/errors"
	"github.com/flyteorg/flyteplugins/go/tasks/logs"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery"
	pluginsCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/k8s"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/tasklog"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/utils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	daskAPI "github.com/bstadlbauer/dask-k8s-operator-go-client/pkg/apis/kubernetes.dask.org/v1"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/plugins"
	v1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var resourceMapping = map[core.Resources_ResourceName]v1.ResourceName{
	core.Resources_CPU: v1.ResourceCPU,
	core.Resources_EPHEMERAL_STORAGE: v1.ResourceEphemeralStorage,
	core.Resources_GPU: resourceNvidiaGPU,
	core.Resources_MEMORY: v1.ResourceMemory,
	core.Resources_STORAGE: v1.ResourceEphemeralStorage,
}

const (
	daskTaskType        = "dask"
	resourceNvidiaGPU = "nvidia.com/gpu"
	KindDaskJob         = "DaskJob"
	PrimaryContainerKey = "primary_container_name"
)


func createResourceList(pb_resources []*core.Resources_ResourceEntry) (v1.ResourceList, error) {
	resourceList := v1.ResourceList{}
	for _, resourceEntry := range pb_resources {
		resourceName, ok := resourceMapping[resourceEntry.GetName()]
		if !ok {
			return nil, errors.Errorf(errors.BadTaskSpecification, fmt.Sprintf("Could not tranlate resource with name %s to k8s resource", resourceEntry.GetName()))
		}
		quantity, err := resource.ParseQuantity(resourceEntry.GetValue())
		if err != nil {
			return nil, errors.Errorf(errors.BadTaskSpecification, fmt.Sprintf("Could not parse %s as k8s resource", resourceEntry.GetValue()))
		}
		resourceList[resourceName] = quantity
	}
	return resourceList, nil
}


func convertProtobufResourcesToK8sResources(pb_resources *core.Resources) (*v1.ResourceRequirements, error) {
	var err error

	k8s_requests := v1.ResourceList{}
	if pb_resources.GetRequests() != nil {
		k8s_requests, err = createResourceList(pb_resources.GetRequests())
		if err != nil {
			return nil, err
		}
	}

	k8s_limits := v1.ResourceList{}
	if pb_resources.GetLimits() != nil {
		k8s_limits, err = createResourceList(pb_resources.GetLimits())
		if err != nil {
			return nil, err
		}
	}

	if len(k8s_requests) == 0 && len(k8s_limits) == 0 {
		return nil, nil
	}

	return &v1.ResourceRequirements{
		Requests: k8s_requests,
		Limits: k8s_limits,
	}, nil
}


type daskResourceHandler struct {
}

func (daskResourceHandler) BuildIdentityResource(_ context.Context, _ pluginsCore.TaskExecutionMetadata) (
	client.Object, error) {
		return &daskAPI.DaskJob{
			TypeMeta: metav1.TypeMeta{
				Kind:       KindDaskJob,
				APIVersion: daskAPI.SchemeGroupVersion.String(),
			},
		}, nil
}

func (p daskResourceHandler) BuildResource(ctx context.Context, taskCtx pluginsCore.TaskExecutionContext) (client.Object, error) {
	taskTemplate, err := taskCtx.TaskReader().Read(ctx)
	if err != nil {
		return nil, errors.Errorf(errors.BadTaskSpecification, "unable to fetch task specification [%v]", err.Error())
	} else if taskTemplate == nil {
		return nil, errors.Errorf(errors.BadTaskSpecification, "nil task specification")
	}
	executionMetadata := taskCtx.TaskExecutionMetadata()
	clusterName := executionMetadata.GetTaskExecutionID().GetGeneratedName()

	defaultContainer := taskTemplate.GetContainer()
	if defaultContainer == nil {
		return nil, errors.Errorf(errors.BadTaskSpecification, "task is missing a default container")
	}
	defaultImage := defaultContainer.GetImage()
	if defaultImage == "" {
		return nil, errors.Errorf(errors.BadTaskSpecification, "task is missing a default image")
	}
	
	var defaultResources *v1.ResourceRequirements
	if executionMetadata.GetOverrides() != nil && executionMetadata.GetOverrides().GetResources() != nil {
		defaultResources = executionMetadata.GetOverrides().GetResources()
	}

	daskJob := plugins.DaskJob{}
	err = utils.UnmarshalStruct(taskTemplate.GetCustom(), &daskJob)
	if err != nil {
		return nil, errors.Wrapf(errors.BadTaskSpecification, err, "invalid TaskSpecification [%v], failed to unmarshal", taskTemplate.GetCustom())
	}

	workerSpec, err := createWorkerSpec(*daskJob.Cluster, defaultImage, defaultResources)
	if err != nil {
		return nil, err
	}
	schedulerSpec, err := createSchedulerSpec(*daskJob.Cluster, clusterName, defaultImage, defaultResources)
	if err != nil {
		return nil, err
	}
	jobSpec, err := createJobSpec(*daskJob.JobPodSpec, *workerSpec, *schedulerSpec, defaultContainer.GetArgs(), defaultImage, defaultResources)
	if err != nil {
		return nil, err
	}

	job := &daskAPI.DaskJob{
		TypeMeta: metav1.TypeMeta{
			Kind:       KindDaskJob,
			APIVersion: daskAPI.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "will-be-overridden", // Will be overridden by Flyte to `taskName`
		},
		Spec: *jobSpec,
	}
	return job, nil
}

func createWorkerSpec(cluster plugins.DaskCluster, defaultImage string, defaultResources *v1.ResourceRequirements) (*daskAPI.WorkerSpec, error) {
	image := defaultImage
	if cluster.GetImage() != "" {
		image = cluster.GetImage()
	}

	var err error
	resources := defaultResources
	if cluster.GetResources() != nil {
		resources, err = convertProtobufResourcesToK8sResources(cluster.GetResources())
		if err != nil {
			return nil, err
		}
	}

	workerArgs := []string{
		"dask-worker",
		"--name",
		"$(DASK_WORKER_NAME)",
	}

	// If limits are set, append `--nthreads` and `--memory-limit` as per these docs:
	// https://kubernetes.dask.org/en/latest/kubecluster.html?#best-practices
	if resources.Limits != nil {
		limits := resources.Limits
		if limits.Cpu() != nil {
			cpuCount := limits.Cpu().String()
			workerArgs = append(workerArgs, "--nthreads", cpuCount)
		}
		if limits.Memory() != nil {
			memory := limits.Memory().String()
			workerArgs = append(workerArgs, "--memory-limit", memory)
		}
	}

	return &daskAPI.WorkerSpec{
		Replicas: int(cluster.GetNWorkers()),
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:            "dask-worker",
					Image:           image,
					ImagePullPolicy: "IfNotPresent",
					Args: workerArgs,
					Resources: *resources,
				},
			},
		},
	}, nil
}

func createSchedulerSpec(cluster plugins.DaskCluster, clusterName string, defaultImage string, defaultResources *v1.ResourceRequirements) (*daskAPI.SchedulerSpec, error) {
	schedulerImage := defaultImage
	if cluster.GetImage() != "" {
		schedulerImage = cluster.GetImage()
	}

	var err error
	resources := defaultResources
	if cluster.GetResources() != nil {
		resources, err = convertProtobufResourcesToK8sResources(cluster.GetResources())
		if err != nil {
			return nil, err
		}
	}

	return &daskAPI.SchedulerSpec{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "scheduler",
					Image: schedulerImage,
					Args:  []string{"dask-scheduler"},
					Resources: *resources,
					Ports: []v1.ContainerPort{
						{
							Name:          "comm",
							ContainerPort: 8786,
							Protocol:      "TCP",
						},
						{
							Name:          "dashboard",
							ContainerPort: 8787,
							Protocol:      "TCP",
						},
					},
				},
			},
		},
		Service: v1.ServiceSpec{
			Type: "NodePort",
			Selector: map[string]string{
				"dask.org/cluster-name": clusterName,
				"dask.org/component":    "scheduler",
			},
			Ports: []v1.ServicePort{
				{
					Name:       "comm",
					Protocol:   "TCP",
					Port:       8786,
					TargetPort: intstr.FromString("comm"),
				},
				{
					Name:       "dashboard",
					Protocol:   "TCP",
					Port:       8787,
					TargetPort: intstr.FromString("dashboard"),
				},
			},
		},
	}, nil
}

func createJobSpec(jobPodSpec plugins.JobPodSpec, workerSpec daskAPI.WorkerSpec, schedulerSpec daskAPI.SchedulerSpec, defaultArgs []string, defaultImage string, defaultResources *v1.ResourceRequirements) (*daskAPI.DaskJobSpec, error) {
	jobContainerImage := defaultImage
	if jobPodSpec.GetImage() != "" {
		jobContainerImage = jobPodSpec.GetImage()
	}

	var err error
	resources := defaultResources
	if jobPodSpec.GetResources() != nil {
		resources, err = convertProtobufResourcesToK8sResources(jobPodSpec.GetResources())
		if err != nil {
			return nil, err
		}
	}

	jobContainer := v1.Container{
		Name:  "job-runner",
		Image: jobContainerImage,
		Args:  defaultArgs,
		Resources: *resources,
	}
	return &daskAPI.DaskJobSpec{
		Job: daskAPI.JobSpec{
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					jobContainer,
				},
			},
		},
		Cluster: daskAPI.JobClusterSpec{
			Spec: daskAPI.DaskClusterSpec{
				Worker:    workerSpec,
				Scheduler: schedulerSpec,
			},
		},
	}, nil
}

func (p daskResourceHandler) GetTaskPhase(ctx context.Context, pluginContext k8s.PluginContext, r client.Object) (pluginsCore.PhaseInfo, error) {
	logPlugin, err := logs.InitializeLogPlugins(logs.GetLogConfig())
	if err != nil {
		return pluginsCore.PhaseInfoUndefined, err
	}

	return p.GetTaskPhaseWithLogs(ctx, pluginContext, r, logPlugin, " (User)")
}

func (daskResourceHandler) GetTaskPhaseWithLogs(ctx context.Context, pluginContext k8s.PluginContext, r client.Object, logPlugin tasklog.Plugin, logSuffix string) (pluginsCore.PhaseInfo, error) {
	job := r.(*daskAPI.DaskJob)
	
	// FIXME
	info := &pluginsCore.TaskInfo{
		Logs: []*core.TaskLog{},
	}


	occurredAt := time.Now()
	switch job.Status.JobStatus {
	case daskAPI.DaskJobCreated:
		return pluginsCore.PhaseInfoQueued(occurredAt, pluginsCore.DefaultPhaseVersion, "job created"), nil
	case daskAPI.DaskJobClusterCreated:
		return pluginsCore.PhaseInfoQueued(occurredAt, pluginsCore.DefaultPhaseVersion, "cluster created"), nil
	case daskAPI.DaskJobFailed:
		reason := "Dask Job failed"
		return pluginsCore.PhaseInfoRetryableFailure(errors.DownstreamSystemError, reason, info), nil
	case daskAPI.DaskJobSuccessful:
		return pluginsCore.PhaseInfoSuccess(info), nil
	}
	return pluginsCore.PhaseInfoRunning(pluginsCore.DefaultPhaseVersion, info), nil
}


func (daskResourceHandler) GetProperties() k8s.PluginProperties {
	return k8s.PluginProperties{}
}

func init() {
	if err := daskAPI.AddToScheme(scheme.Scheme); err != nil {
		panic(err)
	}

	pluginmachinery.PluginRegistry().RegisterK8sPlugin(
		k8s.PluginEntry{
			ID:                  daskTaskType,
			RegisteredTaskTypes: []pluginsCore.TaskType{daskTaskType},
			ResourceToWatch:     &daskAPI.DaskJob{},
			Plugin:              daskResourceHandler{},
			IsDefault:           false,
		})
}
