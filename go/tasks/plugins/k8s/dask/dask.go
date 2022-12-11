package dask

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/scheme"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flyteplugins/go/tasks/errors"
	"github.com/flyteorg/flyteplugins/go/tasks/logs"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery"
	pluginsCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core/template"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/flytek8s"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/k8s"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/tasklog"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/utils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	daskAPI "github.com/bstadlbauer/dask-k8s-operator-go-client/pkg/apis/kubernetes.dask.org/v1"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/plugins"
	v1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)


const (
	daskTaskType        = "dask"
	resourceNvidiaGPU   = "nvidia.com/gpu"
	KindDaskJob         = "DaskJob"
	PrimaryContainerKey = "primary_container_name"
)

type defaults struct {
	Namespace   string
	Image       string
	Container   v1.Container
	Resources   *v1.ResourceRequirements
	Env         []v1.EnvVar
	Annotations map[string]string
}

func getDefaults(ctx context.Context, taskCtx pluginsCore.TaskExecutionContext, taskTemplate core.TaskTemplate) (*defaults, error) {
	executionMetadata := taskCtx.TaskExecutionMetadata()

	defaultContainerSpec := taskTemplate.GetContainer()
	if defaultContainerSpec == nil {
		return nil, errors.Errorf(errors.BadTaskSpecification, "task is missing a default container")
	}

	defaultImage := defaultContainerSpec.GetImage()
	if defaultImage == "" {
		return nil, errors.Errorf(errors.BadTaskSpecification, "task is missing a default image")
	}

	envVars := []v1.EnvVar{}
	if taskTemplate.GetContainer().GetEnv() != nil {
		for _, keyValuePair := range taskTemplate.GetContainer().GetEnv() {
			envVars = append(envVars, v1.EnvVar{Name: keyValuePair.Key, Value: keyValuePair.Value})
		}
	}

	var defaultResources *v1.ResourceRequirements
	if executionMetadata.GetOverrides() != nil && executionMetadata.GetOverrides().GetResources() != nil {
		defaultResources = executionMetadata.GetOverrides().GetResources()
	}

	// The default container will be used to run the job (i.e. within the driver pod)
	defaultContainer := v1.Container{
		Name:  "job-runner",
		Image: defaultImage,
		Args:  defaultContainerSpec.GetArgs(),
		Env:   envVars,
	}

	templateParameters := template.Parameters{
		TaskExecMetadata: taskCtx.TaskExecutionMetadata(),
		Inputs:           taskCtx.InputReader(),
		OutputPath:       taskCtx.OutputWriter(),
		Task:             taskCtx.TaskReader(),
	}
	err := flytek8s.AddFlyteCustomizationsToContainer(ctx, templateParameters, flytek8s.ResourceCustomizationModeAssignResources, &defaultContainer)
	if err != nil {
		return nil, err
	}

	return &defaults{
		Namespace:   executionMetadata.GetNamespace(),
		Image:       defaultImage,
		Container:   defaultContainer,
		Resources:   defaultResources,
		Env:         envVars,
		Annotations: executionMetadata.GetAnnotations(),
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
	defaults, err := getDefaults(ctx, taskCtx, *taskTemplate)
	if err != nil {
		return nil, err
	}
	clusterName := taskCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()

	daskJob := plugins.DaskJob{}
	err = utils.UnmarshalStruct(taskTemplate.GetCustom(), &daskJob)
	if err != nil {
		return nil, errors.Wrapf(errors.BadTaskSpecification, err, "invalid TaskSpecification [%v], failed to unmarshal", taskTemplate.GetCustom())
	}

	workerSpec, err := createWorkerSpec(*daskJob.Cluster, *defaults)
	if err != nil {
		return nil, err
	}
	schedulerSpec, err := createSchedulerSpec(*daskJob.Cluster, clusterName, *defaults)
	if err != nil {
		return nil, err
	}
	jobSpec, err := createJobSpec(*daskJob.JobPodSpec, *workerSpec, *schedulerSpec, *defaults)
	if err != nil {
		return nil, err
	}

	namespace := defaults.Namespace
	if daskJob.GetNamespace() != "" {
		namespace = daskJob.GetNamespace()
	}
	job := &daskAPI.DaskJob{
		TypeMeta: metav1.TypeMeta{
			Kind:       KindDaskJob,
			APIVersion: daskAPI.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        "will-be-overridden", // Will be overridden by Flyte to `clusterName`
			Namespace:   namespace,
			Annotations: defaults.Annotations,
		},
		Spec: *jobSpec,
	}
	return job, nil
}

func createWorkerSpec(cluster plugins.DaskCluster, defaults defaults) (*daskAPI.WorkerSpec, error) {
	image := defaults.Image
	if cluster.GetImage() != "" {
		image = cluster.GetImage()
	}

	var err error
	resources := defaults.Resources
	if cluster.GetResources() != nil {
		resources, err = flytek8s.ToK8sResourceRequirements(cluster.GetResources())
		if err != nil {
			return nil, err
		}
	}
	if resources == nil {
		resources = &v1.ResourceRequirements{}
	}

	workerArgs := []string{
		"dask-worker",
		"--name",
		"$(DASK_WORKER_NAME)",
	}

	// If limits are set, append `--nthreads` and `--memory-limit` as per these docs:
	// https://kubernetes.dask.org/en/latest/kubecluster.html?#best-practices
	if resources != nil && resources.Limits != nil {
		limits := resources.Limits
		if limits.Cpu() != nil {
			cpuCount := fmt.Sprintf("%v", limits.Cpu().Value())
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
					ImagePullPolicy: v1.PullIfNotPresent,
					Args:            workerArgs,
					Resources:       *resources,
					Env:             defaults.Env,
				},
			},
		},
	}, nil
}

func createSchedulerSpec(cluster plugins.DaskCluster, clusterName string, defaults defaults) (*daskAPI.SchedulerSpec, error) {
	schedulerImage := defaults.Image
	if cluster.GetImage() != "" {
		schedulerImage = cluster.GetImage()
	}

	var err error
	resources := defaults.Resources
	if cluster.GetResources() != nil {
		resources, err = flytek8s.ToK8sResourceRequirements(cluster.GetResources())
		if err != nil {
			return nil, err
		}
	}
	if resources == nil {
		resources = &v1.ResourceRequirements{}
	}

	return &daskAPI.SchedulerSpec{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:      "scheduler",
					Image:     schedulerImage,
					Args:      []string{"dask-scheduler"},
					Resources: *resources,
					Env:       defaults.Env,
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
			Type: v1.ServiceTypeNodePort,
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

func createJobSpec(jobPodSpec plugins.JobPodSpec, workerSpec daskAPI.WorkerSpec, schedulerSpec daskAPI.SchedulerSpec, defaults defaults) (*daskAPI.DaskJobSpec, error) {
	jobContainer := defaults.Container

	if jobPodSpec.GetImage() != "" {
		jobContainer.Image = jobPodSpec.GetImage()
	}

	if jobPodSpec.GetResources() != nil {
		resources, err := flytek8s.ToK8sResourceRequirements(jobPodSpec.GetResources())
		if err != nil {
			return nil, err
		}
		jobContainer.Resources = *resources
	}

	return &daskAPI.DaskJobSpec{
		Job: daskAPI.JobSpec{
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					jobContainer,
				},
			},
		},
		Cluster: daskAPI.DaskCluster{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: defaults.Annotations,
			},
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
	job := r.(*daskAPI.DaskJob)
	status := job.Status.JobStatus
	occurredAt := time.Now()

	info := pluginsCore.TaskInfo{
		OccurredAt: &occurredAt,
	}

	isQueued := status == daskAPI.DaskJobCreated ||
		status == daskAPI.DaskJobClusterCreated

	if !isQueued {
		o, err := logPlugin.GetTaskLogs(tasklog.Input{
			Namespace: job.ObjectMeta.Namespace,
			PodName:   job.Status.JobRunnerPodName,
			LogName:   "(User logs)",
		},
		)
		if err != nil {
			return pluginsCore.PhaseInfoUndefined, err
		}
		info.Logs = o.TaskLogs
	}

	switch status {
	case daskAPI.DaskJobCreated:
		return pluginsCore.PhaseInfoInitializing(occurredAt, pluginsCore.DefaultPhaseVersion, "job created", &info), nil
	case daskAPI.DaskJobClusterCreated:
		return pluginsCore.PhaseInfoInitializing(occurredAt, pluginsCore.DefaultPhaseVersion, "cluster created", &info), nil
	case daskAPI.DaskJobFailed:
		reason := "Dask Job failed"
		return pluginsCore.PhaseInfoRetryableFailure(errors.DownstreamSystemError, reason, &info), nil
	case daskAPI.DaskJobSuccessful:
		return pluginsCore.PhaseInfoSuccess(&info), nil
	}
	return pluginsCore.PhaseInfoRunning(pluginsCore.DefaultPhaseVersion, &info), nil
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
