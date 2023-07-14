package dask

import (
	"context"
	"fmt"
	"time"

	daskAPI "github.com/dask/dask-kubernetes/v2023/dask_kubernetes/operator/go_client/pkg/apis/kubernetes.dask.org/v1"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/plugins"
	"github.com/flyteorg/flyteplugins/go/tasks/errors"
	"github.com/flyteorg/flyteplugins/go/tasks/logs"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery"
	pluginsCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/flytek8s"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/k8s"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/tasklog"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/utils"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	daskTaskType = "dask"
	KindDaskJob  = "DaskJob"
)

func getPrimaryContainer(spec *v1.PodSpec, primaryContainerName string) *v1.Container {
	for _, container := range spec.Containers {
		if container.Name == primaryContainerName {
			return &container
		}
	}
	return nil
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

	daskJob := plugins.DaskJob{}
	err = utils.UnmarshalStruct(taskTemplate.GetCustom(), &daskJob)
	if err != nil {
		return nil, errors.Wrapf(errors.BadTaskSpecification, err, "invalid TaskSpecification [%v], failed to unmarshal", taskTemplate.GetCustom())
	}

	podSpec, objectMeta, primaryContainerName, err := flytek8s.ToK8sPodSpec(ctx, taskCtx)

	workerSpec, err := createWorkerSpec(*daskJob.Workers, podSpec, primaryContainerName)
	if err != nil {
		return nil, err
	}

	clusterName := taskCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()
	schedulerSpec, err := createSchedulerSpec(*daskJob.Scheduler, clusterName, podSpec, primaryContainerName)
	if err != nil {
		return nil, err
	}

	jobSpec := createJobSpec(*workerSpec, *schedulerSpec, podSpec, objectMeta)

	job := &daskAPI.DaskJob{
		TypeMeta: metav1.TypeMeta{
			Kind:       KindDaskJob,
			APIVersion: daskAPI.SchemeGroupVersion.String(),
		},
		ObjectMeta: *objectMeta,
		Spec:       *jobSpec,
	}
	return job, nil
}

func createWorkerSpec(cluster plugins.DaskWorkerGroup, podSpec *v1.PodSpec, primaryContainerName string) (*daskAPI.WorkerSpec, error) {
	workerPodSpec := podSpec.DeepCopy()
	primaryContainer := getPrimaryContainer(workerPodSpec, primaryContainerName)
	primaryContainer.Name = "dask-worker"

	// Set custom image if present
	if cluster.GetImage() != "" {
		primaryContainer.Image = cluster.GetImage()
	}

	// Set custom resources
	var err error
	resources := &primaryContainer.Resources
	clusterResources := cluster.GetResources()
	if len(clusterResources.Requests) >= 1 || len(clusterResources.Limits) >= 1 {
		resources, err = flytek8s.ToK8sResourceRequirements(cluster.GetResources())
		if err != nil {
			return nil, err
		}
	}
	primaryContainer.Resources = *resources

	// Set custom args
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
	primaryContainer.Args = workerArgs

	return &daskAPI.WorkerSpec{
		Replicas: int(cluster.GetNumberOfWorkers()),
		Spec:     *workerPodSpec,
	}, nil
}

func createSchedulerSpec(scheduler plugins.DaskScheduler, clusterName string, podSpec *v1.PodSpec, primaryContainerName string) (*daskAPI.SchedulerSpec, error) {
	schedulerPodSpec := podSpec.DeepCopy()
	primaryContainer := getPrimaryContainer(schedulerPodSpec, primaryContainerName)
	primaryContainer.Name = "scheduler"

	// Override image if applicable
	if scheduler.GetImage() != "" {
		primaryContainer.Image = scheduler.GetImage()
	}

	// Override resources if applicable
	var err error
	resources := &primaryContainer.Resources
	schedulerResources := scheduler.GetResources()
	if len(schedulerResources.Requests) >= 1 || len(schedulerResources.Limits) >= 1 {
		resources, err = flytek8s.ToK8sResourceRequirements(scheduler.GetResources())
		if err != nil {
			return nil, err
		}
	}
	primaryContainer.Resources = *resources

	// Override args
	primaryContainer.Args = []string{"dask-scheduler"}

	// Add ports
	primaryContainer.Ports = []v1.ContainerPort{
		{
			Name:          "tcp-comm",
			ContainerPort: 8786,
			Protocol:      "TCP",
		},
		{
			Name:          "dashboard",
			ContainerPort: 8787,
			Protocol:      "TCP",
		},
	}

	// Set restart policy
	schedulerPodSpec.RestartPolicy = v1.RestartPolicyAlways

	return &daskAPI.SchedulerSpec{
		Spec: *schedulerPodSpec,
		Service: v1.ServiceSpec{
			Type: v1.ServiceTypeNodePort,
			Selector: map[string]string{
				"dask.org/scheduler-name": clusterName,
				"dask.org/component":      "scheduler",
			},
			Ports: []v1.ServicePort{
				{
					Name:       "tcp-comm",
					Protocol:   "TCP",
					Port:       8786,
					TargetPort: intstr.FromString("tcp-comm"),
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

func createJobSpec(workerSpec daskAPI.WorkerSpec, schedulerSpec daskAPI.SchedulerSpec, podSpec *v1.PodSpec, objectMeta *metav1.ObjectMeta) *daskAPI.DaskJobSpec {
	jobPodSpec := podSpec.DeepCopy()
	jobPodSpec.RestartPolicy = v1.RestartPolicyNever

	return &daskAPI.DaskJobSpec{
		Job: daskAPI.JobSpec{
			Spec: *jobPodSpec,
		},
		Cluster: daskAPI.DaskCluster{
			ObjectMeta: *objectMeta,
			Spec: daskAPI.DaskClusterSpec{
				Worker:    workerSpec,
				Scheduler: schedulerSpec,
			},
		},
	}
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

	// There is a short period between the `DaskJob` resource being created and `Status.JobStatus` being set by the `dask-operator`.
	// In that period, the `JobStatus` will be an empty string. We're treating this as Initializing/Queuing.
	isQueued := status == "" ||
		status == daskAPI.DaskJobCreated ||
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
	case "":
		return pluginsCore.PhaseInfoInitializing(occurredAt, pluginsCore.DefaultPhaseVersion, "unknown", &info), nil
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
