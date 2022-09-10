package dask

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flyteplugins/go/tasks/logs"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery"
	pluginsCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core/template"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/flytek8s"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/k8s"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/tasklog"
	"github.com/flyteorg/flytestdlib/logger"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "k8s.io/api/core/v1"

	daskAPI "github.com/bstadlbauer/dask-k8s-operator-go-client/pkg/apis/kubernetes.dask.org/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	daskTaskType        = "dask"
	jobRunnerPodPostfix = "-runner"
	KindDaskJob         = "DaskJob"
	PrimaryContainerKey = "primary_container_name"
)

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
	task, err := taskCtx.TaskReader().Read(ctx)
	if err != nil {
		logger.Warnf(ctx, "failed to read task information when trying to construct Pod, err: %s", err.Error())
		return nil, err
	}
	if task.GetContainer() == nil {
		logger.Errorf(ctx, "Default Pod creation logic works for default container in the task template only.")
		return nil, fmt.Errorf("container not specified in task template")
	}
	executionMetadata := taskCtx.TaskExecutionMetadata()
	taskName := executionMetadata.GetTaskExecutionID().GetGeneratedName()

	container := task.GetContainer()
	image := container.GetImage()

	workerSpec := createWorkerSpec(image)
	schedulerSpec := createSchedulerSpec(taskName, image)

	templateParameters := template.Parameters{
		TaskExecMetadata: taskCtx.TaskExecutionMetadata(),
		Inputs:           taskCtx.InputReader(),
		OutputPath:       taskCtx.OutputWriter(),
		Task:             taskCtx.TaskReader(),
	}
	resourceMode := flytek8s.ResourceCustomizationModeMergeExistingResources
	jobContainer := v1.Container{
		Name:  "dask-job",
		Image: image,
		Args:  container.GetArgs(),
		Resources: v1.ResourceRequirements{
			Limits: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU:    resource.MustParse("0.5"),
				v1.ResourceMemory: resource.MustParse("200Mi"),
			},
		},
	}
	err = flytek8s.AddFlyteCustomizationsToContainer(ctx, templateParameters, resourceMode, &jobContainer)
	if err != nil {
		return nil, err
	}
	jobSpec := createJobSpec(image, container, jobContainer, workerSpec, schedulerSpec)

	job := &daskAPI.DaskJob{
		TypeMeta: metav1.TypeMeta{
			Kind:       KindDaskJob,
			APIVersion: daskAPI.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "dask-job", // Will be overridden by Flyte
		},
		Spec: jobSpec,
	}
	return job, nil
}

func createWorkerSpec(image string) daskAPI.WorkerSpec {
	return daskAPI.WorkerSpec{
		Replicas: 1,
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:            "worker",
					Image:           image,
					ImagePullPolicy: "IfNotPresent",
					Args: []string{
						"dask-worker",
						"--name",
						"$(DASK_WORKER_NAME)",
					},
					Resources: v1.ResourceRequirements{
						Limits: map[v1.ResourceName]resource.Quantity{
							v1.ResourceCPU:    resource.MustParse("0.5"),
							v1.ResourceMemory: resource.MustParse("200Mi"),
						},
					},
				},
			},
		},
	}
}

func createSchedulerSpec(taskName string, image string) daskAPI.SchedulerSpec {
	return daskAPI.SchedulerSpec{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "scheduler",
					Image: image,
					Args:  []string{"dask-scheduler"},
					Resources: v1.ResourceRequirements{
						Limits: map[v1.ResourceName]resource.Quantity{
							v1.ResourceCPU:    resource.MustParse("0.5"),
							v1.ResourceMemory: resource.MustParse("200Mi"),
						},
					},
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
				"dask.org/cluster-name": taskName + "-cluster",
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
	}
}

func createJobSpec(image string, container *core.Container, jobContainer v1.Container, workerSpec daskAPI.WorkerSpec, schedulerSpec daskAPI.SchedulerSpec) daskAPI.DaskJobSpec {

	return daskAPI.DaskJobSpec{
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
	}
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

	pod, err := getJobPodFromJobResource(job, ctx)
	if err != nil {
		return pluginsCore.PhaseInfoUndefined, err
	}
	
	// Logic copied from the pod plugin
	transitionOccurredAt := flytek8s.GetLastTransitionOccurredAt(pod).Time
	info := pluginsCore.TaskInfo{
		OccurredAt: &transitionOccurredAt,
	}

	if pod.Status.Phase != v1.PodPending && pod.Status.Phase != v1.PodUnknown {
		taskLogs, err := logs.GetLogsForContainerInPod(ctx, logPlugin, pod, 0, logSuffix)
		if err != nil {
			return pluginsCore.PhaseInfoUndefined, err
		}
		info.Logs = taskLogs
	}

	switch pod.Status.Phase {
	case v1.PodSucceeded:
		return flytek8s.DemystifySuccess(pod.Status, info)
	case v1.PodFailed:
		return flytek8s.DemystifyFailure(pod.Status, info)
	case v1.PodPending:
		return flytek8s.DemystifyPending(pod.Status)
	case v1.PodReasonUnschedulable:
		return pluginsCore.PhaseInfoQueued(transitionOccurredAt, pluginsCore.DefaultPhaseVersion, "pod unschedulable"), nil
	case v1.PodUnknown:
		return pluginsCore.PhaseInfoUndefined, nil
	}

	if len(info.Logs) > 0 {
		return pluginsCore.PhaseInfoRunning(pluginsCore.DefaultPhaseVersion+1, &info), nil
	}
	return pluginsCore.PhaseInfoRunning(pluginsCore.DefaultPhaseVersion, &info), nil
}


func getClientset() (*kubernetes.Clientset, error) {
	// FIXME: Handle auth
	config, err := clientcmd.BuildConfigFromFlags("", "/Users/bstadlbauer/.kube/config")
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return clientset, nil
}

func getJobPodFromJobResource(job *daskAPI.DaskJob, ctx context.Context) (*v1.Pod, error) {
	clientset, err := getClientset()
	if err != nil {
		return nil, err
	}

	jobPodName := job.ObjectMeta.Name + jobRunnerPodPostfix
	jobPodNamespace := job.ObjectMeta.Namespace
	pod, err := clientset.CoreV1().Pods(jobPodNamespace).Get(ctx, jobPodName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return pod, nil
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
