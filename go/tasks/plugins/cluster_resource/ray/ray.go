package ray

import (
	"context"
	"fmt"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flyteplugins/go/tasks/errors"
	"github.com/flyteorg/flyteplugins/go/tasks/logs"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery"
	pluginsCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/flytek8s"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/k8s"
	rayv1alpha1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"strings"
	"time"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/tasklog"
	v1 "k8s.io/api/core/v1"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	rayTaskType = "ray"
	KindRayJob  = "RayJob"
)

type rayJobResourceHandler struct {
}

func (rayJobResourceHandler) GetProperties() k8s.PluginProperties {
	return k8s.PluginProperties{}
}

// BuildResource Creates a new ray job resource.
func (rayJobResourceHandler) BuildResource(ctx context.Context, taskCtx pluginsCore.TaskExecutionContext) (client.Object, error) {
	taskTemplate, err := taskCtx.TaskReader().Read(ctx)
	if err != nil {
		return nil, errors.Errorf(errors.BadTaskSpecification, "unable to fetch task specification [%v]", err.Error())
	} else if taskTemplate == nil {
		return nil, errors.Errorf(errors.BadTaskSpecification, "nil task specification")
	}

	if taskTemplate.GetContainer() == nil {
		return nil, fmt.Errorf("container not specified in task template")
	}
	ray := taskTemplate.Resources[taskTemplate.Id.Name].GetRay()
	podSpec, err := flytek8s.ToK8sPodSpec(ctx, taskCtx)
	if err != nil {
		return nil, errors.Errorf(errors.BadTaskSpecification, "Unable to create pod spec: [%v]", err.Error())
	}

	headReplicas := int32(1)
	if ray.ClusterSpec.HeadGroupSpec.RayStartParams == nil {
		ray.ClusterSpec.HeadGroupSpec.RayStartParams = make(map[string]string)
	}
	ray.ClusterSpec.HeadGroupSpec.RayStartParams["node-ip-address"] = "$MY_POD_IP"
	ray.ClusterSpec.HeadGroupSpec.RayStartParams["dashboard-host"] = "0.0.0.0"
	ray.ClusterSpec.HeadGroupSpec.RayStartParams["port"] = "6379"

	rayClusterSpec := rayv1alpha1.RayClusterSpec{
		HeadGroupSpec: rayv1alpha1.HeadGroupSpec{
			ServiceType:    v1.ServiceType(ray.ClusterSpec.HeadGroupSpec.ServiceType),
			Template:       buildHeadPodTemplate(podSpec, *ray.ClusterSpec.HeadGroupSpec, ray.Name),
			Replicas:       &headReplicas,
			RayStartParams: ray.ClusterSpec.HeadGroupSpec.RayStartParams,
		},
		WorkerGroupSpecs: []rayv1alpha1.WorkerGroupSpec{},
	}

	for index, spec := range ray.ClusterSpec.WorkerGroupSpec {
		workerPodTemplate := buildWorkerPodTemplate(podSpec, ray.ClusterSpec.WorkerGroupSpec[index], ray.Name)

		minReplicas := spec.Replicas
		maxReplicas := spec.Replicas
		if spec.MinReplicas != 0 {
			minReplicas = spec.MinReplicas
		}
		if spec.MaxReplicas != 0 {
			maxReplicas = spec.MaxReplicas
		}

		if spec.RayStartParams == nil {
			spec.RayStartParams = make(map[string]string)
		}
		spec.RayStartParams["node-ip-address"] = "$MY_POD_IP"

		workerNodeSpec := rayv1alpha1.WorkerGroupSpec{
			GroupName:      spec.GroupName,
			MinReplicas:    &minReplicas,
			MaxReplicas:    &maxReplicas,
			Replicas:       &spec.Replicas,
			RayStartParams: spec.RayStartParams,
			Template:       workerPodTemplate,
		}

		rayClusterSpec.WorkerGroupSpecs = append(rayClusterSpec.WorkerGroupSpecs, workerNodeSpec)
	}

	serviceAccountName := flytek8s.GetServiceAccountNameFromTaskExecutionMetadata(taskCtx.TaskExecutionMetadata())

	rayClusterSpec.HeadGroupSpec.Template.Spec.ServiceAccountName = serviceAccountName
	for index, _ := range rayClusterSpec.WorkerGroupSpecs {
		rayClusterSpec.WorkerGroupSpecs[index].Template.Spec.ServiceAccountName = serviceAccountName
	}

	jobSpec := rayv1alpha1.RayJobSpec{
		RayClusterSpec:           rayClusterSpec,
		Entrypoint:               strings.Join(podSpec.Containers[0].Args, " "),
		ShutdownAfterJobFinishes: true,
		RuntimeEnv:               "eyJwaXAiOiBbImdpdCtodHRwczovL2dpdGh1Yi5jb20vZmx5dGVvcmcvZmx5dGVraXQuZ2l0QHJheSNlZ2c9Zmx5dGVraXRwbHVnaW5zLXJheSZzdWJkaXJlY3Rvcnk9cGx1Z2lucy9mbHl0ZWtpdC1yYXkiLCJnaXQraHR0cHM6Ly9naXRodWIuY29tL2ZseXRlb3JnL2ZseXRla2l0QHJheSIsICJnaXQraHR0cHM6Ly9naXRodWIuY29tL2ZseXRlb3JnL2ZseXRlaWRsQHJheSJdfQ==",
	}

	rayJob := rayv1alpha1.RayJob{
		TypeMeta: metav1.TypeMeta{
			Kind:       KindRayJob,
			APIVersion: rayv1alpha1.SchemeGroupVersion.String(),
		},
		Spec: jobSpec,
	}

	return &rayJob, nil
}

func buildHeadPodTemplate(podSpec *v1.PodSpec, headSpec core.HeadGroupSpec, rayName string) v1.PodTemplateSpec {
	primaryContainer := &v1.Container{Name: "ray-head", Image: headSpec.Image}
	primaryContainer.Resources = podSpec.Containers[0].Resources
	primaryContainer.Env = []v1.EnvVar{
		{
			Name: "MY_POD_IP",
			ValueFrom: &v1.EnvVarSource{
				FieldRef: &v1.ObjectFieldSelector{
					FieldPath: "status.podIP",
				},
			},
		},
		{
			Name:  "RAY_LOG_TO_STDERR",
			Value: "1",
		},
	}
	primaryContainer.Env = append(primaryContainer.Env, podSpec.Containers[0].Env...)
	primaryContainer.Ports = []v1.ContainerPort{
		{
			Name:          "redis",
			ContainerPort: 6379,
		},
		{
			Name:          "head",
			ContainerPort: 10001,
		},
		{
			Name:          "dashboard",
			ContainerPort: 8265,
		},
	}

	podTemplateSpec := v1.PodTemplateSpec{
		Spec: v1.PodSpec{
			Containers: []v1.Container{*primaryContainer},
		},
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{"rayCluster": rayName},
		},
	}
	return podTemplateSpec
}

func buildWorkerPodTemplate(podSpec *v1.PodSpec, workerSpec *core.WorkerGroupSpec, rayName string) v1.PodTemplateSpec {
	initContainers := []v1.Container{
		{
			Name:  "init-myservice",
			Image: "busybox:1.28",
			Command: []string{
				"sh",
				"-c",
				"until nslookup $RAY_IP.$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace).svc.cluster.local; do echo waiting for myservice; sleep 2; done",
			},
			Resources: podSpec.Containers[0].Resources,
		},
	}

	primaryContainer := &v1.Container{Name: "ray-worker", Image: workerSpec.Image}
	primaryContainer.Resources = podSpec.Containers[0].Resources
	primaryContainer.Env = []v1.EnvVar{
		{
			Name:  "RAY_DISABLE_DOCKER_CPU_WRARNING",
			Value: "1",
		},
		{
			Name:  "TYPE",
			Value: "worker",
		},
		{
			Name:  "RAY_LOG_TO_STDERR",
			Value: "1",
		},
		{
			Name: "CPU_REQUEST",
			ValueFrom: &v1.EnvVarSource{
				ResourceFieldRef: &v1.ResourceFieldSelector{
					ContainerName: "ray-worker",
					Resource:      "requests.cpu",
				},
			},
		},
		{
			Name: "CPU_LIMITS",
			ValueFrom: &v1.EnvVarSource{
				ResourceFieldRef: &v1.ResourceFieldSelector{
					ContainerName: "ray-worker",
					Resource:      "limits.cpu",
				},
			},
		},
		{
			Name: "MEMORY_REQUESTS",
			ValueFrom: &v1.EnvVarSource{
				ResourceFieldRef: &v1.ResourceFieldSelector{
					ContainerName: "ray-worker",
					Resource:      "requests.cpu",
				},
			},
		},
		{
			Name: "MEMORY_LIMITS",
			ValueFrom: &v1.EnvVarSource{
				ResourceFieldRef: &v1.ResourceFieldSelector{
					ContainerName: "ray-worker",
					Resource:      "limits.cpu",
				},
			},
		},
		{
			Name: "MY_POD_NAME",
			ValueFrom: &v1.EnvVarSource{
				FieldRef: &v1.ObjectFieldSelector{
					FieldPath: "metadata.name",
				},
			},
		},
		{
			Name: "MY_POD_IP",
			ValueFrom: &v1.EnvVarSource{
				FieldRef: &v1.ObjectFieldSelector{
					FieldPath: "status.podIP",
				},
			},
		},
	}
	primaryContainer.Env = append(primaryContainer.Env, podSpec.Containers[0].Env...)
	primaryContainer.Lifecycle = &v1.Lifecycle{
		PreStop: &v1.LifecycleHandler{
			Exec: &v1.ExecAction{
				Command: []string{
					"/bin/sh", "-c", "ray stop",
				},
			},
		},
	}

	primaryContainer.Ports = []v1.ContainerPort{
		{
			Name:          "redis",
			ContainerPort: 6379,
		},
		{
			Name:          "head",
			ContainerPort: 10001,
		},
		{
			Name:          "dashboard",
			ContainerPort: 8265,
		},
	}

	podTemplateSpec := v1.PodTemplateSpec{
		Spec: v1.PodSpec{
			Containers:     []v1.Container{*primaryContainer},
			InitContainers: initContainers,
		},
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{"rayCluster": rayName},
		},
	}
	return podTemplateSpec
}

func (rayJobResourceHandler) BuildIdentityResource(ctx context.Context, taskCtx pluginsCore.TaskExecutionMetadata) (client.Object, error) {
	return &rayv1alpha1.RayJob{
		TypeMeta: metav1.TypeMeta{
			Kind:       KindRayJob,
			APIVersion: rayv1alpha1.SchemeGroupVersion.String(),
		},
	}, nil
}

func getEventInfoForRayJob(rayJob *rayv1alpha1.RayJob) (*pluginsCore.TaskInfo, error) {
	taskLogs := make([]*core.TaskLog, 0, 3)
	logPlugin, err := logs.InitializeLogPlugins(logs.GetLogConfig())

	if err != nil {
		return nil, err
	}

	if logPlugin == nil {
		return nil, nil
	}

	o, err := logPlugin.GetTaskLogs(tasklog.Input{
		PodName:   rayJob.Status.RayClusterName + "-head",
		Namespace: rayJob.Namespace,
		LogName:   "(Head Node)",
	})

	if err != nil {
		return nil, err
	}

	taskLogs = append(taskLogs, o.TaskLogs...)

	if len(rayJob.Status.DashboardURL) != 0 {
		taskLogs = append(taskLogs, &core.TaskLog{
			Uri:           rayJob.Status.DashboardURL,
			Name:          "Ray Dashboard URL",
			MessageFormat: core.TaskLog_JSON,
		})
	}

	return &pluginsCore.TaskInfo{
		Logs: taskLogs,
	}, nil
}

func (rayJobResourceHandler) GetTaskPhase(ctx context.Context, pluginContext k8s.PluginContext, resource client.Object) (pluginsCore.PhaseInfo, error) {
	rayJob := resource.(*rayv1alpha1.RayJob)
	info, err := getEventInfoForRayJob(rayJob)
	if err != nil {
		return pluginsCore.PhaseInfoUndefined, err
	}
	switch rayJob.Status.JobStatus {
	case rayv1alpha1.JobStatusPending:
		return pluginsCore.PhaseInfoNotReady(time.Now(), pluginsCore.DefaultPhaseVersion, "job is pending"), nil
	case rayv1alpha1.JobStatusFailed:
		reason := fmt.Sprintf("Failed to create Ray job: %s", rayJob.Name)
		return pluginsCore.PhaseInfoFailure(errors.TaskFailedWithError, reason, info), nil
	case rayv1alpha1.JobStatusSucceeded:
		return pluginsCore.PhaseInfoSuccess(info), nil
	}
	return pluginsCore.PhaseInfoQueued(time.Now(), pluginsCore.DefaultPhaseVersion, "JobCreated"), nil
}

func init() {
	if err := rayv1alpha1.AddToScheme(scheme.Scheme); err != nil {
		panic(err)
	}

	pluginmachinery.PluginRegistry().RegisterK8sPlugin(
		k8s.PluginEntry{
			ID:                  rayTaskType,
			RegisteredTaskTypes: []pluginsCore.TaskType{rayTaskType},
			ResourceToWatch:     &rayv1alpha1.RayJob{},
			Plugin:              rayJobResourceHandler{},
			IsDefault:           false,
		})
}
