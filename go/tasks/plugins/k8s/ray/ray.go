package ray

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/plugins"
	"github.com/flyteorg/flyteplugins/go/tasks/errors"
	"github.com/flyteorg/flyteplugins/go/tasks/logs"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery"
	pluginsCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/flytek8s"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/k8s"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/utils"
	rayv1alpha1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"

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

	rayJob := plugins.RayJob{}
	err = utils.UnmarshalStruct(taskTemplate.GetCustom(), &rayJob)
	if err != nil {
		return nil, errors.Errorf(errors.BadTaskSpecification, "invalid TaskSpecification [%v], Err: [%v]", taskTemplate.GetCustom(), err.Error())
	}

	podSpec, err := flytek8s.ToK8sPodSpec(ctx, taskCtx)
	if err != nil {
		return nil, errors.Errorf(errors.BadTaskSpecification, "Unable to create pod spec: [%v]", err.Error())
	}

	headReplicas := int32(1)
	if rayJob.RayCluster.ClusterSpec.HeadGroupSpec.RayStartParams == nil {
		rayJob.RayCluster.ClusterSpec.HeadGroupSpec.RayStartParams = make(map[string]string)
	}
	rayJob.RayCluster.ClusterSpec.HeadGroupSpec.RayStartParams["include-dashboard"] = "True"
	rayJob.RayCluster.ClusterSpec.HeadGroupSpec.RayStartParams["node-ip-address"] = "$MY_POD_IP"
	rayJob.RayCluster.ClusterSpec.HeadGroupSpec.RayStartParams["dashboard-host"] = "0.0.0.0"

	rayClusterSpec := rayv1alpha1.RayClusterSpec{
		HeadGroupSpec: rayv1alpha1.HeadGroupSpec{
			Template:       buildHeadPodTemplate(podSpec),
			Replicas:       &headReplicas,
			RayStartParams: rayJob.RayCluster.ClusterSpec.HeadGroupSpec.RayStartParams,
		},
		WorkerGroupSpecs: []rayv1alpha1.WorkerGroupSpec{},
	}

	for _, spec := range rayJob.RayCluster.ClusterSpec.WorkerGroupSpec {
		workerPodTemplate := buildWorkerPodTemplate(podSpec)

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
	for index := range rayClusterSpec.WorkerGroupSpecs {
		rayClusterSpec.WorkerGroupSpecs[index].Template.Spec.ServiceAccountName = serviceAccountName
	}

	jobSpec := rayv1alpha1.RayJobSpec{
		RayClusterSpec:           rayClusterSpec,
		Entrypoint:               strings.Join(podSpec.Containers[0].Args, " "),
		ShutdownAfterJobFinishes: true,
		TTLSecondsAfterFinished:  &rayJob.TtlSecondsAfterFinished,
		RuntimeEnv:               rayJob.RuntimeEnv,
	}

	rayJobObject := rayv1alpha1.RayJob{
		TypeMeta: metav1.TypeMeta{
			Kind:       KindRayJob,
			APIVersion: rayv1alpha1.SchemeGroupVersion.String(),
		},
		Spec: jobSpec,
	}

	return &rayJobObject, nil
}

func buildHeadPodTemplate(podSpec *v1.PodSpec) v1.PodTemplateSpec {
	// Some configs are copy from  https://github.com/ray-project/kuberay/blob/b72e6bdcd9b8c77a9dc6b5da8560910f3a0c3ffd/apiserver/pkg/util/cluster.go#L97
	// They should always be the same, so we could hard code here.
	primaryContainer := &v1.Container{Name: "ray-head", Image: podSpec.Containers[0].Image}
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
	}
	return podTemplateSpec
}

func buildWorkerPodTemplate(podSpec *v1.PodSpec) v1.PodTemplateSpec {
	// Some configs are copy from  https://github.com/ray-project/kuberay/blob/b72e6bdcd9b8c77a9dc6b5da8560910f3a0c3ffd/apiserver/pkg/util/cluster.go#L185
	// They should always be the same, so we could hard code here.
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

	primaryContainer := &v1.Container{Name: "ray-worker", Image: podSpec.Containers[0].Image}
	primaryContainer.Resources = podSpec.Containers[0].Resources
	primaryContainer.Env = []v1.EnvVar{
		{
			Name:  "RAY_DISABLE_DOCKER_CPU_WARNING",
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

	// TODO: Add ray Dashboard URI to task logs

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
