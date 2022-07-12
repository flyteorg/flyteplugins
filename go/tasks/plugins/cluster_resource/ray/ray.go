package ray

import (
	"context"
	"fmt"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/tasklog"
	v1 "k8s.io/api/core/v1"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/flyteorg/flyteplugins/go/tasks/errors"
	"github.com/flyteorg/flyteplugins/go/tasks/logs"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery"
	pluginsCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/flytek8s"
	rayv1alpha1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1alpha1"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/k8s"
	"k8s.io/client-go/kubernetes/scheme"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	rayTaskType    = "ray"
	KindRayCluster = "RayCluster"
)

type rayClusterResourceHandler struct {
}

func (rayClusterResourceHandler) GetProperties() k8s.PluginProperties {
	return k8s.PluginProperties{}
}

// BuildResource Creates a new ray cluster resource.
func (rayClusterResourceHandler) BuildResource(ctx context.Context, taskCtx pluginsCore.TaskExecutionContext) (client.Object, error) {
	taskTemplate, err := taskCtx.TaskReader().Read(ctx)
	if err != nil {
		return nil, errors.Errorf(errors.BadTaskSpecification, "unable to fetch task specification [%v]", err.Error())
	} else if taskTemplate == nil {
		return nil, errors.Errorf(errors.BadTaskSpecification, "nil task specification")
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

	rayCluster := &rayv1alpha1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: ray.Name,
		},
		Spec: rayv1alpha1.RayClusterSpec{
			HeadGroupSpec: rayv1alpha1.HeadGroupSpec{
				ServiceType:    v1.ServiceType(ray.ClusterSpec.HeadGroupSpec.ServiceType),
				Template:       buildHeadPodTemplate(podSpec, *ray.ClusterSpec.HeadGroupSpec, ray.Name),
				Replicas:       &headReplicas,
				RayStartParams: ray.ClusterSpec.HeadGroupSpec.RayStartParams,
			},
			WorkerGroupSpecs: []rayv1alpha1.WorkerGroupSpec{},
		},
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

		rayCluster.Spec.WorkerGroupSpecs = append(rayCluster.Spec.WorkerGroupSpecs, workerNodeSpec)
	}

	serviceAccountName := flytek8s.GetServiceAccountNameFromTaskExecutionMetadata(taskCtx.TaskExecutionMetadata())
	rayCluster.Spec.HeadGroupSpec.Template.Spec.ServiceAccountName = serviceAccountName
	for _, worker := range rayCluster.Spec.WorkerGroupSpecs {
		worker.Template.Spec.ServiceAccountName = serviceAccountName
	}

	return rayCluster, nil
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

func (rayClusterResourceHandler) BuildIdentityResource(ctx context.Context, taskCtx pluginsCore.TaskExecutionMetadata) (client.Object, error) {
	return &rayv1alpha1.RayCluster{
		TypeMeta: metav1.TypeMeta{
			Kind:       KindRayCluster,
			APIVersion: rayv1alpha1.SchemeGroupVersion.String(),
		},
	}, nil
}

func getEventInfoForRayCluster(rayCluster *rayv1alpha1.RayCluster) (*pluginsCore.TaskInfo, error) {
	taskLogs := make([]*core.TaskLog, 0, 3)
	logPlugin, err := logs.InitializeLogPlugins(logs.GetLogConfig())

	if err != nil {
		return nil, err
	}

	if logPlugin == nil {
		return nil, nil
	}

	o, err := logPlugin.GetTaskLogs(tasklog.Input{
		PodName:   rayCluster.Name + "-head",
		Namespace: rayCluster.Namespace,
		LogName:   "(Head Node)",
	})

	if err != nil {
		return nil, err
	}

	taskLogs = append(taskLogs, o.TaskLogs...)

	// TODO: Add Ray dashboard URI

	return &pluginsCore.TaskInfo{
		Logs: taskLogs,
	}, nil
}

func (rayClusterResourceHandler) GetTaskPhase(ctx context.Context, pluginContext k8s.PluginContext, resource client.Object) (pluginsCore.PhaseInfo, error) {
	rayCluster := resource.(*rayv1alpha1.RayCluster)
	info, err := getEventInfoForRayCluster(rayCluster)
	if err != nil {
		return pluginsCore.PhaseInfoUndefined, err
	}
	switch rayCluster.Status.State {
	case rayv1alpha1.Unhealthy:
	case rayv1alpha1.Failed:
		reason := fmt.Sprintf("Failed to create Ray cluster: %s", rayCluster.Name)
		return pluginsCore.PhaseInfoFailure(errors.DownstreamSystemError, reason, info), nil
	case rayv1alpha1.Ready:
		return pluginsCore.PhaseInfoClusterRunning(pluginsCore.DefaultPhaseVersion, info), nil
	}
	return pluginsCore.PhaseInfoNotReady(time.Now(), pluginsCore.DefaultPhaseVersion, "job submitted"), nil
}

func init() {
	if err := rayv1alpha1.AddToScheme(scheme.Scheme); err != nil {
		panic(err)
	}

	pluginmachinery.PluginRegistry().RegisterClusterResourcePlugin(
		k8s.PluginEntry{
			ID:                  rayTaskType,
			RegisteredTaskTypes: []pluginsCore.TaskType{rayTaskType},
			ResourceToWatch:     &rayv1alpha1.RayCluster{},
			Plugin:              rayClusterResourceHandler{},
			IsDefault:           false,
		})
}
