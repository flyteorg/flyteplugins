package ray

import (
	"context"
	"fmt"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/tasklog"
	"github.com/ray-project/kuberay/apiserver/pkg/util"
	api "github.com/ray-project/kuberay/proto/go_client"

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

func validateRayCluster(rayCluster *api.Cluster) error {
	if rayCluster == nil {
		return fmt.Errorf("empty rayCluster")
	}

	if rayCluster.ClusterSpec.HeadGroupSpec == nil && rayCluster.ClusterSpec.WorkerGroupSpec == nil {
		return fmt.Errorf("both HeadGroupSpeca and WorkerGroupSpecs must be set")
	}

	return nil
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
	headGroupSpec := ray.ClusterSpec.HeadGroupSpec
	var workerGroupSpec []*api.WorkerGroupSpec
	for _, val := range ray.ClusterSpec.WorkerGroupSpec {
		workerGroupSpec = append(workerGroupSpec, &api.WorkerGroupSpec{
			GroupName:       val.GroupName,
			ComputeTemplate: val.ComputeTemplate,
			Image:           val.Image,
			Replicas:        val.Replicas,
			MaxReplicas:     val.MaxReplicas,
			MinReplicas:     val.MinReplicas,
			RayStartParams:  val.RayStartParams,
		})
	}

	rayCluster := &api.Cluster{
		ClusterSpec: &api.ClusterSpec{
			HeadGroupSpec: &api.HeadGroupSpec{
				ComputeTemplate: headGroupSpec.ComputeTemplate,
				Image:           headGroupSpec.Image,
				ServiceType:     headGroupSpec.ServiceType,
				RayStartParams:  headGroupSpec.RayStartParams,
			},
			WorkerGroupSpec: workerGroupSpec,
		},
	}

	if err = validateRayCluster(rayCluster); err != nil {
		return nil, errors.Wrapf(errors.BadTaskSpecification, err, "invalid TaskSpecification [%v].", taskTemplate.Resources)
	}

	// convert *api.Cluster to v1alpha1.RayCluster
	rayClusterObject := util.NewRayCluster(rayCluster, map[string]*api.ComputeTemplate{})

	serviceAccountName := flytek8s.GetServiceAccountNameFromTaskExecutionMetadata(taskCtx.TaskExecutionMetadata())
	rayClusterObject.Spec.HeadGroupSpec.Template.Spec.ServiceAccountName = serviceAccountName
	for _, worker := range rayClusterObject.Spec.WorkerGroupSpecs {
		worker.Template.Spec.ServiceAccountName = serviceAccountName
	}

	return rayClusterObject, nil
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
		return pluginsCore.PhaseInfoSuccess(info), nil
	}
	return pluginsCore.PhaseInfoRunning(pluginsCore.DefaultPhaseVersion, info), nil
}

func init() {
	if err := rayv1alpha1.AddToScheme(scheme.Scheme); err != nil {
		panic(err)
	}

	pluginmachinery.PluginRegistry().RegisterK8sPlugin(
		k8s.PluginEntry{
			ID:                  rayTaskType,
			RegisteredTaskTypes: []pluginsCore.TaskType{rayTaskType},
			ResourceToWatch:     &rayv1alpha1.RayCluster{},
			Plugin:              rayClusterResourceHandler{},
			IsDefault:           false,
		})
}
