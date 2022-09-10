package dask

import (
	"context"
	"time"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/scheme"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery"
	pluginsCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/k8s"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "k8s.io/api/core/v1"

	daskAPI "github.com/bstadlbauer/dask-k8s-operator-go-client/pkg/apis/kubernetes.dask.org/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	daskTaskType    = "dask"
	KindDaskCluster = "DaskCluster"
)


type daskResourceHandler struct {
}

func (daskResourceHandler) BuildIdentityResource(_ context.Context, _ pluginsCore.TaskExecutionMetadata) (
	client.Object, error) {
	return &daskAPI.DaskCluster{
		TypeMeta: metav1.TypeMeta{
			Kind:       KindDaskCluster,
			APIVersion: daskAPI.SchemeGroupVersion.String(),
		},
	}, nil
}

func (p daskResourceHandler) BuildResource(ctx context.Context, taskCtx pluginsCore.TaskExecutionContext) (client.Object, error) {
	cluster := &daskAPI.DaskCluster{
		TypeMeta: metav1.TypeMeta{
			Kind:       KindDaskCluster,
			APIVersion: daskAPI.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "my-cluster", // FIXME
		},
		Spec: daskAPI.DaskClusterSpec{
			Worker: daskAPI.WorkerSpec{
				Replicas: 1,
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:            "scheduler",
							Image:           "ghcr.io/dask/dask:latest",
							ImagePullPolicy: "IfNotPresent",
							Args: []string{
								"dask-worker",
								"tcp://simple-cluster-service.default.svc.cluster.local:8786",
								"--name",
								"my-worker",
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
			},
			Scheduler: daskAPI.SchedulerSpec{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "scheduler",
							Image: "ghcr.io/dask/dask:latest",
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
						"dask.org/cluster-name": "simple-cluster",
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
			},
		},
	}
	return cluster, nil
}

func (p daskResourceHandler) GetTaskPhase(ctx context.Context, pluginContext k8s.PluginContext, r client.Object) (pluginsCore.PhaseInfo, error) {
	occurredAt := time.Now()
	info := &pluginsCore.TaskInfo{}
	return pluginsCore.PhaseInfoInitializing(occurredAt, pluginsCore.DefaultPhaseVersion, "job submitted", info), nil
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
			ResourceToWatch:     &daskAPI.DaskCluster{},
			Plugin:              daskResourceHandler{},
			IsDefault:           false,
		})
}
