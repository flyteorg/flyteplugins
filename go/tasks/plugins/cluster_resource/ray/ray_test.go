package ray

import (
	"context"
	"testing"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	pluginsCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/flytek8s"
	pluginIOMocks "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/io/mocks"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/k8s"
	rayv1alpha1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	corev1 "k8s.io/api/core/v1"
	k8sV1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const testImage = "image://"
const serviceAccount = "ray_sa"

var (
	dummyEnvVars = []*core.KeyValuePair{
		{Key: "Env_Var", Value: "Env_Val"},
	}

	testArgs = []string{
		"test-args",
	}

	resourceRequirements = &corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:         resource.MustParse("1000m"),
			corev1.ResourceMemory:      resource.MustParse("1Gi"),
			flytek8s.ResourceNvidiaGPU: resource.MustParse("1"),
		},
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:         resource.MustParse("100m"),
			corev1.ResourceMemory:      resource.MustParse("512Mi"),
			flytek8s.ResourceNvidiaGPU: resource.MustParse("1"),
		},
	}

	clusterName     = "testRayCluster"
	rayImage        = "rayproject/ray:1.8.0"
	workerGroupName = "worker-group"
)

func dummyRayCustomObj() *core.RayCluster {
	return &core.RayCluster{
		Name: "testRayCluster",
		ClusterSpec: &core.ClusterSpec{
			HeadGroupSpec:   &core.HeadGroupSpec{Image: "rayproject/ray:1.8.0", ServiceType: "NodePort"},
			WorkerGroupSpec: []*core.WorkerGroupSpec{{GroupName: "test-group", Replicas: 3}},
		},
	}
}

func dummyRayTaskTemplate(id string) *core.TaskTemplate {
	return &core.TaskTemplate{
		Id:   &core.Identifier{Name: id},
		Type: "container",
		Target: &core.TaskTemplate_Container{
			Container: &core.Container{
				Image: testImage,
				Args:  testArgs,
				Env:   dummyEnvVars,
			},
		},
		Resources: map[string]*core.Resource{id: {Value: &core.Resource_Ray{Ray: &core.RayCluster{
			Name: clusterName,
			ClusterSpec: &core.ClusterSpec{
				HeadGroupSpec:   &core.HeadGroupSpec{Image: rayImage, ServiceType: "ClusterIP"},
				WorkerGroupSpec: []*core.WorkerGroupSpec{{Image: rayImage, GroupName: workerGroupName, Replicas: 3}},
			},
		}}}},
	}
}

func dummyRayTaskContext(taskTemplate *core.TaskTemplate) pluginsCore.TaskExecutionContext {
	taskCtx := &mocks.TaskExecutionContext{}
	inputReader := &pluginIOMocks.InputReader{}
	inputReader.OnGetInputPrefixPath().Return("/input/prefix")
	inputReader.OnGetInputPath().Return("/input")
	inputReader.OnGetMatch(mock.Anything).Return(&core.LiteralMap{}, nil)
	taskCtx.OnInputReader().Return(inputReader)

	outputReader := &pluginIOMocks.OutputWriter{}
	outputReader.OnGetOutputPath().Return("/data/outputs.pb")
	outputReader.OnGetOutputPrefixPath().Return("/data/")
	outputReader.OnGetRawOutputPrefix().Return("")
	outputReader.OnGetCheckpointPrefix().Return("/checkpoint")
	outputReader.OnGetPreviousCheckpointsPrefix().Return("/prev")
	taskCtx.OnOutputWriter().Return(outputReader)

	taskReader := &mocks.TaskReader{}
	taskReader.OnReadMatch(mock.Anything).Return(taskTemplate, nil)
	taskCtx.OnTaskReader().Return(taskReader)

	tID := &mocks.TaskExecutionID{}
	tID.OnGetID().Return(core.TaskExecutionIdentifier{
		NodeExecutionId: &core.NodeExecutionIdentifier{
			ExecutionId: &core.WorkflowExecutionIdentifier{
				Name:    "my_name",
				Project: "my_project",
				Domain:  "my_domain",
			},
		},
	})
	tID.OnGetGeneratedName().Return("some-acceptable-name")

	resources := &mocks.TaskOverrides{}
	resources.OnGetResources().Return(resourceRequirements)

	taskExecutionMetadata := &mocks.TaskExecutionMetadata{}
	taskExecutionMetadata.OnGetTaskExecutionID().Return(tID)
	taskExecutionMetadata.OnGetNamespace().Return("test-namespace")
	taskExecutionMetadata.OnGetAnnotations().Return(map[string]string{"annotation-1": "val1"})
	taskExecutionMetadata.OnGetLabels().Return(map[string]string{"label-1": "val1"})
	taskExecutionMetadata.OnGetOwnerReference().Return(v1.OwnerReference{
		Kind: "node",
		Name: "blah",
	})
	taskExecutionMetadata.OnIsInterruptible().Return(true)
	taskExecutionMetadata.OnGetOverrides().Return(resources)
	taskExecutionMetadata.OnGetK8sServiceAccount().Return(serviceAccount)
	taskExecutionMetadata.OnGetPlatformResources().Return(&corev1.ResourceRequirements{})
	taskExecutionMetadata.OnGetSecurityContext().Return(core.SecurityContext{RunAs: &core.Identity{K8SServiceAccount: serviceAccount}})
	taskCtx.OnTaskExecutionMetadata().Return(taskExecutionMetadata)
	return taskCtx
}

func TestBuildResourceRay(t *testing.T) {
	rayResourceHandler := rayClusterResourceHandler{}
	taskTemplate := dummyRayTaskTemplate("ray-id")

	RayResource, err := rayResourceHandler.BuildResource(context.TODO(), dummyRayTaskContext(taskTemplate))
	assert.Nil(t, err)

	assert.NotNil(t, RayResource)
	ray, ok := RayResource.(*rayv1alpha1.RayCluster)
	assert.True(t, ok)
	assert.Equal(t, ray.Name, clusterName)

	headReplica := int32(1)
	assert.Equal(t, ray.Spec.HeadGroupSpec.Replicas, &headReplica)
	assert.Equal(t, ray.Spec.HeadGroupSpec.ServiceType, k8sV1.ServiceType("ClusterIP"))
	assert.Equal(t, ray.Spec.HeadGroupSpec.Template.Spec.ServiceAccountName, serviceAccount)
	assert.Equal(t, ray.Spec.HeadGroupSpec.RayStartParams, map[string]string{"node-ip-address": "$MY_POD_IP"})

	workerReplica := int32(3)
	assert.Equal(t, ray.Spec.WorkerGroupSpecs[0].Replicas, &workerReplica)
	assert.Equal(t, ray.Spec.WorkerGroupSpecs[0].MinReplicas, &workerReplica)
	assert.Equal(t, ray.Spec.WorkerGroupSpecs[0].MaxReplicas, &workerReplica)
	assert.Equal(t, ray.Spec.WorkerGroupSpecs[0].GroupName, workerGroupName)
	assert.Equal(t, ray.Spec.WorkerGroupSpecs[0].Template.Spec.ServiceAccountName, serviceAccount)
	assert.Equal(t, ray.Spec.WorkerGroupSpecs[0].RayStartParams, map[string]string{"node-ip-address": "$MY_POD_IP"})
}

func TestGetPropertiesRay(t *testing.T) {
	rayResourceHandler := rayClusterResourceHandler{}
	expected := k8s.PluginProperties{}
	assert.Equal(t, expected, rayResourceHandler.GetProperties())
}
