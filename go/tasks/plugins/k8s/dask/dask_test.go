package dask

import (
	"context"
	"testing"

	daskAPI "github.com/bstadlbauer/dask-k8s-operator-go-client/pkg/apis/kubernetes.dask.org/v1"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/plugins"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	pluginsCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/k8s"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/utils"
	"github.com/golang/protobuf/jsonpb"
	"google.golang.org/protobuf/types/known/structpb"
)


const (
	defaultTestImage = "image://"
	testNWorkers = 10
	testTaskID = "some-acceptable-name"
)

var(
	dummyEnvVars = []*core.KeyValuePair{
		{Key: "Env_Var", Value: "Env_Val"},
	}
	testArgs = []string{
		"execute-dask-task",
	}
)


func dummpyDaskCustomObj(customImage string, resources *core.Resources) *plugins.DaskJob {
	jobPodSpec := plugins.JobPodSpec{
		Image: customImage,
		Resources: resources,
	}
	
	cluster := plugins.DaskCluster{
		Image: customImage, 
		NWorkers: 10,
		Resources: resources,
	}

	daskJob := plugins.DaskJob{
		Namespace: "default",
		JobPodSpec: &jobPodSpec,
		Cluster: &cluster,
	}
	return &daskJob
}


func dummyDaskTaskTemplate(id string, customImage string, resources *core.Resources) *core.TaskTemplate {
	daskJob := dummpyDaskCustomObj(customImage, resources)
	daskJobJSON, err := utils.MarshalToString(daskJob)
	if err != nil {
		panic(err)
	}

	structObj := structpb.Struct{}
	err = jsonpb.UnmarshalString(daskJobJSON, &structObj)
	if err != nil {
		panic(err)
	}

	return &core.TaskTemplate{
		Id:   &core.Identifier{Name: id},
		Type: daskTaskType,
		Target: &core.TaskTemplate_Container{
			Container: &core.Container{
				Image: defaultTestImage,
				Args:  testArgs,
				Env:   dummyEnvVars,
			},
		},
		Custom: &structObj,
	}
}

func dummyDaskTaskContext(taskTemplate *core.TaskTemplate, resources *v1.ResourceRequirements) pluginsCore.TaskExecutionContext {
	taskCtx := &mocks.TaskExecutionContext{}

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
	tID.On("GetGeneratedName").Return(testTaskID)

	// TODO: Remove unused uncommented ones
	taskExecutionMetadata := &mocks.TaskExecutionMetadata{}
	taskExecutionMetadata.OnGetTaskExecutionID().Return(tID)
	// taskExecutionMetadata.OnGetNamespace().Return("test-namespace")
	taskExecutionMetadata.OnGetAnnotations().Return(map[string]string{"annotation-1": "val1"})
	taskExecutionMetadata.OnGetLabels().Return(map[string]string{"label-1": "val1"})
	// taskExecutionMetadata.OnGetOwnerReference().Return(v1Meta.OwnerReference{
	// 	Kind: "node",
	// 	Name: "blah",
	// })
	// taskExecutionMetadata.OnGetSecurityContext().Return(core.SecurityContext{
	// 	RunAs: &core.Identity{K8SServiceAccount: "new-val"},
	// })
	taskExecutionMetadata.OnGetMaxAttempts().Return(uint32(1))
	taskExecutionMetadata.OnIsInterruptible().Return(true)
	overrides := &mocks.TaskOverrides{}
	overrides.OnGetResources().Return(resources)
	taskExecutionMetadata.OnGetOverrides().Return(overrides)
	taskCtx.On("TaskExecutionMetadata").Return(taskExecutionMetadata)
	return taskCtx
}


func TestBuildResourceDaskHappyPath(t *testing.T) {
	taskName := "test-build-resource"
	daskResourceHandler := daskResourceHandler{}

	taskTemplate := dummyDaskTaskTemplate(taskName, "", nil)
	taskContext := dummyDaskTaskContext(taskTemplate, &v1.ResourceRequirements{})
	resource, err := daskResourceHandler.BuildResource(context.TODO(), taskContext)
	assert.Nil(t, err)
	assert.NotNil(t, resource)
	daskJob, ok := resource.(*daskAPI.DaskJob)
	assert.True(t, ok)

	// Job
	jobSpec := daskJob.Spec.Job.Spec
	assert.Equal(t, "job-runner", jobSpec.Containers[0].Name)
	assert.Equal(t, defaultTestImage, jobSpec.Containers[0].Image)
	assert.Equal(t, testArgs, jobSpec.Containers[0].Args)
	assert.Equal(t, v1.ResourceRequirements{}, jobSpec.Containers[0].Resources)

	// Scheduler
	schedulerSpec := daskJob.Spec.Cluster.Spec.Scheduler.Spec
	expectedPorts := []v1.ContainerPort{
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
	}
	assert.Equal(t, defaultTestImage, schedulerSpec.Containers[0].Image)
	assert.Equal(t, v1.ResourceRequirements{}, schedulerSpec.Containers[0].Resources)
	assert.Equal(t, []string{"dask-scheduler"}, schedulerSpec.Containers[0].Args)
	assert.Equal(t, expectedPorts, schedulerSpec.Containers[0].Ports)

	schedulerServiceSpec := daskJob.Spec.Cluster.Spec.Scheduler.Service
	expectedSelector := map[string]string{
		"dask.org/cluster-name": testTaskID,
		"dask.org/component":    "scheduler",
	}
	expectedSerivcePorts := []v1.ServicePort{
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
	}
	assert.Equal(t, v1.ServiceTypeNodePort, schedulerServiceSpec.Type)
	assert.Equal(t, expectedSelector, schedulerServiceSpec.Selector)
	assert.Equal(t, expectedSerivcePorts, schedulerServiceSpec.Ports)
	
	// Default Workers
	workerSpec := daskJob.Spec.Cluster.Spec.Worker.Spec
	assert.Equal(t, testNWorkers, daskJob.Spec.Cluster.Spec.Worker.Replicas)
	assert.Equal(t, "dask-worker", workerSpec.Containers[0].Name)
	assert.Equal(t, v1.PullIfNotPresent, workerSpec.Containers[0].ImagePullPolicy)
	assert.Equal(t, defaultTestImage, workerSpec.Containers[0].Image)
	assert.Equal(t, v1.ResourceRequirements{}, workerSpec.Containers[0].Resources)
	assert.Equal(t, []string{
		"dask-worker",
		"--name",
		"$(DASK_WORKER_NAME)",
	}, workerSpec.Containers[0].Args)
	assert.NotContains(t, workerSpec.Containers[0].Args, "--nthreads")
	assert.NotContains(t, workerSpec.Containers[0].Args, "--memory-limit")
}

func TestBuildResourceDaskCustomImages(t *testing.T) {
	customImage := "customImage"

	daskResourceHandler := daskResourceHandler{}
	taskTemplate := dummyDaskTaskTemplate("test-build-resource", customImage, nil)
	taskContext := dummyDaskTaskContext(taskTemplate, &v1.ResourceRequirements{})
	resource, err := daskResourceHandler.BuildResource(context.TODO(), taskContext)
	assert.Nil(t, err)
	assert.NotNil(t, resource)
	daskJob, ok := resource.(*daskAPI.DaskJob)
	assert.True(t, ok)

	// Job
	jobSpec := daskJob.Spec.Job.Spec
	assert.Equal(t, customImage, jobSpec.Containers[0].Image)

	// Scheduler
	schedulerSpec := daskJob.Spec.Cluster.Spec.Scheduler.Spec
	assert.Equal(t, customImage, schedulerSpec.Containers[0].Image)

	// Default Workers
	workerSpec := daskJob.Spec.Cluster.Spec.Worker.Spec
	assert.Equal(t, customImage, workerSpec.Containers[0].Image)
}


func TestBuildResourceDaskDefaultResoureRequirements(t *testing.T) {
	flyteWorkflowResources := v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU: resource.MustParse("1"),
		},
		Limits: v1.ResourceList{
			v1.ResourceCPU: resource.MustParse("2"),
			v1.ResourceMemory: resource.MustParse("2G"),
		},
	}

	daskResourceHandler := daskResourceHandler{}
	taskTemplate := dummyDaskTaskTemplate("test-build-resource", "", nil)
	taskContext := dummyDaskTaskContext(taskTemplate, &flyteWorkflowResources)
	resource, err := daskResourceHandler.BuildResource(context.TODO(), taskContext)
	assert.Nil(t, err)
	assert.NotNil(t, resource)
	daskJob, ok := resource.(*daskAPI.DaskJob)
	assert.True(t, ok)

	// Job
	jobSpec := daskJob.Spec.Job.Spec
	assert.Equal(t, flyteWorkflowResources, jobSpec.Containers[0].Resources)

	// Scheduler
	schedulerSpec := daskJob.Spec.Cluster.Spec.Scheduler.Spec
	assert.Equal(t, flyteWorkflowResources, schedulerSpec.Containers[0].Resources)

	// Default Workers
	workerSpec := daskJob.Spec.Cluster.Spec.Worker.Spec
	assert.Equal(t, flyteWorkflowResources, workerSpec.Containers[0].Resources)
	assert.Contains(t, workerSpec.Containers[0].Args, "--nthreads")
	assert.Contains(t, workerSpec.Containers[0].Args, "2")
	assert.Contains(t, workerSpec.Containers[0].Args, "--memory-limit")
	assert.Contains(t, workerSpec.Containers[0].Args, "2G")
}


func TestBuildResourcesDaskCustomResoureRequirements(t *testing.T) {
	protobufResources := core.Resources{
		Requests: []*core.Resources_ResourceEntry{
			{
				Name: core.Resources_CPU,
				Value: "5",
			},
		},
		Limits: []*core.Resources_ResourceEntry{
			{
				Name: core.Resources_CPU,
				Value: "10",
			},
			{
				Name: core.Resources_MEMORY,
				Value: "15G",
			},
		},
	}
	expectedResources, _ := convertProtobufResourcesToK8sResources(&protobufResources)

	flyteWorkflowResources := v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU: resource.MustParse("1"),
		},
		Limits: v1.ResourceList{
			v1.ResourceCPU: resource.MustParse("2"),
			v1.ResourceMemory: resource.MustParse("2G"),
		},
	}

	daskResourceHandler := daskResourceHandler{}
	taskTemplate := dummyDaskTaskTemplate("test-build-resource", "", &protobufResources)
	taskContext := dummyDaskTaskContext(taskTemplate, &flyteWorkflowResources)
	resource, err := daskResourceHandler.BuildResource(context.TODO(), taskContext)
	assert.Nil(t, err)
	assert.NotNil(t, resource)
	daskJob, ok := resource.(*daskAPI.DaskJob)
	assert.True(t, ok)

	// Job
	jobSpec := daskJob.Spec.Job.Spec
	assert.Equal(t, *expectedResources, jobSpec.Containers[0].Resources)

	// Scheduler
	schedulerSpec := daskJob.Spec.Cluster.Spec.Scheduler.Spec
	assert.Equal(t, *expectedResources, schedulerSpec.Containers[0].Resources)

	// Default Workers
	workerSpec := daskJob.Spec.Cluster.Spec.Worker.Spec
	assert.Equal(t, *expectedResources, workerSpec.Containers[0].Resources)
	assert.Contains(t, workerSpec.Containers[0].Args, "--nthreads")
	assert.Contains(t, workerSpec.Containers[0].Args, "10")
	assert.Contains(t, workerSpec.Containers[0].Args, "--memory-limit")
	assert.Contains(t, workerSpec.Containers[0].Args, "15G")
}


func TestGetPropertiesDask(t *testing.T) {
	daskResourceHandler := daskResourceHandler{}
	expected := k8s.PluginProperties{}
	assert.Equal(t, expected, daskResourceHandler.GetProperties())
}

func TestBuildIdentityResourceDask(t *testing.T) {
	daskResourceHandler := daskResourceHandler{}
	expected := &daskAPI.DaskJob{
		TypeMeta: metav1.TypeMeta{
			Kind:       KindDaskJob,
			APIVersion: daskAPI.SchemeGroupVersion.String(),
		},
	}

	taskTemplate := dummyDaskTaskTemplate("test-build-resource", "", nil)
	taskContext := dummyDaskTaskContext(taskTemplate, &v1.ResourceRequirements{})
	identityResources, err := daskResourceHandler.BuildIdentityResource(context.TODO(), taskContext.TaskExecutionMetadata())
	if err != nil {
		panic(err)
	}
	assert.Equal(t, expected, identityResources)
}