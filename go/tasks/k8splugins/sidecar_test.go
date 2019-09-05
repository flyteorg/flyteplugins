package k8splugins

import (
	"context"
	"io/ioutil"
	"k8s.io/apimachinery/pkg/api/resource"
	"os"
	"path"
	"testing"

	"github.com/lyft/flytestdlib/storage"
	"github.com/stretchr/testify/mock"

	"github.com/golang/protobuf/jsonpb"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/lyft/flyteplugins/go/tasks/flytek8s/config"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/utils"

	pluginsCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	pluginsCoreMock "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	pluginsIOMock "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io/mocks"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/k8s"
)

const ResourceNvidiaGPU = "nvidia.com/gpu"

func getSidecarTaskTemplateForTest(sideCarJob plugins.SidecarJob) *core.TaskTemplate {
	sidecarJSON, err := utils.MarshalToString(&sideCarJob)
	if err != nil {
		panic(err)
	}
	structObj := structpb.Struct{}
	err = jsonpb.UnmarshalString(sidecarJSON, &structObj)
	if err != nil {
		panic(err)
	}
	return &core.TaskTemplate{
		Custom: &structObj,
	}
}

func getDummySidecarTaskContext(taskTemplate *core.TaskTemplate, resources *v1.ResourceRequirements) pluginsCore.TaskExecutionContext {
	taskCtx := &pluginsCoreMock.TaskExecutionContext{}
	dummyTaskMetadata := dummyContainerTaskMetadata(resources)
	inputReader := &pluginsIOMock.InputReader{}
	inputReader.On("GetInputPath").Return(storage.DataReference("test-data-reference"))
	inputReader.On("Get", mock.Anything).Return(&core.LiteralMap{}, nil)
	taskCtx.On("InputReader").Return(inputReader)

	outputReader := &pluginsIOMock.OutputWriter{}
	outputReader.On("GetOutputPath").Return(storage.DataReference("/data/outputs.pb"))
	outputReader.On("GetOutputPrefixPath").Return(storage.DataReference("/data/"))
	taskCtx.On("OutputWriter").Return(outputReader)

	taskReader := &pluginsCoreMock.TaskReader{}
	taskReader.On("Read", mock.Anything).Return(taskTemplate, nil)
	taskCtx.On("TaskReader").Return(taskReader)

	taskCtx.On("TaskExecutionMetadata").Return(dummyTaskMetadata)
	return taskCtx
}

func TestBuildSidecarResource(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	sidecarCustomJSON, err := ioutil.ReadFile(path.Join(dir, "mocks", "sidecar_custom"))
	if err != nil {
		t.Fatal(sidecarCustomJSON)
	}
	sidecarCustom := structpb.Struct{}
	if err := jsonpb.UnmarshalString(string(sidecarCustomJSON), &sidecarCustom); err != nil {
		t.Fatal(err)
	}
	task := core.TaskTemplate{
		Custom: &sidecarCustom,
	}

	tolGPU := v1.Toleration{
		Key:      "flyte/gpu",
		Value:    "dedicated",
		Operator: v1.TolerationOpEqual,
		Effect:   v1.TaintEffectNoSchedule,
	}

	tolStorage := v1.Toleration{
		Key:      "storage",
		Value:    "dedicated",
		Operator: v1.TolerationOpExists,
		Effect:   v1.TaintEffectNoSchedule,
	}
	assert.NoError(t, config.SetK8sPluginConfig(&config.K8sPluginConfig{
		ResourceTolerations: map[v1.ResourceName][]v1.Toleration{
			v1.ResourceStorage: {tolStorage},
			ResourceNvidiaGPU:  {tolGPU},
		},
	}))
	handler := &sidecarResourceHandler{}
	taskCtx := getDummySidecarTaskContext(&task, &v1.ResourceRequirements{
		Limits: v1.ResourceList{
			v1.ResourceCPU:     resource.MustParse("1024m"),
			v1.ResourceStorage: resource.MustParse("100M"),
		},
	})
	resource, err := handler.BuildResource(context.TODO(), taskCtx)
	assert.Nil(t, err)
	assert.EqualValues(t, map[string]string{
		primaryContainerKey: "a container",
	}, resource.GetAnnotations())
	assert.Contains(t, resource.(*v1.Pod).Spec.Tolerations, tolGPU)
}

func TestBuildSidecarResourceMissingPrimary(t *testing.T) {
	sideCarJob := plugins.SidecarJob{
		PrimaryContainerName: "PrimaryContainer",
		PodSpec: &v1.PodSpec{
			Containers: []v1.Container{
				{
					Name: "SecondaryContainer",
				},
			},
		},
	}

	task := getSidecarTaskTemplateForTest(sideCarJob)

	handler := &sidecarResourceHandler{}
	taskCtx := getDummySidecarTaskContext(task, resourceRequirements)
	_, err := handler.BuildResource(context.TODO(), taskCtx)
	assert.EqualError(t, err,
		"task failed, BadTaskSpecification: invalid Sidecar task, primary container [PrimaryContainer] not defined")
}

func TestGetTaskSidecarStatus(t *testing.T) {
	sideCarJob := plugins.SidecarJob{
		PrimaryContainerName: "PrimaryContainer",
		PodSpec: &v1.PodSpec{
			Containers: []v1.Container{
				{
					Name: "PrimaryContainer",
				},
			},
		},
	}

	task := getSidecarTaskTemplateForTest(sideCarJob)

	var testCases = map[v1.PodPhase]pluginsCore.Phase{
		v1.PodSucceeded:           pluginsCore.PhaseSuccess,
		v1.PodFailed:              pluginsCore.PhaseRetryableFailure,
		v1.PodReasonUnschedulable: pluginsCore.PhaseQueued,
		v1.PodUnknown:             pluginsCore.PhaseUndefined,
	}

	for podPhase, expectedTaskPhase := range testCases {
		var resource k8s.Resource
		resource = &v1.Pod{
			Status: v1.PodStatus{
				Phase: podPhase,
			},
		}
		resource.SetAnnotations(map[string]string{
			primaryContainerKey: "PrimaryContainer",
		})
		handler := &sidecarResourceHandler{}
		taskCtx := getDummySidecarTaskContext(task, resourceRequirements)
		phaseInfo, err := handler.GetTaskPhase(context.TODO(), taskCtx, resource)
		assert.Nil(t, err)
		assert.Equal(t, expectedTaskPhase, phaseInfo.Phase(),
			"Expected [%v] got [%v] instead", expectedTaskPhase, phaseInfo.Phase())
	}
}

func TestDemystifiedSidecarStatus_PrimaryFailed(t *testing.T) {
	var resource k8s.Resource
	resource = &v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
			ContainerStatuses: []v1.ContainerStatus{
				{
					Name: "Primary",
					State: v1.ContainerState{
						Terminated: &v1.ContainerStateTerminated{
							ExitCode: 1,
						},
					},
				},
			},
		},
	}
	resource.SetAnnotations(map[string]string{
		primaryContainerKey: "Primary",
	})
	handler := &sidecarResourceHandler{}
	taskCtx := getDummySidecarTaskContext(&core.TaskTemplate{}, resourceRequirements)
	phaseInfo, err := handler.GetTaskPhase(context.TODO(), taskCtx, resource)
	assert.Nil(t, err)
	assert.Equal(t, pluginsCore.PhaseRetryableFailure, phaseInfo.Phase())
}

func TestDemystifiedSidecarStatus_PrimarySucceeded(t *testing.T) {
	var resource k8s.Resource
	resource = &v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
			ContainerStatuses: []v1.ContainerStatus{
				{
					Name: "Primary",
					State: v1.ContainerState{
						Terminated: &v1.ContainerStateTerminated{
							ExitCode: 0,
						},
					},
				},
			},
		},
	}
	resource.SetAnnotations(map[string]string{
		primaryContainerKey: "Primary",
	})
	handler := &sidecarResourceHandler{}
	taskCtx := getDummySidecarTaskContext(&core.TaskTemplate{}, resourceRequirements)
	phaseInfo, err := handler.GetTaskPhase(context.TODO(), taskCtx, resource)
	assert.Nil(t, err)
	assert.Equal(t, pluginsCore.PhaseSuccess, phaseInfo.Phase())
}

func TestDemystifiedSidecarStatus_PrimaryRunning(t *testing.T) {
	var resource k8s.Resource
	resource = &v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
			ContainerStatuses: []v1.ContainerStatus{
				{
					Name: "Primary",
					State: v1.ContainerState{
						Waiting: &v1.ContainerStateWaiting{
							Reason: "stay patient",
						},
					},
				},
			},
		},
	}
	resource.SetAnnotations(map[string]string{
		primaryContainerKey: "Primary",
	})
	handler := &sidecarResourceHandler{}
	taskCtx := getDummySidecarTaskContext(&core.TaskTemplate{}, resourceRequirements)
	phaseInfo, err := handler.GetTaskPhase(context.TODO(), taskCtx, resource)
	assert.Nil(t, err)
	assert.Equal(t, pluginsCore.PhaseRunning, phaseInfo.Phase())
}

func TestDemystifiedSidecarStatus_PrimaryMissing(t *testing.T) {
	var resource k8s.Resource
	resource = &v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
			ContainerStatuses: []v1.ContainerStatus{
				{
					Name: "Secondary",
				},
			},
		},
	}
	resource.SetAnnotations(map[string]string{
		primaryContainerKey: "Primary",
	})
	handler := &sidecarResourceHandler{}
	taskCtx := getDummySidecarTaskContext(&core.TaskTemplate{}, resourceRequirements)
	phaseInfo, err := handler.GetTaskPhase(context.TODO(), taskCtx, resource)
	assert.Nil(t, err)
	assert.Equal(t, pluginsCore.PhasePermanentFailure, phaseInfo.Phase())
}