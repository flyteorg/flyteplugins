package flytek8s

import (
	"context"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/stretchr/testify/assert"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/lyft/flyteplugins/go/tasks/v1/flytek8s/config"

	pluginsCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/core"
	pluginsCoreMock "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/core/mocks"
)

func dummyTaskExecutionMetadata(resources *v1.ResourceRequirements) pluginsCore.TaskExecutionMetadata {
	taskExecutionMetadata := &pluginsCoreMock.TaskExecutionMetadata{}
	taskExecutionMetadata.On("GetNamespace").Return("test-namespace")
	taskExecutionMetadata.On("GetAnnotations").Return(map[string]string{"annotation-1": "val1"})
	taskExecutionMetadata.On("GetLabels").Return(map[string]string{"label-1": "val1"})
	taskExecutionMetadata.On("GetOwnerReference").Return(metaV1.OwnerReference{
		Kind: "node",
		Name: "blah",
	})
	taskExecutionMetadata.On("GetK8sServiceAccount").Return("service-account")
	tID := &pluginsCoreMock.TaskExecutionID{}
	tID.On("GetID").Return(core.TaskExecutionIdentifier{
		NodeExecutionId: &core.NodeExecutionIdentifier{
			ExecutionId: &core.WorkflowExecutionIdentifier{
				Name:    "my_name",
				Project: "my_project",
				Domain:  "my_domain",
			},
		},
	})
	tID.On("GetGeneratedName").Return("some-acceptable-name")
	taskExecutionMetadata.On("GetTaskExecutionID").Return(tID)

	to := &pluginsCoreMock.TaskOverrides{}
	to.On("GetResources").Return(resources)
	taskExecutionMetadata.On("GetOverrides").Return(to)

	return taskExecutionMetadata
}

func dummyContainerTaskContext(resources *v1.ResourceRequirements, command []string, args []string) pluginsCore.TaskExecutionContext {
	taskCtx := &pluginsCoreMock.TaskExecutionContext{}

	task := &core.TaskTemplate{
		Type: "test",
		Target: &core.TaskTemplate_Container{
			Container: &core.Container{
				Command: command,
				Args:    args,
			},
		},
	}

	taskReader := &pluginsCoreMock.TaskReader{}
	taskReader.On("Read", mock.Anything).Return(task, nil)
	taskCtx.On("TaskReader").Return(taskReader)

	dummyTaskMetadata := dummyTaskExecutionMetadata(resources)
	taskCtx.On("TaskExecutionMetadata").Return(dummyTaskMetadata)
	return taskCtx
}

func TestAddFlyteModificationsForPodSpec(t *testing.T) {
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
		}}),
	)

	t.Run("WithGPU", func(t *testing.T) {
		resourceRequirements := v1.ResourceRequirements{
				Limits: v1.ResourceList{
					v1.ResourceCPU:     resource.MustParse("1024m"),
					ResourceNvidiaGPU:  resource.MustParse("1"),
				},
				Requests: v1.ResourceList{
					v1.ResourceCPU:     resource.MustParse("1024m"),
				},
		}
		var podSpec v1.PodSpec
		AddFlyteModificationsForPodSpec(
			dummyContainerTaskContext(&resourceRequirements, []string{"command"}, []string{"args"}),
			[]v1.Container{}, []v1.ResourceRequirements{resourceRequirements}, &podSpec)
		assert.Equal(t, len(podSpec.Tolerations), 1)
	})

	t.Run("NoGPU", func(t *testing.T) {
		resourceRequirements := v1.ResourceRequirements{
			Limits: v1.ResourceList{
				v1.ResourceCPU:     resource.MustParse("1024m"),
			},
			Requests: v1.ResourceList{
				v1.ResourceCPU:     resource.MustParse("1024m"),
			},
		}

		var podSpec v1.PodSpec
		AddFlyteModificationsForPodSpec(
			dummyContainerTaskContext(&resourceRequirements, []string{"command"}, []string{"args"}),
			[]v1.Container{
				{
					Name: "some-acceptable-name",
				},
			}, []v1.ResourceRequirements{resourceRequirements}, &podSpec)
		assert.Equal(t, len(podSpec.Tolerations), 0)
		assert.Equal(t, "some-acceptable-name", podSpec.Containers[0].Name)
	})
}

func TestDemystifySuccess(t *testing.T) {
	genuineSuccess := &v1.PodStatus{
		Phase:                 v1.PodSucceeded,
		ContainerStatuses: []v1.ContainerStatus{
			{
				State: v1.ContainerState{
					Terminated: &v1.ContainerStateTerminated{
						ExitCode:    0,
						Reason:      "Container terminated successfully",
					},
				},
			},
		},
	}
	phaseInfo, err := DemystifySuccess(genuineSuccess, pluginsCore.TaskInfo{})
	assert.Nil(t, err)
	assert.Equal(t, pluginsCore.PhaseSuccess, phaseInfo.Phase())

	oomKilled := &v1.PodStatus{
		Phase:                 v1.PodSucceeded,
		ContainerStatuses: []v1.ContainerStatus{
			{
				State: v1.ContainerState{
					Terminated: &v1.ContainerStateTerminated{
						ExitCode:    0,
						Reason:      OOMKilled,
					},
				},
			},
		},
	}
	phaseInfo, err = DemystifySuccess(oomKilled, pluginsCore.TaskInfo{})
	assert.Nil(t, err)
	assert.Equal(t, pluginsCore.PhaseRetryableFailure, phaseInfo.Phase())
}

func TestDemystifyPending(t *testing.T) {

	t.Run("PodNotScheduled", func(t *testing.T) {
		s := v1.PodStatus{
			Phase: v1.PodPending,
			Conditions: []v1.PodCondition{
				{
					Type:   v1.PodScheduled,
					Status: v1.ConditionFalse,
				},
			},
		}
		taskStatus, err := DemystifyPending(s)
		assert.NoError(t, err)
		assert.Equal(t, pluginsCore.PhaseQueued, taskStatus.Phase())
	})

	t.Run("PodUnschedulable", func(t *testing.T) {
		s := v1.PodStatus{
			Phase: v1.PodPending,
			Conditions: []v1.PodCondition{
				{
					Type:   v1.PodReasonUnschedulable,
					Status: v1.ConditionFalse,
				},
			},
		}
		taskStatus, err := DemystifyPending(s)
		assert.NoError(t, err)
		assert.Equal(t, pluginsCore.PhaseQueued, taskStatus.Phase())
	})

	t.Run("PodNotScheduled", func(t *testing.T) {
		s := v1.PodStatus{
			Phase: v1.PodPending,
			Conditions: []v1.PodCondition{
				{
					Type:   v1.PodScheduled,
					Status: v1.ConditionTrue,
				},
			},
		}
		taskStatus, err := DemystifyPending(s)
		assert.NoError(t, err)
		assert.Equal(t, pluginsCore.PhaseQueued, taskStatus.Phase())
	})

	t.Run("PodUnschedulable", func(t *testing.T) {
		s := v1.PodStatus{
			Phase: v1.PodPending,
			Conditions: []v1.PodCondition{
				{
					Type:   v1.PodReasonUnschedulable,
					Status: v1.ConditionUnknown,
				},
			},
		}
		taskStatus, err := DemystifyPending(s)
		assert.NoError(t, err)
		assert.Equal(t, pluginsCore.PhaseQueued, taskStatus.Phase())
	})

	s := v1.PodStatus{
		Phase: v1.PodPending,
		Conditions: []v1.PodCondition{
			{
				Type:   v1.PodReady,
				Status: v1.ConditionFalse,
			},
			{
				Type:   v1.PodReasonUnschedulable,
				Status: v1.ConditionUnknown,
			},
			{
				Type:   v1.PodScheduled,
				Status: v1.ConditionTrue,
			},
		},
	}

	t.Run("ContainerCreating", func(t *testing.T) {
		s.ContainerStatuses = []v1.ContainerStatus{
			{
				Ready: false,
				State: v1.ContainerState{
					Waiting: &v1.ContainerStateWaiting{
						Reason:  "ContainerCreating",
						Message: "this is not an error",
					},
				},
			},
		}
		taskStatus, err := DemystifyPending(s)
		assert.NoError(t, err)
		assert.Equal(t, pluginsCore.PhaseInitializing, taskStatus.Phase())
	})

	t.Run("ErrImagePull", func(t *testing.T) {
		s.ContainerStatuses = []v1.ContainerStatus{
			{
				Ready: false,
				State: v1.ContainerState{
					Waiting: &v1.ContainerStateWaiting{
						Reason:  "ErrImagePull",
						Message: "this is not an error",
					},
				},
			},
		}
		taskStatus, err := DemystifyPending(s)
		assert.NoError(t, err)
		assert.Equal(t, pluginsCore.PhaseInitializing, taskStatus.Phase())
	})

	t.Run("PodInitializing", func(t *testing.T) {
		s.ContainerStatuses = []v1.ContainerStatus{
			{
				Ready: false,
				State: v1.ContainerState{
					Waiting: &v1.ContainerStateWaiting{
						Reason:  "PodInitializing",
						Message: "this is not an error",
					},
				},
			},
		}
		taskStatus, err := DemystifyPending(s)
		assert.NoError(t, err)
		assert.Equal(t, pluginsCore.PhaseInitializing, taskStatus.Phase())
	})

	t.Run("ImagePullBackOff", func(t *testing.T) {
		s.ContainerStatuses = []v1.ContainerStatus{
			{
				Ready: false,
				State: v1.ContainerState{
					Waiting: &v1.ContainerStateWaiting{
						Reason:  "ImagePullBackOff",
						Message: "this is an error",
					},
				},
			},
		}
		taskStatus, err := DemystifyPending(s)
		assert.NoError(t, err)
		assert.Equal(t, pluginsCore.PhaseRetryableFailure, taskStatus.Phase())
	})

	t.Run("InvalidImageName", func(t *testing.T) {
		s.ContainerStatuses = []v1.ContainerStatus{
			{
				Ready: false,
				State: v1.ContainerState{
					Waiting: &v1.ContainerStateWaiting{
						Reason:  "InvalidImageName",
						Message: "this is an error",
					},
				},
			},
		}
		taskStatus, err := DemystifyPending(s)
		assert.NoError(t, err)
		assert.Equal(t, pluginsCore.PhaseRetryableFailure, taskStatus.Phase())
	})

	t.Run("RegistryUnavailable", func(t *testing.T) {
		s.ContainerStatuses = []v1.ContainerStatus{
			{
				Ready: false,
				State: v1.ContainerState{
					Waiting: &v1.ContainerStateWaiting{
						Reason:  "RegistryUnavailable",
						Message: "this is an error",
					},
				},
			},
		}
		taskStatus, err := DemystifyPending(s)
		assert.NoError(t, err)
		assert.Equal(t, pluginsCore.PhaseRetryableFailure, taskStatus.Phase())
	})

	t.Run("RandomError", func(t *testing.T) {
		s.ContainerStatuses = []v1.ContainerStatus{
			{
				Ready: false,
				State: v1.ContainerState{
					Waiting: &v1.ContainerStateWaiting{
						Reason:  "RandomError",
						Message: "this is an error",
					},
				},
			},
		}
		taskStatus, err := DemystifyPending(s)
		assert.NoError(t, err)
		assert.Equal(t, pluginsCore.PhaseRetryableFailure, taskStatus.Phase())
	})
}

func TestConvertPodFailureToError(t *testing.T) {
	t.Run("unknown-error", func(t *testing.T) {
		code, _ := ConvertPodFailureToError(v1.PodStatus{})
		assert.Equal(t, code, "UnknownError")
	})

	t.Run("known-error", func(t *testing.T) {
		code, _ := ConvertPodFailureToError(v1.PodStatus{Reason: "hello"})
		assert.Equal(t, code, "hello")
	})
}

func getRunningPod() *v1.Pod{
	return &v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
			ContainerStatuses: []v1.ContainerStatus{
				{
					LastTerminationState: v1.ContainerState{
						Running:    &v1.ContainerStateRunning{
							StartedAt: metaV1.NewTime(time.Now()),
						},
					},
				},
			},
		},
	}
}

func getTerminatedPod(phase v1.PodPhase) *v1.Pod{
	pod := getRunningPod()
	pod.Status.Phase = phase
	pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, v1.ContainerStatus{
			LastTerminationState: v1.ContainerState{
				Terminated:    &v1.ContainerStateTerminated{
					StartedAt: metaV1.NewTime(pod.Status.ContainerStatuses[0].LastTerminationState.Running.StartedAt.Add(30 *time.Second)),
					FinishedAt: metaV1.NewTime(pod.Status.ContainerStatuses[0].LastTerminationState.Running.StartedAt.Add(time.Minute)),
				},
			},
		})
	return pod
}

func TestGetTaskPhaseFromPod(t *testing.T) {
	ctx := context.TODO()
	t.Run("running", func(t *testing.T) {
		job := getRunningPod()
		phaseInfo, err := GetTaskPhaseFromPod(ctx, job, UserOnly)
		assert.NoError(t, err)
		assert.Equal(t, pluginsCore.PhaseRunning, phaseInfo.Phase())
		assert.Equal(t, job.Status.ContainerStatuses[0].LastTerminationState.Running.StartedAt.Unix(),
			phaseInfo.Info().OccurredAt.Unix())
	})

	t.Run("queued", func(t *testing.T) {
		job := getRunningPod()
		job.Status.Phase = v1.PodPending
		phaseInfo, err := GetTaskPhaseFromPod(ctx, job, UserOnly)
		assert.NoError(t, err)
		assert.Equal(t, pluginsCore.PhaseQueued, phaseInfo.Phase())
	})

	t.Run("failNoCondition", func(t *testing.T) {
		job := getTerminatedPod(v1.PodFailed)
		phaseInfo, err := GetTaskPhaseFromPod(ctx, job, UserOnly)
		assert.NoError(t, err)
		assert.Equal(t, pluginsCore.PhaseRetryableFailure, phaseInfo.Phase())
		ec := phaseInfo.Err().GetCode()
		assert.Equal(t, "UnknownError", ec)
		assert.Equal(t, job.Status.ContainerStatuses[1].LastTerminationState.Terminated.StartedAt.Unix(),
			phaseInfo.Info().OccurredAt.Unix())
	})

	t.Run("failConditionUnschedulable", func(t *testing.T) {
		job := getTerminatedPod(v1.PodFailed)
		job.Status.Reason = "Unschedulable"
		job.Status.Message = "some message"
		job.Status.Conditions = []v1.PodCondition{
			{
				Type: v1.PodReasonUnschedulable,
			},
		}
		phaseInfo, err := GetTaskPhaseFromPod(ctx, job, UserOnly)
		assert.NoError(t, err)
		assert.Equal(t, pluginsCore.PhaseRetryableFailure, phaseInfo.Phase())
		ec := phaseInfo.Err().GetCode()
		assert.Equal(t, "Unschedulable", ec)
	})

	t.Run("success", func(t *testing.T) {
		job := getTerminatedPod(v1.PodSucceeded)
		phaseInfo, err := GetTaskPhaseFromPod(ctx, job, UserOnly)
		assert.NoError(t, err)
		assert.NotNil(t, phaseInfo)
		assert.Equal(t, pluginsCore.PhaseSuccess, phaseInfo.Phase())
		assert.Equal(t, job.Status.ContainerStatuses[1].LastTerminationState.Terminated.StartedAt.Unix(),
			phaseInfo.Info().OccurredAt.Unix())
	})
}