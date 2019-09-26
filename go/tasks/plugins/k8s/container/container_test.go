package container

import (
	"context"
	"github.com/lyft/flyteplugins/go/tasks/plugins/k8s/test"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	pluginsCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	pluginsCoreMock "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/flytek8s"
)

var resourceRequirements = &v1.ResourceRequirements{
	Limits: v1.ResourceList{
		v1.ResourceCPU:     resource.MustParse("1024m"),
		v1.ResourceStorage: resource.MustParse("100M"),
	},
}
func TestContainerTaskExecutor_BuildIdentityResource(t *testing.T) {
	c := containerTaskExecutor{}
	taskMetadata := &pluginsCoreMock.TaskExecutionMetadata{}
	r, err := c.BuildIdentityResource(context.TODO(), taskMetadata)
	assert.NoError(t, err)
	assert.NotNil(t, r)
	_, ok := r.(*v1.Pod)
	assert.True(t, ok)
	assert.Equal(t, flytek8s.PodKind, r.GetObjectKind().GroupVersionKind().Kind)
}

func TestContainerTaskExecutor_BuildResource(t *testing.T) {
	c := containerTaskExecutor{}
	command := []string{"command"}
	args := []string{"{{.Input}}"}
	taskCtx := test.DummyContainerTaskContext(resourceRequirements, command, args)

	r, err := c.BuildResource(context.TODO(), taskCtx)
	assert.NoError(t, err)
	assert.NotNil(t, r)
	j, ok := r.(*v1.Pod)
	assert.True(t, ok)

	assert.NotEmpty(t, j.Spec.Containers)
	assert.Equal(t, resourceRequirements.Limits[v1.ResourceCPU], j.Spec.Containers[0].Resources.Limits[v1.ResourceCPU])

	// TODO: Once configurable, test when setting storage is supported on the cluster vs not.
	storageRes := j.Spec.Containers[0].Resources.Limits[v1.ResourceStorage]
	assert.Equal(t, int64(0), (&storageRes).Value())

	assert.Equal(t, command, j.Spec.Containers[0].Command)
	assert.Equal(t, []string{"command"}, j.Spec.Containers[0].Args)

	assert.Equal(t, "service-account", j.Spec.ServiceAccountName)
}

func TestContainerTaskExecutor_GetTaskStatus(t *testing.T) {
	c := containerTaskExecutor{}
	j := &v1.Pod{
		Status: v1.PodStatus{},
	}

	ctx := context.TODO()
	t.Run("running", func(t *testing.T) {
		j.Status.Phase = v1.PodRunning
		phaseInfo, err := c.GetTaskPhase(ctx, nil, j)
		assert.NoError(t, err)
		assert.Equal(t, pluginsCore.PhaseRunning, phaseInfo.Phase())
	})

	t.Run("queued", func(t *testing.T) {
		j.Status.Phase = v1.PodPending
		phaseInfo, err := c.GetTaskPhase(ctx, nil, j)
		assert.NoError(t, err)
		assert.Equal(t, pluginsCore.PhaseQueued, phaseInfo.Phase())
	})

	t.Run("failNoCondition", func(t *testing.T) {
		j.Status.Phase = v1.PodFailed
		phaseInfo, err := c.GetTaskPhase(ctx, nil, j)
		assert.NoError(t, err)
		assert.Equal(t, pluginsCore.PhaseRetryableFailure, phaseInfo.Phase())
		ec := phaseInfo.Err().GetCode()
		assert.Equal(t, "UnknownError", ec)
	})

	t.Run("failConditionUnschedulable", func(t *testing.T) {
		j.Status.Phase = v1.PodFailed
		j.Status.Reason = "Unschedulable"
		j.Status.Message = "some message"
		j.Status.Conditions = []v1.PodCondition{
			{
				Type: v1.PodReasonUnschedulable,
			},
		}
		phaseInfo, err := c.GetTaskPhase(ctx, nil, j)
		assert.NoError(t, err)
		assert.Equal(t, pluginsCore.PhaseRetryableFailure, phaseInfo.Phase())
		ec := phaseInfo.Err().GetCode()
		assert.Equal(t, "Unschedulable", ec)
	})

	t.Run("success", func(t *testing.T) {
		j.Status.Phase = v1.PodSucceeded
		phaseInfo, err := c.GetTaskPhase(ctx, nil, j)
		assert.NoError(t, err)
		assert.NotNil(t, phaseInfo)
		assert.Equal(t, pluginsCore.PhaseSuccess, phaseInfo.Phase())
	})
}
