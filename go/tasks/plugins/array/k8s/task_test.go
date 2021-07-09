package k8s

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core/mocks"

	"github.com/stretchr/testify/assert"
)

func TestFinalize(t *testing.T) {
	ctx := context.Background()

	tCtx := getMockTaskExecutionContext(ctx)
	kubeClient := mocks.KubeClient{}
	kubeClient.OnGetClient().Return(mocks.NewFakeKubeClient())

	resourceManager := mocks.ResourceManager{}
	podTemplate, _, _ := FlyteArrayJobToK8sPodTemplate(ctx, tCtx, "")
	pod := addPodFinalizer(&podTemplate)
	pod.Name = formatSubTaskName(ctx, tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), "1")
	assert.NoError(t, kubeClient.GetClient().Create(ctx, pod))

	resourceManager.OnReleaseResourceMatch(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	tCtx.OnResourceManager().Return(&resourceManager)

	config := Config{
		MaxArrayJobSize: 100,
		ResourceConfig: ResourceConfig{
			PrimaryLabel: "p",
			Limit:        10,
		},
	}

	task := &Task{
		Config:   &config,
		ChildIdx: 1,
	}

	err := task.Finalize(ctx, tCtx, &kubeClient)
	assert.NoError(t, err)
}
