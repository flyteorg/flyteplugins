package sagemaker

import (
	"context"
	"fmt"

	"testing"

	ptOp "github.com/kubeflow/pytorch-operator/pkg/apis/pytorch/v1"
	commonOp "github.com/kubeflow/tf-operator/pkg/apis/common/v1"
	"github.com/lyft/flyteplugins/go/tasks/logs"
	pluginsCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/stretchr/testify/assert"
)

func TestBuildResourcePytorch(t *testing.T) {
	awsSageMakerPluginHandler := awsSagemakerPlugin{}

	ptObj := dummyPytorchCustomObj(100)
	taskTemplate := dummySparkTaskTemplate("the job", ptObj)

	resource, err := pytorchResourceHandler.BuildResource(context.TODO(), dummyPytorchTaskContext(taskTemplate))
	assert.NoError(t, err)
	assert.NotNil(t, resource)

	pytorchJob, ok := resource.(*ptOp.PyTorchJob)
	assert.True(t, ok)
	assert.Equal(t, int32(100), *pytorchJob.Spec.PyTorchReplicaSpecs[ptOp.PyTorchReplicaTypeWorker].Replicas)

	for _, replicaSpec := range pytorchJob.Spec.PyTorchReplicaSpecs {
		var hasContainerWithDefaultPytorchName = false

		for _, container := range replicaSpec.Template.Spec.Containers {
			if container.Name == ptOp.DefaultContainerName {
				hasContainerWithDefaultPytorchName = true
			}

			assert.Equal(t, resourceRequirements.Requests, container.Resources.Requests)
			assert.Equal(t, resourceRequirements.Limits, container.Resources.Limits)
		}

		assert.True(t, hasContainerWithDefaultPytorchName)
	}
}

func TestGetTaskPhase(t *testing.T) {
	pytorchResourceHandler := pytorchOperatorResourceHandler{}
	ctx := context.TODO()

	dummyPytorchJobResourceCreator := func(conditionType commonOp.JobConditionType) *ptOp.PyTorchJob {
		return dummyPytorchJobResource(pytorchResourceHandler, 2, conditionType)
	}

	taskPhase, err := pytorchResourceHandler.GetTaskPhase(ctx, nil, dummyPytorchJobResourceCreator(commonOp.JobCreated))
	assert.NoError(t, err)
	assert.Equal(t, pluginsCore.PhaseQueued, taskPhase.Phase())
	assert.NotNil(t, taskPhase.Info())
	assert.Nil(t, err)

	taskPhase, err = pytorchResourceHandler.GetTaskPhase(ctx, nil, dummyPytorchJobResourceCreator(commonOp.JobRunning))
	assert.NoError(t, err)
	assert.Equal(t, pluginsCore.PhaseRunning, taskPhase.Phase())
	assert.NotNil(t, taskPhase.Info())
	assert.Nil(t, err)

	taskPhase, err = pytorchResourceHandler.GetTaskPhase(ctx, nil, dummyPytorchJobResourceCreator(commonOp.JobSucceeded))
	assert.NoError(t, err)
	assert.Equal(t, pluginsCore.PhaseSuccess, taskPhase.Phase())
	assert.NotNil(t, taskPhase.Info())
	assert.Nil(t, err)

	taskPhase, err = pytorchResourceHandler.GetTaskPhase(ctx, nil, dummyPytorchJobResourceCreator(commonOp.JobFailed))
	assert.NoError(t, err)
	assert.Equal(t, pluginsCore.PhaseRetryableFailure, taskPhase.Phase())
	assert.NotNil(t, taskPhase.Info())
	assert.Nil(t, err)

	taskPhase, err = pytorchResourceHandler.GetTaskPhase(ctx, nil, dummyPytorchJobResourceCreator(commonOp.JobRestarting))
	assert.NoError(t, err)
	assert.Equal(t, pluginsCore.PhaseRetryableFailure, taskPhase.Phase())
	assert.NotNil(t, taskPhase.Info())
	assert.Nil(t, err)
}

func TestGetLogs(t *testing.T) {
	assert.NoError(t, logs.SetLogConfig(&logs.LogConfig{
		IsKubernetesEnabled: true,
		KubernetesURL:       "k8s.com",
	}))

	workers := int32(2)

	awsSagemakerPluginHandler := awsSagemakerPlugin{}
	jobLogs, err := getLogs(dummyPytorchJobResource(awsSagemakerPluginHandler, workers, commonOp.JobRunning), workers)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(jobLogs))
	assert.Equal(t, fmt.Sprintf("k8s.com/#!/log/%s/%s-master-0/pod?namespace=pytorch-namespace", jobNamespace, jobName), jobLogs[0].Uri)
	assert.Equal(t, fmt.Sprintf("k8s.com/#!/log/%s/%s-worker-0/pod?namespace=pytorch-namespace", jobNamespace, jobName), jobLogs[1].Uri)
	assert.Equal(t, fmt.Sprintf("k8s.com/#!/log/%s/%s-worker-1/pod?namespace=pytorch-namespace", jobNamespace, jobName), jobLogs[2].Uri)
}
