package test

import (
	"github.com/stretchr/testify/mock"

	"k8s.io/apimachinery/pkg/types"


	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/storage"
	v1 "k8s.io/api/core/v1"

	pluginsCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	pluginsCoreMock "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	pluginsIOMock "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io/mocks"
)

func DummyContainerTaskMetadata(resources *v1.ResourceRequirements) pluginsCore.TaskExecutionMetadata {
	taskMetadata := &pluginsCoreMock.TaskExecutionMetadata{}
	taskMetadata.On("GetNamespace").Return("test-namespace")
	taskMetadata.On("GetAnnotations").Return(map[string]string{"annotation-1": "val1"})
	taskMetadata.On("GetLabels").Return(map[string]string{"label-1": "val1"})
	taskMetadata.On("GetOwnerReference").Return(metav1.OwnerReference{
		Kind: "node",
		Name: "blah",
	})
	taskMetadata.On("GetK8sServiceAccount").Return("service-account")
	taskMetadata.On("GetOwnerID").Return(types.NamespacedName{
		Namespace: "test-namespace",
		Name:      "test-owner-name",
	})

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
	tID.On("GetGeneratedName").Return("my_project:my_domain:my_name")
	taskMetadata.On("GetTaskExecutionID").Return(tID)

	to := &pluginsCoreMock.TaskOverrides{}
	to.On("GetResources").Return(resources)
	taskMetadata.On("GetOverrides").Return(to)

	return taskMetadata
}

func DummyContainerTaskContext(resources *v1.ResourceRequirements, command []string, args []string) pluginsCore.TaskExecutionContext {
	task := &core.TaskTemplate{
		Type: "test",
		Target: &core.TaskTemplate_Container{
			Container: &core.Container{
				Command: command,
				Args:    args,
			},
		},
	}

	dummyTaskMetadata := DummyContainerTaskMetadata(resources)
	taskCtx := &pluginsCoreMock.TaskExecutionContext{}
	inputReader := &pluginsIOMock.InputReader{}
	inputReader.On("GetInputPath").Return(storage.DataReference("test-data-reference"))
	inputReader.On("Get", mock.Anything).Return(&core.LiteralMap{}, nil)
	taskCtx.On("InputReader").Return(inputReader)

	outputReader := &pluginsIOMock.OutputWriter{}
	outputReader.On("GetOutputPath").Return(storage.DataReference("/data/outputs.pb"))
	outputReader.On("GetOutputPrefixPath").Return(storage.DataReference("/data/"))
	taskCtx.On("OutputWriter").Return(outputReader)

	taskReader := &pluginsCoreMock.TaskReader{}
	taskReader.On("Read", mock.Anything).Return(task, nil)
	taskCtx.On("TaskReader").Return(taskReader)

	taskCtx.On("TaskExecutionMetadata").Return(dummyTaskMetadata)
	return taskCtx
}

