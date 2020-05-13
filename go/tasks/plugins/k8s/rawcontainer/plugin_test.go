package raw_container

import (
	"context"
	"testing"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/contextutils"
	"github.com/lyft/flytestdlib/promutils/labeled"
	"github.com/lyft/flytestdlib/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	pluginsCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	pluginsCoreMock "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	pluginsIOMock "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io/mocks"
)

var resourceRequirements = &v1.ResourceRequirements{
	Limits: v1.ResourceList{
		v1.ResourceCPU:     resource.MustParse("1024m"),
		v1.ResourceStorage: resource.MustParse("100M"),
	},
}

func dummyContainerTaskMetadata(resources *v1.ResourceRequirements) pluginsCore.TaskExecutionMetadata {
	taskMetadata := &pluginsCoreMock.TaskExecutionMetadata{}
	taskMetadata.OnGetNamespace().Return("test-namespace")
	taskMetadata.OnGetAnnotations().Return(map[string]string{"annotation-1": "val1"})
	taskMetadata.OnGetLabels().Return(map[string]string{"label-1": "val1"})
	taskMetadata.OnGetOwnerReference().Return(metav1.OwnerReference{
		Kind: "node",
		Name: "blah",
	})
	taskMetadata.OnGetK8sServiceAccount().Return("")
	taskMetadata.OnGetOwnerID().Return(types.NamespacedName{
		Namespace: "test-namespace",
		Name:      "test-owner-name",
	})
	taskMetadata.OnIsInterruptible().Return(false)

	tID := &pluginsCoreMock.TaskExecutionID{}
	tID.OnGetID().Return(core.TaskExecutionIdentifier{
		NodeExecutionId: &core.NodeExecutionIdentifier{
			ExecutionId: &core.WorkflowExecutionIdentifier{
				Name:    "my_name",
				Project: "my_project",
				Domain:  "my_domain",
			},
		},
	})
	tID.OnGetGeneratedName().Return("name")
	taskMetadata.OnGetTaskExecutionID().Return(tID)

	to := &pluginsCoreMock.TaskOverrides{}
	to.OnGetResources().Return(resources)
	taskMetadata.OnGetOverrides().Return(to)

	return taskMetadata
}

func dummyContainerTaskContext(resources *v1.ResourceRequirements, args []string, iface *core.TypedInterface, basePath string) pluginsCore.TaskExecutionContext {
	task := &core.TaskTemplate{
		Type:      "test",
		Interface: iface,
		Target: &core.TaskTemplate_Container{
			Container: &core.Container{
				Image:   "busybox",
				Command: []string{"/bin/sh", "-c"},
				Args:    args,
			},
		},
	}

	dummyTaskMetadata := dummyContainerTaskMetadata(resources)
	taskCtx := &pluginsCoreMock.TaskExecutionContext{}
	inputReader := &pluginsIOMock.InputReader{}
	inputs := basePath + "/inputs"
	inputReader.OnGetInputPrefixPath().Return(storage.DataReference(inputs))
	inputReader.OnGetInputPath().Return(storage.DataReference(inputs + "/inputs.pb"))
	inputReader.OnGetMatch(mock.Anything).Return(&core.LiteralMap{}, nil)
	taskCtx.OnInputReader().Return(inputReader)

	outputReader := &pluginsIOMock.OutputWriter{}
	outputs := basePath + "/outputs"
	outputReader.OnGetOutputPath().Return(storage.DataReference(outputs + "/outputs.pb"))
	outputReader.OnGetOutputPrefixPath().Return(storage.DataReference(outputs))
	outputReader.OnGetRawOutputPrefix().Return(storage.DataReference(outputs + "/raw-outputs/"))
	taskCtx.OnOutputWriter().Return(outputReader)

	taskReader := &pluginsCoreMock.TaskReader{}
	taskReader.OnReadMatch(mock.Anything).Return(task, nil)
	taskCtx.OnTaskReader().Return(taskReader)

	taskCtx.OnTaskExecutionMetadata().Return(dummyTaskMetadata)
	return taskCtx
}

func TestBuildResource(t *testing.T) {
	ctx := context.TODO()

	iface := &core.TypedInterface{
		Inputs: &core.VariableMap{
			Variables: map[string]*core.Variable{
				"x": {Type: &core.LiteralType{Type: &core.LiteralType_Simple{Simple: core.SimpleType_INTEGER}}},
				"y": {Type: &core.LiteralType{Type: &core.LiteralType_Simple{Simple: core.SimpleType_INTEGER}}},
			},
		},
		Outputs: &core.VariableMap{
			Variables: map[string]*core.Variable{
				"o": {Type: &core.LiteralType{Type: &core.LiteralType_Simple{Simple: core.SimpleType_INTEGER}}},
			},
		},
	}
	taskExecCtx := dummyContainerTaskContext(resourceRequirements, []string{"cd /var/flyte; mkdir outputs; paste ./inputs/x ./inputs/y | awk '{print ($1 + $2)}' > ./outputs/o"}, iface, "s3://my-s3-bucket/data/test1")
	taskExecCtx.InputReader().GetInputPath()
	p := Plugin{}
	r, err := p.BuildResource(ctx, taskExecCtx)
	assert.NoError(t, err)
	assert.NotNil(t, r)
	pod, ok := r.(*v1.Pod)
	assert.True(t, ok)
	assert.Equal(t, 2, len(pod.Spec.Containers))
	assert.Equal(t, 1, len(pod.Spec.InitContainers))
	assert.Equal(t, 3, len(pod.Spec.Volumes))
}

func init() {
	labeled.SetMetricKeys(contextutils.RoutineLabelKey)
}
