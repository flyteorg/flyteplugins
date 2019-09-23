package k8s

import (
	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/utils"
	"github.com/lyft/flytestdlib/storage"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func NewArrayJobTaskTemplate() (*core.TaskTemplate, error) {
	st := &structpb.Struct{}
	err := utils.MarshalStruct(&plugins.ArrayJob{
		Size:         2,
		MinSuccesses: 2,
	}, st)
	if err != nil {
		return nil, err
	}

	return &core.TaskTemplate{
		Type:   arrayTaskType,
		Custom: st,
		Target: &core.TaskTemplate_Container{
			Container: &core.Container{
				Image:  "img",
				Config: []*core.KeyValuePair{},
				Resources: &core.Resources{
					Requests: []*core.Resources_ResourceEntry{
						{
							Name:  core.Resources_MEMORY,
							Value: "100M",
						},
					},
					Limits: []*core.Resources_ResourceEntry{},
				},
			},
		},
	}, nil
}

func newMockTaskContext(phase types.TaskPhase, state types.CustomState) types.TaskContext {
	taskCtx := &mocks.TaskContext{}
	taskCtx.On("GetNamespace").Return("fake-namespace")
	taskCtx.On("GetAnnotations").Return(map[string]string{"aKey": "aVal"})
	taskCtx.On("GetLabels").Return(map[string]string{"lKey": "lVal"})
	taskCtx.On("GetOwnerReference").Return(v1.OwnerReference{Name: "x"})
	taskCtx.On("GetOutputsFile").Return(storage.DataReference("outputs"))
	taskCtx.On("GetInputsFile").Return(storage.DataReference("inputs"))
	taskCtx.On("GetErrorFile").Return(storage.DataReference("error"))
	taskCtx.On("GetDataDir").Return(storage.DataReference("/node_id/"))
	taskCtx.On("GetOwnerID").Return(types2.NamespacedName{Name: "owner_id"})
	taskCtx.On("GetPhase").Return(phase)
	taskCtx.On("GetCustomState").Return(state)
	taskCtx.On("GetPhaseVersion").Return(uint32(1))
	taskCtx.On("GetK8sServiceAccount").Return("")

	res := &mocks2.TaskOverrides{}
	res.On("GetConfig").Return(nil)
	res.On("GetResources").Return(&corev1.ResourceRequirements{})
	taskCtx.On("GetOverrides").Return(res)

	id := &mocks2.TaskExecutionID{}
	id.On("GetGeneratedName").Return("task_test_id")
	id.On("GetID").Return(core.TaskExecutionIdentifier{
		NodeExecutionId: &core.NodeExecutionIdentifier{
			ExecutionId: &core.WorkflowExecutionIdentifier{},
		},
		TaskId: &core.Identifier{},
	})
	taskCtx.On("GetTaskExecutionID").Return(id)
	return taskCtx
}
