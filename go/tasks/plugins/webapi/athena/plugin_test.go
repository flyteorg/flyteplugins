package athena

import (
	"testing"

	coreMocks "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	"k8s.io/apimachinery/pkg/util/rand"

	awsSdk "github.com/aws/aws-sdk-go-v2/aws"
	idlCore "github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/stretchr/testify/assert"
)

func TestCreateTaskInfo(t *testing.T) {
	taskInfo := createTaskInfo("query_id", awsSdk.Config{
		Region: "us-east-1",
	})
	assert.EqualValues(t, []*idlCore.TaskLog{
		{
			Uri:  "https://us-east-1.console.aws.amazon.com/athena/home?force&region=us-east-1#query/history/query_id",
			Name: "Athena Query Console",
		},
	}, taskInfo.Logs)
	assert.Len(t, taskInfo.ExternalResources, 1)
	assert.Equal(t, taskInfo.ExternalResources[0].ExternalID, "query_id")
}

func getMockID(l int) *coreMocks.TaskExecutionID {
	execID := rand.String(l)
	tID := &coreMocks.TaskExecutionID{}
	tID.OnGetGeneratedName().Return(execID + "-t1")
	tID.OnGetID().Return(idlCore.TaskExecutionIdentifier{
		TaskId: &idlCore.Identifier{
			ResourceType: idlCore.ResourceType_TASK,
			Project:      "a",
			Domain:       "d",
			Name:         "n",
			Version:      "abc",
		},
		NodeExecutionId: &idlCore.NodeExecutionIdentifier{
			NodeId: "node1",
			ExecutionId: &idlCore.WorkflowExecutionIdentifier{
				Project: "a",
				Domain:  "d",
				Name:    "exec",
			},
		},
		RetryAttempt: 0,
	})

	return tID
}

func TestTokenGeneration(t *testing.T) {
	id1 := getMockID(3)
	id2 := getMockID(30)
	p := Plugin{}
	token := p.getClientRequestToken(id1)
	assert.True(t, len(token) == 32)
	token = p.getClientRequestToken(id2)
	assert.True(t, len(token) >= 32)
}
