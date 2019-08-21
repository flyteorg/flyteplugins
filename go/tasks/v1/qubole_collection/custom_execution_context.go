package qubole_collection

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	structpb "github.com/golang/protobuf/ptypes/struct"
	idlCore "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"
	"github.com/lyft/flyteplugins/go/tasks/v1/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/v1/utils"
)

type CustomTaskExecutionContext struct {
	core.TaskExecutionContext

	// Override the TaskReader to return a TaskTemplate that returns a QuboleHiveJob that returns just one query
	// instead of an array of queries
	customTaskReader core.TaskReader

	// Override the TaskExecutionMetadata to override the TaskExecutionId to override the GetGeneratedName function
	// that's used to identify the query in the ExecutionState struct, which is also used as the resource manager
	// allocation token and also the key in the AutoRefreshCache.
	customTaskExecutionMetadata CustomTaskExecutionMetadata
}

type CustomTaskExecutionMetadata struct {
	core.TaskExecutionMetadata
	customTaskExecutionId CustomTaskExecutionId
}

func (c CustomTaskExecutionMetadata) GetTaskExecutionID() core.TaskExecutionID {
	return c.customTaskExecutionId
}

type CustomTaskExecutionId struct {
	core.TaskExecutionID
	index int
}

func (c CustomTaskExecutionId) GetGeneratedName() string {
	return fmt.Sprintf("%s_%d", c.TaskExecutionID.GetGeneratedName(), c.index)
}

func NewCustomTaskExecutionContexts(ctx context.Context, parentTCtx core.TaskExecutionContext) ([]CustomTaskExecutionContext, error) {
	originalTaskTemplate, err := parentTCtx.TaskReader().Read(ctx)
	if err != nil {
		return []CustomTaskExecutionContext{}, err
	}

	newTaskReaders, err := NewCustomTaskReaders(originalTaskTemplate)
	if err != nil {
		return []CustomTaskExecutionContext{}, err
	}

	var newContexts = make([]CustomTaskExecutionContext, len(newTaskReaders))
	for i, r := range newTaskReaders {
		customTaskExecutionId := CustomTaskExecutionId{
			TaskExecutionID: parentTCtx.TaskExecutionMetadata().GetTaskExecutionID(),
			index:           i,
		}
		customTaskExecutionMetadata := CustomTaskExecutionMetadata{
			TaskExecutionMetadata: parentTCtx.TaskExecutionMetadata(),
			customTaskExecutionId: customTaskExecutionId,
		}
		customContext := CustomTaskExecutionContext{
			TaskExecutionContext: parentTCtx,
			customTaskReader:     r,
			customTaskExecutionMetadata: customTaskExecutionMetadata,
		}
		newContexts = append(newContexts, customContext)
	}

	return newContexts, nil
}

func (c CustomTaskExecutionContext) TaskReader() core.TaskReader {
	return c.customTaskReader
}

func (c CustomTaskExecutionContext) TaskExecutionMetadata() core.TaskExecutionMetadata {
	return c.customTaskExecutionMetadata
}

type CustomTaskReader struct {
	taskTemplate *idlCore.TaskTemplate
}

func (c CustomTaskReader) Read(ctx context.Context) (*idlCore.TaskTemplate, error) {
	return c.taskTemplate, nil
}

func NewCustomTaskReaders(original *idlCore.TaskTemplate) ([]CustomTaskReader, error) {
	hiveJob := plugins.QuboleHiveJob{}
	err := utils.UnmarshalStruct(original.GetCustom(), &hiveJob)
	if err != nil {
		return []CustomTaskReader{}, err
	}
	var newReaders = make([]CustomTaskReader, len(hiveJob.QueryCollection.Queries))
	for _, x := range hiveJob.QueryCollection.Queries {
		newHiveJob := plugins.QuboleHiveJob{
			ClusterLabel: hiveJob.ClusterLabel,
			Tags:         hiveJob.Tags,
			Query:        x,
		}
		newStruct := structpb.Struct{}
		err = utils.MarshalStruct(&newHiveJob, &newStruct)
		if err != nil {
			return []CustomTaskReader{}, err
		}

		newMessage := proto.Clone(original).(*idlCore.TaskTemplate)
		newMessage.Custom = &newStruct
		newReaders = append(newReaders, CustomTaskReader{taskTemplate: newMessage})
	}
	return newReaders, nil
}
