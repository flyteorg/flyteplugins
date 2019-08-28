package catalog

import (
	"context"
	"fmt"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/io"

	core2 "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/core"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/workqueue"
)

type writerWorkItem struct {
	id workqueue.WorkItemID

	// writerWorkItem outputs:
	workStatus workqueue.WorkStatus

	// writerWorkItem Inputs:
	// TODO: Caching task execution context looks like an anti pattern. Figure out why catalog interface requires it.
	taskCtx      core2.TaskExecutionContext
	outputReader io.OutputReader
}

func (item *writerWorkItem) GetId() workqueue.WorkItemID {
	return item.id
}

func (item *writerWorkItem) GetWorkStatus() workqueue.WorkStatus {
	return item.workStatus
}

func NewWriterWorkItem(id workqueue.WorkItemID, taskCtx core2.TaskExecutionContext, outputReader io.OutputReader) workqueue.WorkItem {

	return &writerWorkItem{
		id:           id,
		taskCtx:      taskCtx,
		outputReader: outputReader,
	}
}

type writerProcessor struct {
	catalogClient core2.CatalogClient
}

func (p writerProcessor) Process(ctx context.Context, workItem workqueue.WorkItem) (workqueue.WorkStatus, error) {
	wi, casted := workItem.(*writerWorkItem)
	if !casted {
		return workqueue.WorkStatusNotDone, fmt.Errorf("wrong work item type")
	}

	err := p.catalogClient.Put(ctx, wi.taskCtx, wi.outputReader)
	if err != nil {
		// TODO: wrap & log error
		return workqueue.WorkStatusNotDone, err
	}

	return workqueue.WorkStatusDone, nil
}

func NewWriterProcessor(catalogClient core2.CatalogClient) workqueue.Processor {
	return writerProcessor{
		catalogClient: catalogClient,
	}
}
