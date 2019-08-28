package catalog

import (
	"context"
	"fmt"

	"github.com/lyft/flytestdlib/logger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/ioutils"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/io"

	core2 "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/core"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/workqueue"
)

type readerWorkItem struct {
	id workqueue.WorkItemID

	// readerWorkItem outputs:
	cached     bool
	workStatus workqueue.WorkStatus

	// readerWorkItem Inputs:
	outputsWriter io.OutputWriter
	// Inputs to query data catalog
	inputReader io.InputReader
	taskReader  core2.TaskReader
}

func (item *readerWorkItem) GetId() workqueue.WorkItemID {
	return item.id
}

func (item *readerWorkItem) GetWorkStatus() workqueue.WorkStatus {
	return item.workStatus
}

func NewReaderWorkItem(id workqueue.WorkItemID, taskReader core2.TaskReader, inputReader io.InputReader,
	outputsWriter io.OutputWriter) workqueue.WorkItem {

	return &readerWorkItem{
		id:            id,
		inputReader:   inputReader,
		outputsWriter: outputsWriter,
		taskReader:    taskReader,
	}
}

type readerProcessor struct {
	catalogClient core2.CatalogClient
}

func (p readerProcessor) Process(ctx context.Context, workItem workqueue.WorkItem) (workqueue.WorkStatus, error) {
	wi, casted := workItem.(*readerWorkItem)
	if !casted {
		return workqueue.WorkStatusNotDone, fmt.Errorf("wrong work item type")
	}

	literalMap, err := p.catalogClient.Get(ctx, wi.taskReader, wi.inputReader)
	if err != nil {
		if taskStatus, ok := status.FromError(err); ok && taskStatus.Code() == codes.NotFound {
			logger.Infof(ctx, "Artifact not found in Catalog.")

			wi.cached = false
			wi.workStatus = workqueue.WorkStatusDone
			return workqueue.WorkStatusDone, nil
		}

		// TODO: wrap & log error
		return workqueue.WorkStatusNotDone, err
	}

	// TODO: Check task interface, if it has outputs but literalmap is empty (or not matching output), error.
	err = wi.outputsWriter.Put(ctx, ioutils.NewSimpleOutputReader(literalMap, nil))
	if err != nil {
		// TODO: wrap error
		return workqueue.WorkStatusNotDone, err
	}

	wi.cached = true
	wi.workStatus = workqueue.WorkStatusDone
	return workqueue.WorkStatusDone, nil
}

func NewReaderProcessor(catalogClient core2.CatalogClient) workqueue.Processor {
	return readerProcessor{
		catalogClient: catalogClient,
	}
}
