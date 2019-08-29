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

type ReaderWorkItem struct {
	id workqueue.WorkItemID

	// ReaderWorkItem outputs:
	cached     bool
	workStatus workqueue.WorkStatus

	// ReaderWorkItem Inputs:
	outputsWriter io.OutputWriter
	// Inputs to query data catalog
	inputReader io.InputReader
	taskReader  core2.TaskReader
}

func (item *ReaderWorkItem) GetId() workqueue.WorkItemID {
	return item.id
}

func (item *ReaderWorkItem) GetWorkStatus() workqueue.WorkStatus {
	return item.workStatus
}

func( item *ReaderWorkItem) IsCached() bool {
	return item.cached
}

func NewReaderWorkItem(id workqueue.WorkItemID, taskReader core2.TaskReader, inputReader io.InputReader,
	outputsWriter io.OutputWriter) *ReaderWorkItem {

	return &ReaderWorkItem{
		id:            id,
		inputReader:   inputReader,
		outputsWriter: outputsWriter,
		taskReader:    taskReader,
	}
}

type ReaderProcessor struct {
	catalogClient core2.CatalogClient
}

func (p ReaderProcessor) Process(ctx context.Context, workItem workqueue.WorkItem) (workqueue.WorkStatus, error) {
	wi, casted := workItem.(*ReaderWorkItem)
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

func NewReaderProcessor(catalogClient core2.CatalogClient) ReaderProcessor {
	return ReaderProcessor{
		catalogClient: catalogClient,
	}
}
