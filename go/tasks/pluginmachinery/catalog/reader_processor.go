package catalog

import (
	"context"
	"fmt"

	"github.com/lyft/flytestdlib/logger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/ioutils"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/workqueue"
)

type ReaderWorkItem struct {
	id workqueue.WorkItemID

	// ReaderWorkItem outputs:
	cached     bool
	workStatus workqueue.WorkStatus

	// ReaderWorkItem Inputs:
	outputsWriter io.OutputWriter
	// Inputs to query data catalog
	key Key
}

func (item ReaderWorkItem) GetId() workqueue.WorkItemID {
	return item.id
}

func (item ReaderWorkItem) GetWorkStatus() workqueue.WorkStatus {
	return item.workStatus
}

func (item ReaderWorkItem) IsCached() bool {
	return item.cached
}

func NewReaderWorkItem(id workqueue.WorkItemID, key Key, outputsWriter io.OutputWriter) *ReaderWorkItem {

	return &ReaderWorkItem{
		id:            id,
		key:           key,
		outputsWriter: outputsWriter,
	}
}

type ReaderProcessor struct {
	catalogClient RawClient
}

func (p ReaderProcessor) Process(ctx context.Context, workItem workqueue.WorkItem) (workqueue.WorkStatus, error) {
	wi, casted := workItem.(*ReaderWorkItem)
	if !casted {
		return workqueue.WorkStatusNotDone, fmt.Errorf("wrong work item type")
	}

	literalMap, err := p.catalogClient.Get(ctx)
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
	err = wi.outputsWriter.Put(ctx, ioutils.NewInMemoryOutputReader(literalMap, nil))
	if err != nil {
		// TODO: wrap error
		return workqueue.WorkStatusNotDone, err
	}

	wi.cached = true
	wi.workStatus = workqueue.WorkStatusDone
	return workqueue.WorkStatusDone, nil
}

func NewReaderProcessor(catalogClient RawClient) ReaderProcessor {
	return ReaderProcessor{
		catalogClient: catalogClient,
	}
}
