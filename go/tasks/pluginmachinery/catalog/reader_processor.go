package catalog

import (
	"context"
	"fmt"

	"github.com/lyft/flytestdlib/logger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/workqueue"
)

type ReaderWorkItem struct {
	// ReaderWorkItem outputs:
	cached bool

	// ReaderWorkItem Inputs:
	outputsWriter io.OutputWriter
	// Inputs to query data catalog
	key Key
}

func (item ReaderWorkItem) IsCached() bool {
	return item.cached
}

func NewReaderWorkItem(key Key, outputsWriter io.OutputWriter) *ReaderWorkItem {
	return &ReaderWorkItem{
		key:           key,
		outputsWriter: outputsWriter,
	}
}

type ReaderProcessor struct {
	catalogClient Client
}

func (p ReaderProcessor) Process(ctx context.Context, workItem workqueue.WorkItem) (workqueue.WorkStatus, error) {
	wi, casted := workItem.(*ReaderWorkItem)
	if !casted {
		return workqueue.WorkStatusNotDone, fmt.Errorf("wrong work item type")
	}

	op, err := p.catalogClient.Get(ctx, wi.key)
	if err != nil {
		if taskStatus, ok := status.FromError(err); ok && taskStatus.Code() == codes.NotFound {
			logger.Infof(ctx, "Artifact not found in Catalog.")

			wi.cached = false
			return workqueue.WorkStatusSucceeded, nil
		}

		// TODO: wrap & log error
		return workqueue.WorkStatusNotDone, err
	}

	if op == nil {
		wi.cached = false
		return workqueue.WorkStatusSucceeded, nil
	}

	// TODO: Check task interface, if it has outputs but literalmap is empty (or not matching output), error.
	err = wi.outputsWriter.Put(ctx, op)
	if err != nil {
		// TODO: wrap error
		return workqueue.WorkStatusNotDone, err
	}

	wi.cached = true
	return workqueue.WorkStatusSucceeded, nil
}

func NewReaderProcessor(catalogClient Client) ReaderProcessor {
	return ReaderProcessor{
		catalogClient: catalogClient,
	}
}
