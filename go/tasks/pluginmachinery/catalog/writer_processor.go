package catalog

import (
	"context"
	"fmt"

	"github.com/lyft/flyteplugins/go/tasks/errors"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/workqueue"
	"github.com/lyft/flytestdlib/logger"
)

type WriterWorkItem struct {
	id workqueue.WorkItemID

	// WriterWorkItem outputs
	workStatus workqueue.WorkStatus

	// WriterWorkItem Inputs
	key      Key
	data     io.OutputReader
	metadata Metadata
}

func (item WriterWorkItem) GetId() workqueue.WorkItemID {
	return item.id
}

func (item WriterWorkItem) GetWorkStatus() workqueue.WorkStatus {
	return item.workStatus
}

func NewWriterWorkItem(id workqueue.WorkItemID, key Key, data io.OutputReader, metadata Metadata) *WriterWorkItem {
	return &WriterWorkItem{
		id:       id,
		key:      key,
		data:     data,
		metadata: metadata,
	}
}

type writerProcessor struct {
	catalogClient RawClient
}

func (p writerProcessor) Process(ctx context.Context, workItem workqueue.WorkItem) (workqueue.WorkStatus, error) {
	wi, casted := workItem.(*WriterWorkItem)
	if !casted {
		return workqueue.WorkStatusNotDone, fmt.Errorf("wrong work item type")
	}

	err := p.catalogClient.Put(ctx, wi.key, wi.data, wi.metadata)
	if err != nil {
		logger.Errorf(ctx, "Error putting to catalog [%s]", err)
		return workqueue.WorkStatusNotDone, errors.Wrapf(errors.DownstreamSystemError, err,
			"Error writing [%s] to catalog, key id [%v] cache version [%v]",
			wi.id, wi.key.Identifier, wi.key.CacheVersion)
	}

	return workqueue.WorkStatusDone, nil
}

func NewWriterProcessor(catalogClient RawClient) workqueue.Processor {
	return writerProcessor{
		catalogClient: catalogClient,
	}
}
