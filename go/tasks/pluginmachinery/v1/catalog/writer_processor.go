package catalog

import (
	"context"
	"fmt"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/io"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/workqueue"
	"github.com/lyft/flyteplugins/go/tasks/v1/errors"
	"github.com/lyft/flytestdlib/logger"
)

type writerWorkItem struct {
	id workqueue.WorkItemID

	// writerWorkItem outputs
	workStatus workqueue.WorkStatus

	// writerWorkItem Inputs
	key      core.CatalogKey
	data     io.OutputReader
	metadata core.CatalogMetadata
}

func (item *writerWorkItem) GetId() workqueue.WorkItemID {
	return item.id
}

func (item *writerWorkItem) GetWorkStatus() workqueue.WorkStatus {
	return item.workStatus
}

func NewWriterWorkItem(id workqueue.WorkItemID, key core.CatalogKey, data io.OutputReader, metadata core.CatalogMetadata) workqueue.WorkItem {
	return &writerWorkItem{
		id:       id,
		key:      key,
		data:     data,
		metadata: metadata,
	}
}

type writerProcessor struct {
	catalogClient core.CatalogClient
}

func (p writerProcessor) Process(ctx context.Context, workItem workqueue.WorkItem) (workqueue.WorkStatus, error) {
	wi, casted := workItem.(*writerWorkItem)
	if !casted {
		return workqueue.WorkStatusNotDone, fmt.Errorf("wrong work item type")
	}

	err := p.catalogClient.Put(ctx, wi.key, wi.data, wi.metadata)
	if err != nil {
		logger.Errorf(ctx, "Error putting to catalog [%s]", err)
		return workqueue.WorkStatusNotDone, errors.Wrapf(errors.DownstreamSystemError, err,
			"Error writing [%s] to catalog, key id [%s] cache version [%s]",
			wi.id, wi.key.Identifier, wi.key.CacheVersion)
	}

	return workqueue.WorkStatusDone, nil
}

func NewWriterProcessor(catalogClient core.CatalogClient) workqueue.Processor {
	return writerProcessor{
		catalogClient: catalogClient,
	}
}
