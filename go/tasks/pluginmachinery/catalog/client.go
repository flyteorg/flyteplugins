package catalog

import (
	"context"
	"fmt"
	"reflect"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/workqueue"
	"github.com/lyft/flytestdlib/bitarray"
	"github.com/lyft/flytestdlib/errors"
)

type ClientImpl struct {
	Reader workqueue.IndexedWorkQueue
	Writer workqueue.IndexedWorkQueue
}

func (c ClientImpl) Download(ctx context.Context, requests ...DownloadRequest) (outputFuture DownloadFuture, err error) {
	status := ResponseStatusReady
	cachedResults := bitarray.NewBitSet(uint(len(requests)))
	cachedCount := 0
	for idx, request := range requests {
		workItemID := fmt.Sprintf("%v-%v", request.Key, idx)
		err := c.Reader.Queue(NewReaderWorkItem(
			workItemID,
			request.Key,
			request.Target))

		if err != nil {
			return nil, err
		}

		item, found, err := c.Reader.Get(workItemID)
		if err != nil {
			return nil, errors.Wrapf(ErrSystemError, err, "Failed to lookup from reader workqueue for item: %v", workItemID)
		}

		if !found {
			return nil, errors.Errorf(ErrSystemError, "Item not found in the reader workqueue even though it was just added. ItemID: %v", workItemID)
		}

		switch item.GetWorkStatus() {
		case workqueue.WorkStatusDone:
			readerWorkItem, casted := item.(*ReaderWorkItem)
			if !casted {
				return nil, errors.Errorf(ErrSystemError, "Item wasn't casted to ReaderWorkItem. ItemID: %v. Type: %v", workItemID, reflect.TypeOf(item))
			}

			if readerWorkItem.IsCached() {
				cachedResults.Set(uint(idx))
				cachedCount++
			}
		case workqueue.WorkStatusNotDone:
			status = ResponseStatusNotReady
		}
	}

	return newDownloadFuture(status, cachedResults, len(requests), cachedCount), nil
}

func (c ClientImpl) Upload(ctx context.Context, requests ...UploadRequest) (putFuture UploadFuture, err error) {
	status := ResponseStatusReady
	for idx, request := range requests {
		workItemID := fmt.Sprintf("%v-%v", request.Key, idx)
		err := c.Writer.Queue(NewWriterWorkItem(
			workItemID,
			request.Key,
			request.ArtifactData,
			request.ArtifactMetadata))

		if err != nil {
			return nil, err
		}

		item, found, err := c.Writer.Get(workItemID)
		if err != nil {
			return nil, errors.Wrapf(ErrSystemError, err, "Failed to lookup from writer workqueue for item: %v", workItemID)
		}

		if !found {
			return nil, errors.Errorf(ErrSystemError, "Item not found in the writer workqueue even though it was just added. ItemID: %v", workItemID)
		}

		switch item.GetWorkStatus() {
		case workqueue.WorkStatusNotDone:
			status = ResponseStatusNotReady
		}
	}

	return newUploadFuture(status), nil
}

func NewClient(rawClient RawClient, cfg Config) (ClientImpl, error) {
	readerWorkQueue, err := workqueue.NewIndexedWorkQueue(NewReaderProcessor(rawClient), cfg.ReaderWorkqueueConfig)
	if err != nil {
		return ClientImpl{}, err
	}

	writerWorkQueue, err := workqueue.NewIndexedWorkQueue(NewReaderProcessor(rawClient), cfg.WriterWorkqueueConfig)
	return ClientImpl{
		Reader: readerWorkQueue,
		Writer: writerWorkQueue,
	}, nil
}
