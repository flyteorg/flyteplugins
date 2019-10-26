package catalog

import (
	"context"
	"fmt"
	"reflect"

	"github.com/lyft/flytestdlib/promutils"

	"github.com/lyft/flytestdlib/bitarray"

	"github.com/lyft/flytestdlib/errors"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/workqueue"
)

// An async-client for catalog that can queue download and upload requests on workqueues.
type AsyncClientImpl struct {
	Reader workqueue.IndexedWorkQueue
	Writer workqueue.IndexedWorkQueue
}

func formatWorkItemID(key Key, idx int) string {
	return fmt.Sprintf("%v-%v", key, idx)
}

func (c AsyncClientImpl) Download(ctx context.Context, requests ...DownloadRequest) (outputFuture DownloadFuture, err error) {
	status := ResponseStatusReady
	cachedResults := bitarray.NewBitSet(uint(len(requests)))
	cachedCount := 0
	var respErr error
	for idx, request := range requests {
		workItemID := formatWorkItemID(request.Key, idx)
		err := c.Reader.Queue(ctx, workItemID, NewReaderWorkItem(
			request.Key,
			request.Target))

		if err != nil {
			return nil, err
		}

		info, found, err := c.Reader.Get(workItemID)
		if err != nil {
			return nil, errors.Wrapf(ErrSystemError, err, "Failed to lookup from reader workqueue for info: %v", workItemID)
		}

		if !found {
			return nil, errors.Errorf(ErrSystemError, "Item not found in the reader workqueue even though it was just added. ItemID: %v", workItemID)
		}

		switch info.Status() {
		case workqueue.WorkStatusSucceeded:
			readerWorkItem, casted := info.Item().(*ReaderWorkItem)
			if !casted {
				return nil, errors.Errorf(ErrSystemError, "Item wasn't casted to ReaderWorkItem. ItemID: %v. Type: %v", workItemID, reflect.TypeOf(info))
			}

			if readerWorkItem.IsCached() {
				cachedResults.Set(uint(idx))
				cachedCount++
			}
		case workqueue.WorkStatusFailed:
			respErr = info.Error()
		case workqueue.WorkStatusNotDone:
			status = ResponseStatusNotReady
		}
	}

	return newDownloadFuture(status, respErr, cachedResults, len(requests), cachedCount), nil
}

func (c AsyncClientImpl) Upload(ctx context.Context, requests ...UploadRequest) (putFuture UploadFuture, err error) {
	status := ResponseStatusReady
	var respErr error
	for idx, request := range requests {
		workItemID := formatWorkItemID(request.Key, idx)
		err := c.Writer.Queue(ctx, workItemID, NewWriterWorkItem(
			request.Key,
			request.ArtifactData,
			request.ArtifactMetadata))

		if err != nil {
			return nil, err
		}

		info, found, err := c.Writer.Get(workItemID)
		if err != nil {
			return nil, errors.Wrapf(ErrSystemError, err, "Failed to lookup from writer workqueue for info: %v", workItemID)
		}

		if !found {
			return nil, errors.Errorf(ErrSystemError, "Item not found in the writer workqueue even though it was just added. ItemID: %v", workItemID)
		}

		switch info.Status() {
		case workqueue.WorkStatusNotDone:
			status = ResponseStatusNotReady
		case workqueue.WorkStatusFailed:
			respErr = info.Error()
		}
	}

	return newUploadFuture(status, respErr), nil
}

func (c AsyncClientImpl) Start(ctx context.Context) error {
	if err := c.Reader.Start(ctx); err != nil {
		return errors.Wrapf(ErrSystemError, err, "Failed to start reader queue.")
	}

	if err := c.Writer.Start(ctx); err != nil {
		return errors.Wrapf(ErrSystemError, err, "Failed to start writer queue.")
	}

	return nil
}

func NewAsyncClient(client Client, cfg Config, scope promutils.Scope) (AsyncClientImpl, error) {
	readerWorkQueue, err := workqueue.NewIndexedWorkQueue("reader", NewReaderProcessor(client), cfg.ReaderWorkqueueConfig,
		scope.NewSubScope("reader"))
	if err != nil {
		return AsyncClientImpl{}, err
	}

	writerWorkQueue, err := workqueue.NewIndexedWorkQueue("writer", NewWriterProcessor(client), cfg.WriterWorkqueueConfig,
		scope.NewSubScope("writer"))
	if err != nil {
		return AsyncClientImpl{}, err
	}

	return AsyncClientImpl{
		Reader: readerWorkQueue,
		Writer: writerWorkQueue,
	}, nil
}
