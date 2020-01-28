package catalog

import (
	"context"
	"encoding/base32"
	"fmt"
	"hash/fnv"
	"reflect"

	"github.com/lyft/flytestdlib/promutils"

	"github.com/lyft/flytestdlib/errors"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/workqueue"
)

const specialEncoderKey = "abcdefghijklmnopqrstuvwxyz123456"

var base32Encoder = base32.NewEncoding(specialEncoderKey).WithPadding(base32.NoPadding)

// An async-client for catalog that can queue download and upload requests on workqueues.
type AsyncClientImpl struct {
	ArrayReader workqueue.IndexedWorkQueue
	ArrayWriter workqueue.IndexedWorkQueue
}

func consistentHash(str string) (string, error) {
	hasher := fnv.New32a()
	_, err := hasher.Write([]byte(str))
	if err != nil {
		return "", err
	}

	b := hasher.Sum(nil)
	return base32Encoder.EncodeToString(b), nil
}

// Returns if an entry exists for the given task and input. It returns the data as a LiteralMap
func (c AsyncClientImpl) DownloadArray(ctx context.Context, request DownloadArrayRequest) (outputFuture DownloadFuture, err error) {
	workItemID := fmt.Sprintf("%v-%v-%v-%v-%v-%v", request.Identifier.String(), request.Count,
		request.BaseTarget.GetOutputPrefixPath(), request.TypedInterface, request.BaseInputReader.GetInputPrefixPath(),
		request.CacheVersion)

	hashedID, err := consistentHash(workItemID)
	if err != nil {
		return nil, err
	}

	err = c.ArrayReader.Queue(ctx, hashedID, NewArrayReaderWorkItem(request))
	if err != nil {
		return nil, err
	}

	info, found, err := c.ArrayReader.Get(hashedID)
	if err != nil {
		return nil, errors.Wrapf(ErrSystemError, err, "Failed to lookup from reader workqueue for info: %v", workItemID)
	}

	if !found {
		return nil, errors.Errorf(ErrSystemError, "Item not found in the reader workqueue even though it was just added. ItemID: %v", workItemID)
	}

	switch info.Status() {
	case workqueue.WorkStatusSucceeded:
		readerWorkItem, casted := info.Item().(*ArrayReaderWorkItem)
		if !casted {
			return nil, errors.Errorf(ErrSystemError, "Item wasn't casted to ReaderWorkItem. ItemID: %v. Type: %v", workItemID, reflect.TypeOf(info))
		}

		return newDownloadFuture(ResponseStatusReady, nil, readerWorkItem.CachedResults(), request.Count), nil
	case workqueue.WorkStatusFailed:
		return newDownloadFuture(ResponseStatusReady, info.Error(), nil, request.Count), nil
	default:
		return newDownloadFuture(ResponseStatusNotReady, nil, nil, request.Count), nil
	}
}

// Adds a new entry to catalog for the given task execution context and the generated output
func (c AsyncClientImpl) UploadArray(ctx context.Context, request UploadArrayRequest) (putFuture UploadFuture, err error) {
	workItemID := fmt.Sprintf("%v-%v-%v-%v-%v", request.Identifier.String(), request.Count,
		request.TypedInterface, request.BaseInputReader.GetInputPrefixPath(), request.CacheVersion)

	hashedID, err := consistentHash(workItemID)
	if err != nil {
		return nil, err
	}

	err = c.ArrayWriter.Queue(ctx, hashedID, NewArrayWriterWorkItem(request))
	if err != nil {
		return nil, err
	}

	info, found, err := c.ArrayWriter.Get(hashedID)
	if err != nil {
		return nil, errors.Wrapf(ErrSystemError, err, "Failed to lookup from reader workqueue for info: %v", workItemID)
	}

	if !found {
		return nil, errors.Errorf(ErrSystemError, "Item not found in the reader workqueue even though it was just added. ItemID: %v", workItemID)
	}

	switch info.Status() {
	case workqueue.WorkStatusSucceeded:
		return newUploadFuture(ResponseStatusReady, nil), nil
	case workqueue.WorkStatusFailed:
		return newUploadFuture(ResponseStatusReady, info.Error()), nil
	default:
		return newUploadFuture(ResponseStatusNotReady, nil), nil
	}
}

func (c AsyncClientImpl) Download(ctx context.Context, request DownloadRequest) (outputFuture DownloadFuture, err error) {
	return c.DownloadArray(ctx, DownloadArrayRequest{
		Identifier:      request.Key.Identifier,
		CacheVersion:    request.Key.CacheVersion,
		TypedInterface:  request.Key.TypedInterface,
		BaseInputReader: request.Key.InputReader,
		BaseTarget:      request.Target,
		dataStore:       request.DataStore,
		Indexes:         nil,
		Count:           0,
	})
}

func (c AsyncClientImpl) Upload(ctx context.Context, requests UploadRequest) (putFuture UploadFuture, err error) {
	return c.UploadArray(ctx, UploadArrayRequest{
		Identifier:       requests.Key.Identifier,
		CacheVersion:     requests.Key.CacheVersion,
		TypedInterface:   requests.Key.TypedInterface,
		ArtifactMetadata: requests.ArtifactMetadata,
		dataStore:        requests.DataStore,
		BaseInputReader:  requests.Key.InputReader,
		BaseArtifactData: requests.ArtifactData,
		Indexes:          nil,
		Count:            0,
	})
}

func (c AsyncClientImpl) Start(ctx context.Context) error {
	if err := c.ArrayReader.Start(ctx); err != nil {
		return errors.Wrapf(ErrSystemError, err, "Failed to start reader queue.")
	}

	if err := c.ArrayWriter.Start(ctx); err != nil {
		return errors.Wrapf(ErrSystemError, err, "Failed to start writer queue.")
	}

	return nil
}

func NewAsyncClient(client Client, cfg Config, scope promutils.Scope) (AsyncClientImpl, error) {
	arrayReaderWorkQueue, err := workqueue.NewIndexedWorkQueue("reader", NewArrayReaderProcessor(client, cfg.Reader.MaxItemsPerRound), cfg.Reader.Workqueue,
		scope.NewSubScope("reader"))
	if err != nil {
		return AsyncClientImpl{}, err
	}

	arrayWriterWorkQueue, err := workqueue.NewIndexedWorkQueue("writer", NewWriterArrayProcessor(client, cfg.Writer.MaxItemsPerRound), cfg.Writer.Workqueue,
		scope.NewSubScope("writer"))
	if err != nil {
		return AsyncClientImpl{}, err
	}

	return AsyncClientImpl{
		ArrayWriter: arrayWriterWorkQueue,
		ArrayReader: arrayReaderWorkQueue,
	}, nil
}
