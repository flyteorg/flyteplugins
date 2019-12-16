package catalog

import (
	"context"
	"fmt"
	"reflect"
	"strconv"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/ioutils"
	"github.com/lyft/flytestdlib/bitarray"

	"github.com/lyft/flyteplugins/go/tasks/errors"

	"github.com/lyft/flytestdlib/logger"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/workqueue"
)

type ArrayReaderWorkItem struct {
	// ArrayReaderWorkItem outputs:
	cached   *bitarray.BitSet
	progress *bitarray.BitSet

	// ArrayReaderWorkItem Inputs:
	downloadRequest DownloadArrayRequest
}

func (item ArrayReaderWorkItem) CachedResults() *bitarray.BitSet {
	return item.cached
}

func NewArrayReaderWorkItem(request DownloadArrayRequest) *ArrayReaderWorkItem {
	return &ArrayReaderWorkItem{
		downloadRequest: request,
	}
}

type ArrayReaderProcessor struct {
	catalogClient    Client
	maxItemsPerRound int
}

func (p ArrayReaderProcessor) Process(ctx context.Context, workItem workqueue.WorkItem) (workqueue.WorkStatus, error) {
	wi, casted := workItem.(*ArrayReaderWorkItem)
	if !casted {
		return workqueue.WorkStatusNotDone, fmt.Errorf("wrong work item type. Received: %v", reflect.TypeOf(workItem))
	}

	if wi.cached == nil {
		wi.cached = bitarray.NewBitSet(uint(wi.downloadRequest.Count))
		wi.progress = bitarray.NewBitSet(uint(wi.downloadRequest.Count))
	}

	isArray := wi.downloadRequest.Count > 0

	for i := uint(0); i < uint(wi.downloadRequest.Count); i++ {
		inputReader := wi.downloadRequest.BaseInputReader
		if isArray {
			if !wi.downloadRequest.Indexes.IsSet(i) {
				continue
			}

			if wi.progress.IsSet(i) {
				logger.Debugf(ctx, "Catalog lookup already ran for index [%v], result [%v].", i, wi.cached.IsSet(i))
				continue
			}

			indexedInputLocation, err := wi.downloadRequest.dataStore.ConstructReference(ctx,
				wi.downloadRequest.BaseInputReader.GetInputPrefixPath(),
				strconv.Itoa(int(i)))
			if err != nil {
				return workqueue.WorkStatusNotDone, err
			}

			inputReader = ioutils.NewRemoteFileInputReader(ctx, wi.downloadRequest.dataStore,
				ioutils.NewInputFilePaths(ctx, wi.downloadRequest.dataStore, indexedInputLocation))
		}

		k := Key{
			Identifier:     wi.downloadRequest.Identifier,
			CacheVersion:   wi.downloadRequest.CacheVersion,
			TypedInterface: wi.downloadRequest.TypedInterface,
			InputReader:    inputReader,
		}
		op, err := p.catalogClient.Get(ctx, k)
		if err != nil {
			if IsNotFound(err) {
				logger.Infof(ctx, "Artifact not found in Catalog. Key: %v", k)
				wi.progress.Set(i)
			} else {
				err = errors.Wrapf("CausedBy", err, "Failed to call catalog for Key: %v.", k)
				logger.Warnf(ctx, "Cache call failed: %v", err)
				return workqueue.WorkStatusFailed, err
			}
		} else if op != nil {
			writer := wi.downloadRequest.BaseTarget
			if isArray {
				// TODO: Check task interface, if it has outputs but literalmap is empty (or not matching output), error.
				dataReference, err := wi.downloadRequest.dataStore.ConstructReference(ctx,
					wi.downloadRequest.BaseTarget.GetOutputPrefixPath(), strconv.Itoa(int(i)))
				if err != nil {
					return workqueue.WorkStatusFailed, err
				}

				writer = ioutils.NewRemoteFileOutputWriter(ctx, wi.downloadRequest.dataStore,
					ioutils.NewRemoteFileOutputPaths(ctx, wi.downloadRequest.dataStore, dataReference))
			}

			logger.Debugf(ctx, "Persisting output to %v", writer.GetOutputPath())
			err = writer.Put(ctx, op)
			if err != nil {
				err = errors.Wrapf("CausedBy", err, "Failed to persist cached output for Key: %v.", k)
				logger.Warnf(ctx, "Cache write to output writer failed: %v", err)
				return workqueue.WorkStatusFailed, err
			}

			wi.cached.Set(i)
			wi.progress.Set(i)
		}
	}

	logger.Debugf(ctx, "Successfully wrote to catalog. Identifier [%v]", wi.downloadRequest.Identifier)
	return workqueue.WorkStatusSucceeded, nil
}

func NewArrayReaderProcessor(catalogClient Client, maxItemsPerRound int) ArrayReaderProcessor {
	return ArrayReaderProcessor{
		catalogClient:    catalogClient,
		maxItemsPerRound: maxItemsPerRound,
	}
}
