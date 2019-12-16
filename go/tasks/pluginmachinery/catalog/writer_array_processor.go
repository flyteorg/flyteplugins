package catalog

import (
	"context"
	"fmt"
	"reflect"
	"strconv"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"

	"github.com/lyft/flytestdlib/bitarray"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/ioutils"

	"github.com/lyft/flyteplugins/go/tasks/errors"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/workqueue"
	"github.com/lyft/flytestdlib/logger"
)

type WriterArrayWorkItem struct {
	// WriterArrayWorkItem Inputs
	request UploadArrayRequest

	progress *bitarray.BitSet
}

func NewArrayWriterWorkItem(request UploadArrayRequest) *WriterArrayWorkItem {
	return &WriterArrayWorkItem{
		request: request,
	}
}

type writerArrayProcessor struct {
	catalogClient    Client
	maxItemsPerRound int
}

func (p writerArrayProcessor) Process(ctx context.Context, workItem workqueue.WorkItem) (workqueue.WorkStatus, error) {
	wi, casted := workItem.(*WriterArrayWorkItem)
	if !casted {
		return workqueue.WorkStatusNotDone, fmt.Errorf("wrong work item type. Received: %v", reflect.TypeOf(workItem))
	}

	if wi.progress == nil {
		wi.progress = bitarray.NewBitSet(uint(wi.request.Count))
	}

	isArray := wi.request.Count > 0
	for i := uint(0); i < uint(wi.request.Count); i++ {
		inputReader := wi.request.BaseInputReader
		var outputReader io.OutputReader
		if isArray {
			if !wi.request.Indexes.IsSet(i) {
				continue
			}

			if wi.progress.IsSet(i) {
				logger.Debugf(ctx, "Catalog lookup already ran for index [%v].", i)
				continue
			}

			indexedInputLocation, err := wi.request.dataStore.ConstructReference(ctx,
				wi.request.BaseInputReader.GetInputPrefixPath(),
				strconv.Itoa(int(i)))
			if err != nil {
				return workqueue.WorkStatusNotDone, err
			}

			inputReader = ioutils.NewRemoteFileInputReader(ctx, wi.request.dataStore,
				ioutils.NewInputFilePaths(ctx, wi.request.dataStore, indexedInputLocation))

			indexedOutputLocation, err := wi.request.dataStore.ConstructReference(ctx,
				wi.request.BaseArtifactData.GetOutputPrefixPath(),
				strconv.Itoa(int(i)))
			if err != nil {
				return workqueue.WorkStatusNotDone, err
			}

			// TODO: size limit is weird to be passed here...
			outputReader = ioutils.NewRemoteFileOutputReader(ctx, wi.request.dataStore,
				ioutils.NewRemoteFileOutputPaths(ctx, wi.request.dataStore, indexedOutputLocation),
				int64(999999999))
		} else {
			outputReader = ioutils.NewRemoteFileOutputReader(ctx, wi.request.dataStore,
				wi.request.BaseArtifactData, int64(999999999))
		}

		k := Key{
			Identifier:     wi.request.Identifier,
			CacheVersion:   wi.request.CacheVersion,
			TypedInterface: wi.request.TypedInterface,
			InputReader:    inputReader,
		}

		err := p.catalogClient.Put(ctx, k, outputReader, wi.request.ArtifactMetadata)
		if err != nil {
			logger.Errorf(ctx, "Error putting to catalog [%s]", err)
			return workqueue.WorkStatusNotDone, errors.Wrapf(errors.DownstreamSystemError, err,
				"Error writing to catalog, key id [%v] cache version [%v]",
				k.Identifier, k.CacheVersion)
		}

		wi.progress.Set(i)
	}

	logger.Debugf(ctx, "Successfully wrote to catalog.")

	return workqueue.WorkStatusSucceeded, nil
}

func NewWriterArrayProcessor(catalogClient Client, maxItemsPerRound int) workqueue.Processor {
	return writerArrayProcessor{
		catalogClient:    catalogClient,
		maxItemsPerRound: maxItemsPerRound,
	}
}
