package catalog

import (
	"github.com/lyft/flytestdlib/bitarray"
	"github.com/lyft/flytestdlib/errors"
)

type future struct {
	responseStatus ResponseStatus
	readyHandler   ReadyHandler
}

func (f future) GetResponseStatus() ResponseStatus {
	return f.responseStatus
}

func (f *future) SetResponseStatus(status ResponseStatus) {
	f.responseStatus = status
}

func (f *future) OnReady(handler ReadyHandler) {
	f.readyHandler = handler
}

type downloadFuture struct {
	*future

	cachedResults *bitarray.BitSet
	cachedCount   int
	resultsSize   int
}

func (r downloadFuture) GetResponse() (DownloadResponse, error) {
	if r.GetResponseStatus() != ResponseStatusReady {
		return nil, errors.Errorf(ErrResponseNotReady, "Response is not ready yet.")
	}

	return r, nil
}

func (r downloadFuture) GetResultsSize() int {
	return r.resultsSize
}

func (r downloadFuture) GetCachedResults() *bitarray.BitSet {
	return r.cachedResults
}

func (r downloadFuture) GetCachedCount() int {
	return r.cachedCount
}

func newDownloadFuture(status ResponseStatus, cachedResults *bitarray.BitSet, resultsSize int,
	cachedCount int) downloadFuture {

	return downloadFuture{
		future: &future{
			responseStatus: status,
		},
		cachedCount:   cachedCount,
		cachedResults: cachedResults,
		resultsSize:   resultsSize,
	}
}

type uploadFuture struct {
	*future
}

func newUploadFuture(status ResponseStatus) uploadFuture {
	return uploadFuture{
		future: &future{
			responseStatus: status,
		},
	}
}
