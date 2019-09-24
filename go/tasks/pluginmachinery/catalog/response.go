package catalog

import (
	"github.com/lyft/flyteplugins/go/tasks/plugins/array/bitarray"
	"github.com/lyft/flytestdlib/errors"
)

type future struct {
	responseStatus ResponseStatus
}

func (f future) GetResponseStatus() ResponseStatus {
	return f.responseStatus
}

func (f *future) SetResponseStatus(status ResponseStatus) {
	f.responseStatus = status
}

type downloadFuture struct {
	*future

	cachedResults *bitarray.BitSet
	cachedCount   int
}

func (r downloadFuture) GetResponse() (DownloadResponse, error) {
	if r.GetResponseStatus() != ResponseStatusReady {
		return nil, errors.Errorf(ErrResponseNotReady, "Response is not ready yet.")
	}

	return r, nil
}

func (r downloadFuture) GetCachedResults() *bitarray.BitSet {
	return r.cachedResults
}

func (r downloadFuture) GetCachedCount() int {
	return r.cachedCount
}

func newDownloadFuture(status ResponseStatus, cachedResults *bitarray.BitSet, cachedCount int) downloadFuture {
	return downloadFuture{
		future: &future{
			responseStatus: status,
		},
		cachedCount:   cachedCount,
		cachedResults: cachedResults,
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
