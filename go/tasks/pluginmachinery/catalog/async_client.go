package catalog

import (
	"context"

	"github.com/lyft/flytestdlib/errors"

	"github.com/lyft/flyteplugins/go/tasks/array/bitarray"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
)

type UploadRequest struct {
	Key              Key
	ArtifactData     io.OutputReader
	ArtifactMetadata Metadata
}

type Future interface {
	GetResponseStatus() ResponseStatus
}

type UploadFuture interface {
	Future
}

type DownloadRequest struct {
	Key    Key
	Target io.OutputWriter
}

type DownloadFuture interface {
	Future
	GetResponse() (DownloadResponse, error)
}

type DownloadResponse interface {
	GetCachedResults() *bitarray.BitSet
	GetCachedCount() int
}

// An interface that helps async interaction with catalog service
type AsyncClient interface {
	// Returns if an entry exists for the given task and input. It returns the data as a LiteralMap
	Download(ctx context.Context, requests ...DownloadRequest) (outputFuture DownloadFuture, err error)

	// Adds a new entry to catalog for the given task execution context and the generated output
	Upload(ctx context.Context, requests ...UploadRequest) (putFuture UploadFuture, err error)
}

var _ AsyncClient = asyncClient{}

type ResponseStatus uint8

const (
	ResponseStatusNotReady ResponseStatus = iota
	ResponseStatusReady
)

const (
	ErrResponseNotReady errors.ErrorCode = "RESPONSE_NOT_READY"
	ErrSystemError      errors.ErrorCode = "SYSTEM_ERROR"
)

