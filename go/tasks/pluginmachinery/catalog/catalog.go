package catalog

import (
	"context"
	"fmt"

	"github.com/lyft/flytestdlib/bitarray"

	"github.com/lyft/flytestdlib/errors"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
)

var _ Client = ClientImpl{}

type ResponseStatus uint8

const (
	ResponseStatusNotReady ResponseStatus = iota
	ResponseStatusReady
)

const (
	ErrResponseNotReady errors.ErrorCode = "RESPONSE_NOT_READY"
	ErrSystemError      errors.ErrorCode = "SYSTEM_ERROR"
)

type Metadata struct {
	WorkflowExecutionIdentifier *core.WorkflowExecutionIdentifier
	NodeExecutionIdentifier     *core.NodeExecutionIdentifier
	TaskExecutionIdentifier     *core.TaskExecutionIdentifier
}

type Key struct {
	Identifier     core.Identifier
	CacheVersion   string
	TypedInterface core.TypedInterface
	InputReader    io.InputReader
}

func (k Key) String() string {
	return fmt.Sprintf("%v:%v", k.Identifier, k.CacheVersion)
}

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
	GetResultsSize() int
	GetCachedCount() int
}

// An interface to interest with the catalog service
type Client interface {
	// Returns if an entry exists for the given task and input. It returns the data as a LiteralMap
	Download(ctx context.Context, requests ...DownloadRequest) (outputFuture DownloadFuture, err error)

	// Adds a new entry to catalog for the given task execution context and the generated output
	Upload(ctx context.Context, requests ...UploadRequest) (putFuture UploadFuture, err error)
}

// TODO: Match the actual catalog service interface
type RawClient interface {
	Get(ctx context.Context) (*core.LiteralMap, error)
	Put(ctx context.Context, key Key, reader io.OutputReader, metadata Metadata) error
}
