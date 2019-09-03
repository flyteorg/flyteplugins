package catalog

import (
	"context"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/workqueue"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
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

type UploadRequest struct {
	Key              Key
	ArtifactData     io.OutputReader
	ArtifactMetadata Metadata
}

type Future interface {
	GetStatus() workqueue.WorkStatus
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
}

// An interface to interest with the catalog service
type Client interface {
	// Returns if an entry exists for the given task and input. It returns the data as a LiteralMap
	Download(ctx context.Context, keys ...DownloadRequest) (outputFuture DownloadFuture, err error)

	// Adds a new entry to catalog for the given task execution context and the generated output
	Upload(ctx context.Context, inputs ...UploadRequest) (putFuture UploadFuture, err error)
}

// TODO: Match the actual catalog service interface
type RawClient interface {
	Get(ctx context.Context) (*core.LiteralMap, error)
	Put(ctx context.Context, key Key, reader io.OutputReader, metadata Metadata) error
}

type client struct {
	Reader workqueue.IndexedWorkQueue
	Writer workqueue.IndexedWorkQueue
}

func (c client) Download(ctx context.Context, keys ...DownloadRequest) (outputFuture DownloadFuture, err error) {
	panic("implement me")
}

func (c client) Upload(ctx context.Context, inputs ...UploadRequest) (putFuture UploadFuture, err error) {
	panic("implement me")
}
