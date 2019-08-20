package core

import (
	"context"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/storage"

	"github.com/lyft/flyteplugins/go/tasks/v1/pluginmachinery/io"
)

type TaskReader interface {
	Read(ctx context.Context) (*core.TaskTemplate, error)
}

type TaskExecutionContext interface {
	EventsRecorder() EventsRecorder
	ResourceManager() ResourceManager
	MaxDatasetSizeBytes() int64
	DataStore() *storage.DataStore
	PluginStateReader() PluginStateReader
	TaskReader() TaskReader
	InputReader() io.InputReader
	TaskExecutionMetadata() TaskExecutionMetadata

	OutputWriter() io.OutputWriter
	PluginStateWriter() PluginStateWriter
	Catalog() CatalogClient
}

type EventsRecorder interface {
	RecordRaw(ctx context.Context, ev PhaseInfo) error
}
