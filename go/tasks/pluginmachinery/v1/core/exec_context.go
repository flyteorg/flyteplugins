package core

import (
	"context"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/storage"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/io"
)

// An interface to access the TaskInformation
type TaskReader interface {
	// Returns the core TaskTemplate
	Read(ctx context.Context) (*core.TaskTemplate, error)
}

// An interface that is passed to every plugin invocation. It carries all meta and contextual information for the current
// task execution
type TaskExecutionContext interface {
	// Returns a resource manager that can be used to create reservations for limited resources
	ResourceManager() ResourceManager
	// Returns a secret manager that can retrieve configured secrets for this plugin
	SecretManager() SecretManager

	// Returns the max allowed dataset size that the outputwriter will accept
	MaxDatasetSizeBytes() int64
	// Returns a handle to the currently configured storage backend that can be used to communicate with the tasks or write metadata
	DataStore() *storage.DataStore

	// Returns a reader that retrieves previously stored plugin internal state. the state itself is immutable
	PluginStateReader() PluginStateReader
	// Returns a TaskReader, to retrieve task details
	TaskReader() TaskReader
	// Returns an input reader to retrieve input data
	InputReader() io.InputReader
	// Returns a handle to the Task's execution metadata.
	TaskExecutionMetadata() TaskExecutionMetadata

	// Provides an output sync of type io.OutputWriter
	OutputWriter() io.OutputWriter
	// Get a handle to the PluginStateWriter. Any mutations to the plugins internal state can be persisted using this
	// These mutation will be visible in the next round
	PluginStateWriter() PluginStateWriter

	// Get a handle to catalog client
	Catalog() CatalogClient

	// Returns a handle to the Task events recorder, which get stored in the Admin.
	EventsRecorder() EventsRecorder
}

// Task events recorder, which get stored in the Admin. If this is invoked multiple times,
// multiple events will be sent to Admin. It is not recommended that one uses this interface, a transition will trigger an auto event to admin
type EventsRecorder interface {
	RecordRaw(ctx context.Context, ev PhaseInfo) error
}
