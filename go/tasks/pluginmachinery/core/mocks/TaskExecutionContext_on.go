package mocks

import (
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/catalog"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
	"github.com/lyft/flytestdlib/storage"
	"github.com/stretchr/testify/mock"
)

type CatalogCall struct {
	*mock.Call
}

func (c *CatalogCall) Return(
	Clientvalue catalog.Client,
) *CatalogCall {
	return &CatalogCall{Call: c.Call.Return(
		Clientvalue,
	)}
}

func (_m *TaskExecutionContext) OnCatalogMatch(matchers ...interface{}) *CatalogCall {
	c := _m.On("Catalog", matchers)
	return &CatalogCall{Call: c}
}

func (_m *TaskExecutionContext) OnCatalog() *CatalogCall {
	c := _m.On("Catalog")
	return &CatalogCall{Call: c}
}

type DataStoreCall struct {
	*mock.Call
}

func (c *DataStoreCall) Return(
	DataStorevalue storage.DataStore,
) *DataStoreCall {
	return &DataStoreCall{Call: c.Call.Return(
		DataStorevalue,
	)}
}

func (_m *TaskExecutionContext) OnDataStoreMatch(matchers ...interface{}) *DataStoreCall {
	c := _m.On("DataStore", matchers)
	return &DataStoreCall{Call: c}
}

func (_m *TaskExecutionContext) OnDataStore() *DataStoreCall {
	c := _m.On("DataStore")
	return &DataStoreCall{Call: c}
}

type EventsRecorderCall struct {
	*mock.Call
}

func (c *EventsRecorderCall) Return(
	EventsRecordervalue core.EventsRecorder,
) *EventsRecorderCall {
	return &EventsRecorderCall{Call: c.Call.Return(
		EventsRecordervalue,
	)}
}

func (_m *TaskExecutionContext) OnEventsRecorderMatch(matchers ...interface{}) *EventsRecorderCall {
	c := _m.On("EventsRecorder", matchers)
	return &EventsRecorderCall{Call: c}
}

func (_m *TaskExecutionContext) OnEventsRecorder() *EventsRecorderCall {
	c := _m.On("EventsRecorder")
	return &EventsRecorderCall{Call: c}
}

type InputReaderCall struct {
	*mock.Call
}

func (c *InputReaderCall) Return(
	InputReadervalue io.InputReader,
) *InputReaderCall {
	return &InputReaderCall{Call: c.Call.Return(
		InputReadervalue,
	)}
}

func (_m *TaskExecutionContext) OnInputReaderMatch(matchers ...interface{}) *InputReaderCall {
	c := _m.On("InputReader", matchers)
	return &InputReaderCall{Call: c}
}

func (_m *TaskExecutionContext) OnInputReader() *InputReaderCall {
	c := _m.On("InputReader")
	return &InputReaderCall{Call: c}
}

type MaxDatasetSizeBytesCall struct {
	*mock.Call
}

func (c *MaxDatasetSizeBytesCall) Return(
	int64value int64,
) *MaxDatasetSizeBytesCall {
	return &MaxDatasetSizeBytesCall{Call: c.Call.Return(
		int64value,
	)}
}

func (_m *TaskExecutionContext) OnMaxDatasetSizeBytesMatch(matchers ...interface{}) *MaxDatasetSizeBytesCall {
	c := _m.On("MaxDatasetSizeBytes", matchers)
	return &MaxDatasetSizeBytesCall{Call: c}
}

func (_m *TaskExecutionContext) OnMaxDatasetSizeBytes() *MaxDatasetSizeBytesCall {
	c := _m.On("MaxDatasetSizeBytes")
	return &MaxDatasetSizeBytesCall{Call: c}
}

type OutputWriterCall struct {
	*mock.Call
}

func (c *OutputWriterCall) Return(
	OutputWritervalue io.OutputWriter,
) *OutputWriterCall {
	return &OutputWriterCall{Call: c.Call.Return(
		OutputWritervalue,
	)}
}

func (_m *TaskExecutionContext) OnOutputWriterMatch(matchers ...interface{}) *OutputWriterCall {
	c := _m.On("OutputWriter", matchers)
	return &OutputWriterCall{Call: c}
}

func (_m *TaskExecutionContext) OnOutputWriter() *OutputWriterCall {
	c := _m.On("OutputWriter")
	return &OutputWriterCall{Call: c}
}

type PluginStateReaderCall struct {
	*mock.Call
}

func (c *PluginStateReaderCall) Return(
	PluginStateReadervalue core.PluginStateReader,
) *PluginStateReaderCall {
	return &PluginStateReaderCall{Call: c.Call.Return(
		PluginStateReadervalue,
	)}
}

func (_m *TaskExecutionContext) OnPluginStateReaderMatch(matchers ...interface{}) *PluginStateReaderCall {
	c := _m.On("PluginStateReader", matchers)
	return &PluginStateReaderCall{Call: c}
}

func (_m *TaskExecutionContext) OnPluginStateReader() *PluginStateReaderCall {
	c := _m.On("PluginStateReader")
	return &PluginStateReaderCall{Call: c}
}

type PluginStateWriterCall struct {
	*mock.Call
}

func (c *PluginStateWriterCall) Return(
	PluginStateWritervalue core.PluginStateWriter,
) *PluginStateWriterCall {
	return &PluginStateWriterCall{Call: c.Call.Return(
		PluginStateWritervalue,
	)}
}

func (_m *TaskExecutionContext) OnPluginStateWriterMatch(matchers ...interface{}) *PluginStateWriterCall {
	c := _m.On("PluginStateWriter", matchers)
	return &PluginStateWriterCall{Call: c}
}

func (_m *TaskExecutionContext) OnPluginStateWriter() *PluginStateWriterCall {
	c := _m.On("PluginStateWriter")
	return &PluginStateWriterCall{Call: c}
}

type ResourceManagerCall struct {
	*mock.Call
}

func (c *ResourceManagerCall) Return(
	ResourceManagervalue core.ResourceManager,
) *ResourceManagerCall {
	return &ResourceManagerCall{Call: c.Call.Return(
		ResourceManagervalue,
	)}
}

func (_m *TaskExecutionContext) OnResourceManagerMatch(matchers ...interface{}) *ResourceManagerCall {
	c := _m.On("ResourceManager", matchers)
	return &ResourceManagerCall{Call: c}
}

func (_m *TaskExecutionContext) OnResourceManager() *ResourceManagerCall {
	c := _m.On("ResourceManager")
	return &ResourceManagerCall{Call: c}
}

type SecretManagerCall struct {
	*mock.Call
}

func (c *SecretManagerCall) Return(
	SecretManagervalue core.SecretManager,
) *SecretManagerCall {
	return &SecretManagerCall{Call: c.Call.Return(
		SecretManagervalue,
	)}
}

func (_m *TaskExecutionContext) OnSecretManagerMatch(matchers ...interface{}) *SecretManagerCall {
	c := _m.On("SecretManager", matchers)
	return &SecretManagerCall{Call: c}
}

func (_m *TaskExecutionContext) OnSecretManager() *SecretManagerCall {
	c := _m.On("SecretManager")
	return &SecretManagerCall{Call: c}
}

type TaskExecutionMetadataCall struct {
	*mock.Call
}

func (c *TaskExecutionMetadataCall) Return(
	TaskExecutionMetadatavalue core.TaskExecutionMetadata,
) *TaskExecutionMetadataCall {
	return &TaskExecutionMetadataCall{Call: c.Call.Return(
		TaskExecutionMetadatavalue,
	)}
}

func (_m *TaskExecutionContext) OnTaskExecutionMetadataMatch(matchers ...interface{}) *TaskExecutionMetadataCall {
	c := _m.On("TaskExecutionMetadata", matchers)
	return &TaskExecutionMetadataCall{Call: c}
}

func (_m *TaskExecutionContext) OnTaskExecutionMetadata() *TaskExecutionMetadataCall {
	c := _m.On("TaskExecutionMetadata")
	return &TaskExecutionMetadataCall{Call: c}
}

type TaskReaderCall struct {
	*mock.Call
}

func (c *TaskReaderCall) Return(
	TaskReadervalue core.TaskReader,
) *TaskReaderCall {
	return &TaskReaderCall{Call: c.Call.Return(
		TaskReadervalue,
	)}
}

func (_m *TaskExecutionContext) OnTaskReaderMatch(matchers ...interface{}) *TaskReaderCall {
	c := _m.On("TaskReader", matchers)
	return &TaskReaderCall{Call: c}
}

func (_m *TaskExecutionContext) OnTaskReader() *TaskReaderCall {
	c := _m.On("TaskReader")
	return &TaskReaderCall{Call: c}
}
