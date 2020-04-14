package service

import (
	"context"

	"github.com/lyft/flytestdlib/promutils"

	pluginsCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
)

//go:generate mockery -all -case=underscore

// A Lazy loading function, that will load the plugin. Plugins should be initialized in this method. It is guaranteed
// that the plugin loader will be called before any Handle/Abort/Finalize functions are invoked
type PluginLoader func(ctx context.Context, iCtx PluginSetupContext) (Plugin, error)

// PluginEntry is a structure that is used to indicate to the system a K8s plugin
type PluginEntry struct {
	// ID/Name of the plugin. This will be used to identify this plugin and has to be unique in the entire system
	// All functions like enabling and disabling a plugin use this ID
	ID pluginsCore.TaskType
	// A list of all the task types for which this plugin is applicable.
	RegisteredTaskTypes []pluginsCore.TaskType
	// An instance of the plugin
	PluginLoader PluginLoader
	// Boolean that indicates if this plugin can be used as the default for unknown task types. There can only be
	// one default in the system
	IsDefault bool
}

type PluginSetupContext interface {
	// a metrics scope to publish stats under
	MetricsScope() promutils.Scope
	// Returns a secret manager that can retrieve configured secrets for this plugin
	SecretManager() pluginsCore.SecretManager
}

type PluginContext interface {
	// Returns a TaskReader, to retrieve task details
	TaskReader() pluginsCore.TaskReader
	// Returns an input reader to retrieve input data
	InputReader() io.InputReader
	// Returns a handle to the Task's execution metadata.
	TaskExecutionMetadata() pluginsCore.TaskExecutionMetadata
	// Provides an output sync of type io.OutputWriter
	OutputWriter() io.OutputWriter
}

// Name/Identifier of the resource in the remote service.
type RemoteResourceKey struct {
	Name string // Optional resourceName, ideally this name is generated using the TaskExecutionMetadata
}

// The resource to be sycned from the remote
type RemoteResource interface{}

// Defines a simplified interface to author plugins for k8s resources.
type Plugin interface {
	GetPluginProperties() PluginProperties
	// Create a new resource using the PluginContext provided. Ideally, the remote service uses the name in the TaskExecutionMetadata
	// to launch the resource idempotently
	Create(ctx context.Context, tCtx PluginContext) (createdResources []RemoteResourceKey, err error)
	// Get multiple resources that match all the keys.
	Get(ctx context.Context, key ...RemoteResourceKey) (resources []RemoteResource, err error)
	// Delete the object in the remote API using the resource key
	Delete(ctx context.Context, key ...RemoteResourceKey) error
	// Status checks the status of a given resource and translates it to a Flyte-understandable PhaseInfo.
	Status(ctx context.Context, tCtx PluginContext, resource RemoteResource) (phase pluginsCore.PhaseInfo, err error)
}
