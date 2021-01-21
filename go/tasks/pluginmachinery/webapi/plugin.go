// Defines the  interfaces to implement to add a Web API Plugin to the Flyte system. A WebAPI plugin is a plugin that
// runs the compute for a task on a separate system through a web call (REST/Grpc... etc.). By implementing the Plugin
// interface, the users of the Flyte system can then declare tasks of the handled task type in their workflows and the
// engine (Propeller) will route these tasks to your plugin to interact with the remote system.
package webapi

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
	SupportedTaskTypes []pluginsCore.TaskType

	// An instance of the plugin
	PluginLoader PluginLoader

	// Boolean that indicates if this plugin can be used as the default for unknown task types. There can only be
	// one default in the system
	IsDefault bool

	// A list of all task types for which this plugin should be default handler when multiple registered plugins
	// support the same task type. This must be a subset of RegisteredTaskTypes and at most one default per task type
	// is supported.
	DefaultForTaskTypes []pluginsCore.TaskType
}

// PluginSetupContext is the interface made available to the plugin loader when initializing the plugin.
type PluginSetupContext interface {
	// a metrics scope to publish stats under
	MetricsScope() promutils.Scope
	// Returns a secret manager that can retrieve configured secrets for this plugin
	SecretManager() pluginsCore.SecretManager
}

type TaskExecutionContext interface {
	// Returns a TaskReader, to retrieve the task details
	TaskReader() pluginsCore.TaskReader

	// Returns an input reader to retrieve input data
	InputReader() io.InputReader

	// Returns a handle to the Task's execution metadata.
	TaskExecutionMetadata() pluginsCore.TaskExecutionMetadata

	// Provides an output sync of type io.OutputWriter
	OutputWriter() io.OutputWriter
}

// Metadata about the resource to be synced from the remote service.
type ResourceMeta = interface{}

// Defines a simplified interface to author plugins for k8s resources.
type Plugin interface {
	// GetConfig gets the loaded plugin config. This will be used to control the interactions with the remote service.
	GetConfig() PluginConfig

	// ResourceRequirements analyzes the task to execute and determines the ResourceNamespace to be used when allocating tokens.
	ResourceRequirements(ctx context.Context, tCtx TaskExecutionContext) (
		namespace pluginsCore.ResourceNamespace, constraints pluginsCore.ResourceConstraintsSpec, err error)

	// Create a new resource using the TaskExecutionContext provided. Ideally, the remote service uses the name in the
	// TaskExecutionMetadata to launch the resource in an idempotent fashion. This function will be on the critical path
	// of the execution of a workflow and therefore it should not do expensive operations before making the webAPI call.
	// Flyte will call this api at least once. It's important that the callee service is idempotent to ensure no
	// resource leakage or duplicate requests. Flyte has an in-memory cache that does a best effort idempotency
	// guarantee.
	Create(ctx context.Context, tCtx TaskExecutionContext) (resource ResourceMeta, err error)

	// Get multiple resources that match all the keys. If the plugin hits any failure, it should stop and return
	// the failure. This batch will not be processed further. This API will be called asynchronously and periodically
	// to update the set of tasks currently in progress. It's acceptable if this API is blocking since it'll be called
	// from a background go-routine.
	Get(ctx context.Context, cached ResourceMeta) (latest ResourceMeta, err error)

	// Delete the object in the remote API using the resource key. Flyte will call this API at least once. If the
	// resource has already been deleted, the API should not fail.
	Delete(ctx context.Context, cached ResourceMeta) error

	// Status checks the status of a given resource and translates it to a Flyte-understandable PhaseInfo. This API
	// should avoid making any network calls and should run very efficiently.
	Status(ctx context.Context, resource ResourceMeta) (phase pluginsCore.PhaseInfo, err error)
}
