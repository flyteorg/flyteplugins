// This provides a simplistic API to implement scalable backend Sensor Plugins
// Sensors are defined as tasks that wait for some event to happen, before marking them-selves are ready.
// Sensor Implementations should be consistent in behavior, such that they are either in ``WaitState`` or proceed to
// ``ReadyState``. Wait Indicates the the condition is not met. ReadyState implies that the condition is met.
// Once ReadyState is achieved, subsequent calls should result in ``ReadyState`` only. This can happen because recovery
// from failures may need successive retries.
package sensors

import (
	"context"
	pluginsCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
)

//go:generate mockery -all -case=underscore
//go:generate enumer -type=Phase

type Phase int8

const (
	// Indicates that the Sensor condition is not yet met. So it is still waiting for the expected condition to be
	// fulfilled
	PhaseWait Phase = iota
	// Indicates the Sensor condition has been met and the Sensor can be marked as completed
	PhaseReady
	// Indicates that the Sensor has encountered a permanent failure and should be marked as failed. Sensors
	// usually do not have retries, so this will permanent mark the task as failed
	PhaseFailure
)

type PhaseInfo struct {
	Phase  Phase
	Reason string
}

type Properties struct {
	// Maximum Desirable Rate (number of requests per second) that the downstream system can handle
	MaxRate float64
	// Maximum Burst rate allowed for this Plugin
	BurstRate int
}

// PluginEntry is a structure that is used to indicate to the system a K8s plugin
type PluginEntry struct {
	// ID/Name of the plugin. This will be used to identify this plugin and has to be unique in the entire system
	// All functions like enabling and disabling a plugin use this ID
	ID pluginsCore.TaskType
	// A list of all the task types for which this plugin is applicable.
	RegisteredTaskTypes []pluginsCore.TaskType
	// Instance of the Sensor Plugin
	Plugin Plugin
	// Properties desirable for this Sensor Plugin
	Properties Properties
}

// Simplified interface for Sensor Plugins. This context is passed for every Poke invoked
type PluginContext interface {
	// Returns a secret manager that can retrieve configured secrets for this plugin
	SecretManager() pluginsCore.SecretManager

	// Returns a TaskReader, to retrieve task details
	TaskReader() pluginsCore.TaskReader

	// Returns an input reader to retrieve input data
	InputReader() io.InputReader

	// Returns a handle to the Task's execution metadata.
	TaskExecutionMetadata() pluginsCore.TaskExecutionMetadata
}

// Simplified interface for Sensor Plugins initialization
type PluginSetupContext interface {
	// Returns a secret manager that can retrieve configured secrets for this plugin
	SecretManager() pluginsCore.SecretManager
}

type Plugin interface {
	// This is called only once, when the Plugin is being initialized. This can be used to initialize remote clients
	// and other such things. Make sure this is not doing extremely expensive operations
	// Error in this case will halt the loading of the module and may result in ejection of the plugin
	Initialize(ctx context.Context, iCtx PluginSetupContext) error

	// The function will be periodically invoked. It should return a Phase or an error
	// Phase indicates a condition in the sensor. For any system error's the ``error`` should be returned.
	// System errors will automatically cause system retries and most importantly indicate the reason for failure
	// this is a blocking call and should not be used to do very expensive operations.
	Poke(ctx context.Context, pluginCtx PluginContext) (PhaseInfo, error)
}
