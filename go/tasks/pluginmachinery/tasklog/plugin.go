package tasklog

import "github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"

type TemplateVars map[string]interface{}

// Input contains all available information about task's execution that a log plugin can use to construct task's
// log links.
type Input struct {
	HostName                string
	PodName                 string
	Namespace               string
	ContainerName           string
	ContainerID             string
	LogName                 string
	PodRFC3339StartTime     string
	PodRFC3339FinishTime    string
	PodUnixStartTime        int64
	PodUnixFinishTime       int64
	PodUID                  string
	TaskExecutionIdentifier core.TaskExecutionIdentifier
}

// Output contains all task logs a plugin generates for a given Input.
type Output struct {
	TaskLogs []*core.TaskLog
}

// Plugin represents an interface for task log plugins to implement to plug generated task log links into task events.
type Plugin interface {
	// Generates a TaskLog object given necessary computation information
	GetTaskLogs(i Input, v ...TemplateVars) (logs Output, err error)
}
