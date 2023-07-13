package tasklog

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
)

func MustCreateRegex(varName string) *regexp.Regexp {
	return regexp.MustCompile(fmt.Sprintf(`(?i){{\s*[\.$]%s\s*}}`, varName))
}

var defaultRegexes = struct {
	PodName              *regexp.Regexp
	PodUID               *regexp.Regexp
	Namespace            *regexp.Regexp
	ContainerName        *regexp.Regexp
	ContainerID          *regexp.Regexp
	LogName              *regexp.Regexp
	Hostname             *regexp.Regexp
	PodRFC3339StartTime  *regexp.Regexp
	PodRFC3339FinishTime *regexp.Regexp
	PodUnixStartTime     *regexp.Regexp
	PodUnixFinishTime    *regexp.Regexp
	TaskID               *regexp.Regexp
	TaskVersion          *regexp.Regexp
	TaskProject          *regexp.Regexp
	TaskDomain           *regexp.Regexp
	TaskRetryAttempt     *regexp.Regexp
	NodeID               *regexp.Regexp
	ExecutionName        *regexp.Regexp
	ExecutionProject     *regexp.Regexp
	ExecutionDomain      *regexp.Regexp
}{
	MustCreateRegex("podName"),
	MustCreateRegex("podUID"),
	MustCreateRegex("namespace"),
	MustCreateRegex("containerName"),
	MustCreateRegex("containerID"),
	MustCreateRegex("logName"),
	MustCreateRegex("hostname"),
	MustCreateRegex("podRFC3339StartTime"),
	MustCreateRegex("podRFC3339FinishTime"),
	MustCreateRegex("podUnixStartTime"),
	MustCreateRegex("podUnixFinishTime"),
	MustCreateRegex("taskID"),
	MustCreateRegex("taskVersion"),
	MustCreateRegex("taskProject"),
	MustCreateRegex("taskDomain"),
	MustCreateRegex("taskRetryAttempt"),
	MustCreateRegex("nodeID"),
	MustCreateRegex("executionName"),
	MustCreateRegex("executionProject"),
	MustCreateRegex("executionDomain"),
}

func replaceAll(template string, vars TemplateVars) string {
	for _, v := range vars {
		if len(v.Value) > 0 {
			template = v.Regex.ReplaceAllLiteralString(template, v.Value)
		}
	}
	return template
}

func (input Input) ToTemplateVars() TemplateVars {
	// Container IDs are prefixed with docker://, cri-o://, etc. which is stripped by fluentd before pushing to a log
	// stream. Therefore, we must also strip the prefix.
	containerID := input.ContainerID
	stripDelimiter := "://"
	if split := strings.Split(input.ContainerID, stripDelimiter); len(split) > 1 {
		containerID = split[1]
	}

	vars := TemplateVars{
		{
			Regex: defaultRegexes.PodName,
			Value: input.PodName,
		},
		{
			Regex: defaultRegexes.PodUID,
			Value: input.PodUID,
		},
		{
			Regex: defaultRegexes.Namespace,
			Value: input.Namespace,
		},
		{
			Regex: defaultRegexes.ContainerName,
			Value: input.ContainerName,
		},
		{
			Regex: defaultRegexes.ContainerID,
			Value: containerID,
		},
		{
			Regex: defaultRegexes.LogName,
			Value: input.LogName,
		},
		{
			Regex: defaultRegexes.Hostname,
			Value: input.HostName,
		},
		{
			Regex: defaultRegexes.PodRFC3339StartTime,
			Value: input.PodRFC3339StartTime,
		},
		{
			Regex: defaultRegexes.PodRFC3339FinishTime,
			Value: input.PodRFC3339FinishTime,
		},
		{
			Regex: defaultRegexes.PodUnixStartTime,
			Value: strconv.FormatInt(input.PodUnixStartTime, 10),
		},
		{
			Regex: defaultRegexes.PodUnixFinishTime,
			Value: strconv.FormatInt(input.PodUnixFinishTime, 10),
		},
	}

	if input.TaskExecutionIdentifier != nil {
		vars = append(vars, TemplateVar{
			Regex: defaultRegexes.TaskRetryAttempt,
			Value: strconv.FormatUint(uint64(input.TaskExecutionIdentifier.RetryAttempt), 10),
		})
		if input.TaskExecutionIdentifier.TaskId != nil {
			vars = append(
				vars,
				TemplateVar{
					Regex: defaultRegexes.TaskID,
					Value: input.TaskExecutionIdentifier.TaskId.Name,
				},
				TemplateVar{
					Regex: defaultRegexes.TaskVersion,
					Value: input.TaskExecutionIdentifier.TaskId.Version,
				},
				TemplateVar{
					Regex: defaultRegexes.TaskProject,
					Value: input.TaskExecutionIdentifier.TaskId.Project,
				},
				TemplateVar{
					Regex: defaultRegexes.TaskDomain,
					Value: input.TaskExecutionIdentifier.TaskId.Domain,
				},
			)
		}
		if input.TaskExecutionIdentifier.NodeExecutionId != nil {
			vars = append(vars, TemplateVar{
				Regex: defaultRegexes.NodeID,
				Value: input.TaskExecutionIdentifier.NodeExecutionId.NodeId,
			})
			if input.TaskExecutionIdentifier.NodeExecutionId.ExecutionId != nil {
				vars = append(
					vars,
					TemplateVar{
						Regex: defaultRegexes.ExecutionName,
						Value: input.TaskExecutionIdentifier.NodeExecutionId.ExecutionId.Name,
					},
					TemplateVar{
						Regex: defaultRegexes.ExecutionProject,
						Value: input.TaskExecutionIdentifier.NodeExecutionId.ExecutionId.Project,
					},
					TemplateVar{
						Regex: defaultRegexes.ExecutionDomain,
						Value: input.TaskExecutionIdentifier.NodeExecutionId.ExecutionId.Domain,
					},
				)
			}
		}
	}

	return append(vars, input.ExtraTemplateVars...)
}

// A simple log plugin that supports templates in urls to build the final log link.
// See `defaultRegexes` for supported templates.
type TemplateLogPlugin struct {
	templateUris  []string
	messageFormat core.TaskLog_MessageFormat
}

func (s TemplateLogPlugin) GetTaskLog(podName, podUID, namespace, containerName, containerID, logName string, podRFC3339StartTime string, podRFC3339FinishTime string, podUnixStartTime, podUnixFinishTime int64) (core.TaskLog, error) {
	o, err := s.GetTaskLogs(Input{
		LogName:              logName,
		Namespace:            namespace,
		PodName:              podName,
		PodUID:               podUID,
		ContainerName:        containerName,
		ContainerID:          containerID,
		PodRFC3339StartTime:  podRFC3339StartTime,
		PodRFC3339FinishTime: podRFC3339FinishTime,
		PodUnixStartTime:     podUnixStartTime,
		PodUnixFinishTime:    podUnixFinishTime,
	})

	if err != nil || len(o.TaskLogs) == 0 {
		return core.TaskLog{}, err
	}

	return *o.TaskLogs[0], nil
}

func (s TemplateLogPlugin) GetTaskLogs(input Input) (Output, error) {
	templateVars := input.ToTemplateVars()
	taskLogs := make([]*core.TaskLog, 0, len(s.templateUris))
	for _, templateURI := range s.templateUris {
		taskLogs = append(taskLogs, &core.TaskLog{
			Uri:           replaceAll(templateURI, templateVars),
			Name:          input.LogName,
			MessageFormat: s.messageFormat,
		})
	}

	return Output{
		TaskLogs: taskLogs,
	}, nil
}

// NewTemplateLogPlugin creates a template-based log plugin with the provided template Uri and message format. Supported
// templates are:
// {{ .podName }}: Gets the pod name as it shows in k8s dashboard,
// {{ .podUID }}: Gets the pod UID,
// {{ .namespace }}: K8s namespace where the pod runs,
// {{ .containerName }}: The container name that generated the log,
// {{ .containerId }}: The container id docker/crio generated at run time,
// {{ .logName }}: A deployment specific name where to expect the logs to be.
// {{ .hostname }}: The hostname where the pod is running and where logs reside.
// {{ .PodRFC3339StartTime }}: The pod creation time in RFC3339 format
// {{ .PodRFC3339FinishTime }}: Don't have a good mechanism for this yet, but approximating with time.Now for now
// {{ .podUnixStartTime }}: The pod creation time (in unix seconds, not millis)
// {{ .podUnixFinishTime }}: Don't have a good mechanism for this yet, but approximating with time.Now for now
func NewTemplateLogPlugin(templateUris []string, messageFormat core.TaskLog_MessageFormat) TemplateLogPlugin {
	return TemplateLogPlugin{
		templateUris:  templateUris,
		messageFormat: messageFormat,
	}
}
