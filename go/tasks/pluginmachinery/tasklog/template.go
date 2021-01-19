package tasklog

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
)

// A simple log plugin that supports templates in urls to build the final log link. Supported templates are:
// {{ .podName }}: Gets the pod name as it shows in k8s dashboard,
// {{ .namespace }}: K8s namespace where the pod runs,
// {{ .containerName }}: The container name that generated the log,
// {{ .containerId }}: The container id docker/crio generated at run time,
// {{ .logName }}: A deployment specific name where to expect the logs to be.
// {{ .hostname }}: The hostname where the pod is running and where logs reside.
type TemplateLogPlugin struct {
	templateUris  []string
	messageFormat core.TaskLog_MessageFormat
}

type regexValPair struct {
	regex *regexp.Regexp
	val   string
}

var podNameRegex = mustCreateRegex("podName")
var namespaceRegex = mustCreateRegex("namespace")
var containerNameRegex = mustCreateRegex("containerName")
var containerIDRegex = mustCreateRegex("containerID")
var logNameRegex = mustCreateRegex("logName")
var hostnameRegex = mustCreateRegex("hostname")

func mustCreateRegex(varName string) *regexp.Regexp {
	return regexp.MustCompile(fmt.Sprintf(`(?i){{\s*[\.$]%s\s*}}`, varName))
}

func replaceAll(template string, values []regexValPair) string {
	for _, v := range values {
		template = v.regex.ReplaceAllString(template, v.val)
	}

	return template
}

func (s TemplateLogPlugin) GetTaskLog(podName, namespace, containerName, containerID, logName string) (core.TaskLog, error) {
	o, err := s.GetTaskLogs(Input{
		LogName:       logName,
		Namespace:     namespace,
		PodName:       podName,
		ContainerName: containerName,
		ContainerID:   containerID,
	})

	if err != nil || len(o.TaskLogs) == 0 {
		return core.TaskLog{}, err
	}

	return *o.TaskLogs[0], nil
}

func (s TemplateLogPlugin) GetTaskLogs(input Input) (Output, error) {
	// Container IDs are prefixed with docker://, cri-o://, etc. which is stripped by fluentd before pushing to a log
	// stream. Therefore, we must also strip the prefix.
	containerID := input.ContainerID
	stripDelimiter := "://"
	if split := strings.Split(input.ContainerID, stripDelimiter); len(split) > 1 {
		containerID = split[1]
	}

	taskLogs := make([]*core.TaskLog, 0, len(s.templateUris))
	for _, templateURI := range s.templateUris {
		taskLogs = append(taskLogs, &core.TaskLog{
			Uri: replaceAll(
				templateURI,
				[]regexValPair{
					{
						regex: podNameRegex,
						val:   input.PodName,
					},
					{
						regex: namespaceRegex,
						val:   input.Namespace,
					},
					{
						regex: containerNameRegex,
						val:   input.ContainerName,
					},
					{
						regex: containerIDRegex,
						val:   containerID,
					},
					{
						regex: logNameRegex,
						val:   input.LogName,
					},
					{
						regex: hostnameRegex,
						val:   input.HostName,
					},
				},
			),
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
// {{ .namespace }}: K8s namespace where the pod runs,
// {{ .containerName }}: The container name that generated the log,
// {{ .containerId }}: The container id docker/crio generated at run time,
// {{ .logName }}: A deployment specific name where to expect the logs to be.
// {{ .hostname }}: The hostname where the pod is running and where logs reside.
func NewTemplateLogPlugin(templateUris []string, messageFormat core.TaskLog_MessageFormat) TemplateLogPlugin {
	return TemplateLogPlugin{
		templateUris:  templateUris,
		messageFormat: messageFormat,
	}
}
