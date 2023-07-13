package tasklog

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"text/template"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
)

func (t TemplateVars) Merge(others ...TemplateVars) {
	for _, other := range others {
		for k, v := range other {
			t[k] = v
		}
	}
}

func (t TemplateVars) MergeProviders(providers ...TemplateVarsProvider) {
	var others []TemplateVars
	for _, p := range providers {
		others = append(others, p.ToTemplateVars())
	}
	t.Merge(others...)
}

func (input Input) ToTemplateVars() TemplateVars {
	// Container IDs are prefixed with docker://, cri-o://, etc. which is stripped by fluentd before pushing to a log
	// stream. Therefore, we must also strip the prefix.
	containerID := input.ContainerID
	stripDelimiter := "://"
	if split := strings.Split(input.ContainerID, stripDelimiter); len(split) > 1 {
		containerID = split[1]
	}

	return TemplateVars{
		"podName":           input.PodName,
		"podUID":            input.PodUID,
		"namespace":         input.Namespace,
		"containerName":     input.ContainerName,
		"containerId":       containerID,
		"logName":           input.LogName,
		"hostname":          input.HostName,
		"podUnixStartTime":  strconv.FormatInt(input.PodUnixStartTime, 10),
		"podUnixFinishTime": strconv.FormatInt(input.PodUnixFinishTime, 10),
	}
}

type TaskExecutionIdentifierTemplateVarsProvider struct {
	core.TaskExecutionIdentifier
}

func (p TaskExecutionIdentifierTemplateVarsProvider) ToTemplateVars() TemplateVars {
	var templateVars TemplateVars
	serialized, _ := json.Marshal(p.TaskExecutionIdentifier)
	_ = json.Unmarshal(serialized, &templateVars)
	return templateVars
}

// A simple log plugin that supports templates in urls to build the final log link. Supported templates are:
// {{ .podName }}: Gets the pod name as it shows in k8s dashboard,
// {{ .podUID }}: Gets the pod UID,
// {{ .namespace }}: K8s namespace where the pod runs,
// {{ .containerName }}: The container name that generated the log,
// {{ .containerId }}: The container id docker/crio generated at run time,
// {{ .logName }}: A deployment specific name where to expect the logs to be.
// {{ .hostname }}: The hostname where the pod is running and where logs reside.
// {{ .podUnixStartTime }}: The pod creation time (in unix seconds, not millis)
// {{ .podUnixFinishTime }}: Don't have a good mechanism for this yet, but approximating with time.Now for now
type TemplateLogPlugin struct {
	templateUris  []string
	messageFormat core.TaskLog_MessageFormat
}

func (s TemplateLogPlugin) GetTaskLog(
	podName, podUID, namespace, containerName, containerID, logName string,
	podUnixStartTime, podUnixFinishTime int64,
) (core.TaskLog, error) {
	o, err := s.GetTaskLogs(Input{
		LogName:           logName,
		Namespace:         namespace,
		PodName:           podName,
		PodUID:            podUID,
		ContainerName:     containerName,
		ContainerID:       containerID,
		PodUnixStartTime:  podUnixStartTime,
		PodUnixFinishTime: podUnixFinishTime,
	})

	if err != nil || len(o.TaskLogs) == 0 {
		return core.TaskLog{}, err
	}

	return *o.TaskLogs[0], nil
}

func (s TemplateLogPlugin) GetTaskLogs(input Input, extraTemplateVars ...TemplateVars) (Output, error) {
	templateVars := input.ToTemplateVars()
	templateVars.Merge(extraTemplateVars...)

	taskLogs := make([]*core.TaskLog, 0, len(s.templateUris))
	for _, templateURI := range s.templateUris {
		var buf bytes.Buffer
		t := template.Must(template.New("uri").Parse(templateURI))
		err := t.Execute(&buf, templateVars)
		if err != nil {
			return Output{}, err
		}

		taskLogs = append(taskLogs, &core.TaskLog{
			Uri:           buf.String(),
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
// {{ .podUnixStartTime }}: The pod creation time (in unix seconds, not millis)
// {{ .podUnixFinishTime }}: Don't have a good mechanism for this yet, but approximating with time.Now for now
func NewTemplateLogPlugin(
	templateUris []string,
	messageFormat core.TaskLog_MessageFormat,
) TemplateLogPlugin {
	return TemplateLogPlugin{
		templateUris:  templateUris,
		messageFormat: messageFormat,
	}
}
