package tasklog

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"text/template"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/imdario/mergo"
)

func (t TemplateVars) Merge(others ...TemplateVars) error {
	for _, o := range others {
		err := mergo.Merge(&t, o)
		if err != nil {
			return err
		}
	}
	return nil
}

func (input Input) ToTemplateVars() (TemplateVars, error) {
	// stream. Therefore, we must also strip the prefix.
	containerID := input.ContainerID
	stripDelimiter := "://"
	if split := strings.Split(input.ContainerID, stripDelimiter); len(split) > 1 {
		containerID = split[1]
	}

	var templateVars TemplateVars
	var err error

	serialized, err := json.Marshal(&struct {
		HostName                string                        `json:"hostname,omitempty"`
		PodName                 string                        `json:"podName,omitempty"`
		Namespace               string                        `json:"namespace,omitempty"`
		ContainerName           string                        `json:"containerName,omitempty"`
		ContainerID             string                        `json:"containerId,omitempty"`
		LogName                 string                        `json:"logName,omitempty"`
		PodRFC3339StartTime     string                        `json:"podRFC3339StartTime,omitempty"`
		PodRFC3339FinishTime    string                        `json:"podRFC3339FinishTime,omitempty"`
		PodUnixStartTime        string                        `json:"podUnixStartTime,omitempty"`
		PodUnixFinishTime       string                        `json:"podUnixFinishTime,omitempty"`
		PodUID                  string                        `json:"podUID,omitempty"`
		TaskExecutionIdentifier *core.TaskExecutionIdentifier `json:"taskExecution,omitempty"`
	}{
		input.HostName,
		input.PodName,
		input.Namespace,
		input.ContainerName,
		containerID,
		input.LogName,
		input.PodRFC3339StartTime,
		input.PodRFC3339FinishTime,
		strconv.FormatInt(input.PodUnixStartTime, 10),
		strconv.FormatInt(input.PodUnixFinishTime, 10),
		input.PodUID,
		input.TaskExecutionIdentifier,
	})
	if err != nil {
		return templateVars, err
	}

	err = json.Unmarshal(serialized, &templateVars)
	return templateVars, err
}

// A simple log plugin that supports templates in urls to build the final log link. Supported templates are:
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

func (s TemplateLogPlugin) GetTaskLogs(input Input, extraTemplateVars ...TemplateVars) (Output, error) {
	var err error
	templateVars, err := input.ToTemplateVars()
	if err != nil {
		return Output{}, err
	}
	err = templateVars.Merge(extraTemplateVars...)
	if err != nil {
		return Output{}, err
	}

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
