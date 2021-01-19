package logs

import (
	"context"
	"fmt"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/tasklog"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/logger"
	v1 "k8s.io/api/core/v1"
)

// Internal
func GetLogsForContainerInPod(ctx context.Context, pod *v1.Pod, index uint32, nameSuffix string) ([]*core.TaskLog, error) {
	logPlugin, err := InitializeLogPlugins(GetLogConfig())
	if err != nil {
		return nil, err
	}

	if logPlugin == nil {
		return nil, nil
	}

	if pod == nil {
		logger.Error(ctx, "cannot extract logs for a nil container")
		return nil, nil
	}

	if uint32(len(pod.Spec.Containers)) <= index {
		logger.Errorf(ctx, "container IndexOutOfBound, requested [%d], but total containers [%d] in pod phase [%v]", index, len(pod.Spec.Containers), pod.Status.Phase)
		return nil, nil
	}

	if uint32(len(pod.Status.ContainerStatuses)) <= index {
		logger.Errorf(ctx, "containerStatus IndexOutOfBound, requested [%d], but total containerStatuses [%d] in pod phase [%v]", index, len(pod.Status.ContainerStatuses), pod.Status.Phase)
		return nil, nil
	}

	logs, err := logPlugin.GetTaskLogs(
		tasklog.Input{
			HostName:      pod.Name,
			PodName:       pod.Namespace,
			Namespace:     pod.Spec.Containers[index].Name,
			ContainerName: pod.Status.ContainerStatuses[index].ContainerID,
			LogName:       nameSuffix,
		},
	)

	if err != nil {
		return nil, err
	}

	return logs.TaskLogs, nil
}

type taskLogPluginWrapper struct {
	logPlugins map[string]tasklog.Plugin
}

func (t taskLogPluginWrapper) GetTaskLogs(input tasklog.Input) (logOutput tasklog.Output, err error) {
	logs := make([]*core.TaskLog, 0, len(t.logPlugins))
	suffix := input.LogName
	for name, plugin := range t.logPlugins {
		input.LogName = name + suffix
		o, err := plugin.GetTaskLogs(input)
		if err != nil {
			return tasklog.Output{}, err
		}

		logs = append(logs, o.TaskLogs...)
	}

	return tasklog.Output{
		TaskLogs: logs,
	}, nil
}

// Internal
func InitializeLogPlugins(cfg *LogConfig) (tasklog.Plugin, error) {
	logPlugins := map[string]tasklog.Plugin{}

	if cfg.IsKubernetesEnabled {
		if len(cfg.KubernetesTemplateURI) > 0 {
			logPlugins["Kubernetes Logs"] = tasklog.NewTemplateLogPlugin([]string{cfg.KubernetesTemplateURI}, core.TaskLog_JSON)
		} else {
			logPlugins["Kubernetes Logs"] = tasklog.NewTemplateLogPlugin([]string{fmt.Sprintf("%s/#!/log/{{ .namespace }}/{{ .podName }}/pod?namespace={{ .namespace }}", cfg.KubernetesURL)}, core.TaskLog_JSON)
		}
	}

	if cfg.IsCloudwatchEnabled {
		if len(cfg.CloudwatchTemplateURI) > 0 {
			logPlugins["Cloudwatch Logs"] = tasklog.NewTemplateLogPlugin([]string{cfg.CloudwatchTemplateURI}, core.TaskLog_JSON)
		} else {
			logPlugins["Cloudwatch Logs"] = tasklog.NewTemplateLogPlugin(
				[]string{fmt.Sprintf("https://console.aws.amazon.com/cloudwatch/home?region=%s#logEventViewer:group=%s;stream=var.log.containers.{{ .podName }}_{{ .namespace }}_{{ .containerName }}-{{ .containerId }}.log", cfg.CloudwatchRegion, cfg.CloudwatchLogGroup)}, core.TaskLog_JSON)
		}
	}

	if cfg.IsStackDriverEnabled {
		if len(cfg.CloudwatchTemplateURI) > 0 {
			logPlugins["Stackdriver Logs"] = tasklog.NewTemplateLogPlugin([]string{cfg.StackDriverTemplateURI}, core.TaskLog_JSON)
		} else {
			logPlugins["Stackdriver Logs"] = tasklog.NewTemplateLogPlugin(
				[]string{fmt.Sprintf("https://console.cloud.google.com/logs/viewer?project=%s&angularJsUrl=%%2Flogs%%2Fviewer%%3Fproject%%3D%s&resource=%s&advancedFilter=resource.labels.pod_name%%3D{{ .podName }}", cfg.GCPProjectName, cfg.GCPProjectName, cfg.StackdriverLogResourceName)}, core.TaskLog_JSON)
		}
	}

	if len(cfg.Templates) > 0 {
		for _, cfg := range cfg.Templates {
			logPlugins[cfg.DisplayName] = tasklog.NewTemplateLogPlugin(cfg.TemplateURIs, cfg.MessageFormat)
		}
	}

	if len(logPlugins) == 0 {
		return nil, nil
	}

	return taskLogPluginWrapper{
		logPlugins: logPlugins,
	}, nil
}
