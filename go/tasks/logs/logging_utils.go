package logs

import (
	"context"
	"fmt"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/tasklog"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/logger"
	v1 "k8s.io/api/core/v1"
)

func GetLogsForContainerInPod(ctx context.Context, pod *v1.Pod, index uint32, nameSuffix string) ([]*core.TaskLog, error) {
	logConfig := GetLogConfig()
	logPlugins := map[string]tasklog.Plugin{}

	if logConfig.IsKubernetesEnabled {
		if len(logConfig.KubernetesTemplateUri) > 0 {
			logPlugins["Kubernetes Logs"] = tasklog.NewTemplateLogPlugin([]string{logConfig.KubernetesTemplateUri}, core.TaskLog_JSON)
		} else {
			logPlugins["Kubernetes Logs"] = tasklog.NewTemplateLogPlugin([]string{fmt.Sprintf("%s/#!/log/{{ .namespace }}/{{ .podName }}/pod?namespace={{ .namespace }}", logConfig.KubernetesURL)}, core.TaskLog_JSON)
		}
	}

	if logConfig.IsCloudwatchEnabled {
		if len(logConfig.CloudwatchTemplateUri) > 0 {
			logPlugins["Cloudwatch Logs"] = tasklog.NewTemplateLogPlugin([]string{logConfig.CloudwatchTemplateUri}, core.TaskLog_JSON)
		} else {
			logPlugins["Cloudwatch Logs"] = tasklog.NewTemplateLogPlugin(
				[]string{fmt.Sprintf("https://console.aws.amazon.com/cloudwatch/home?region=%s#logEventViewer:group=%s;stream=var.log.containers.{{ .podName }}_{{ .namespace }}_{{ .containerName }}-{{ .containerId }}.log", logConfig.CloudwatchRegion, logConfig.CloudwatchLogGroup)}, core.TaskLog_JSON)
		}
	}

	if logConfig.IsStackDriverEnabled {
		if len(logConfig.CloudwatchTemplateUri) > 0 {
			logPlugins["Stackdriver Logs"] = tasklog.NewTemplateLogPlugin([]string{logConfig.StackDriverTemplateUri}, core.TaskLog_JSON)
		} else {
			logPlugins["Stackdriver Logs"] = tasklog.NewTemplateLogPlugin(
				[]string{fmt.Sprintf("https://console.cloud.google.com/logs/viewer?project=%s&angularJsUrl=%%2Flogs%%2Fviewer%%3Fproject%%3D%s&resource=%s&advancedFilter=resource.labels.pod_name%%3D{{ .podName }}", logConfig.GCPProjectName, logConfig.GCPProjectName, logConfig.StackdriverLogResourceName)}, core.TaskLog_JSON)
		}
	}

	if len(logConfig.Templates) > 0 {
		for _, cfg := range logConfig.Templates {
			logPlugins[cfg.DisplayName] = tasklog.NewTemplateLogPlugin(cfg.TemplateURIs, cfg.MessageFormat)
		}
	}

	if len(logPlugins) == 0 {
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

	var logs []*core.TaskLog
	for name, plugin := range logPlugins {
		taskLogs, err := plugin.GetTaskLogs(
			tasklog.Input{
				HostName:      pod.Name,
				PodName:       pod.Namespace,
				Namespace:     pod.Spec.Containers[index].Name,
				ContainerName: pod.Status.ContainerStatuses[index].ContainerID,
				ContainerID:   name + nameSuffix,
			},
		)

		if err != nil {
			return nil, err
		}

		logs = append(logs, taskLogs.TaskLogs...)
	}

	return logs, nil
}
