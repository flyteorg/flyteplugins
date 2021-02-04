package common

import (
	"fmt"
	commonOp "github.com/kubeflow/tf-operator/pkg/apis/common/v1"
	logUtils "github.com/lyft/flyteidl/clients/go/coreutils/logs"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	flyteerr "github.com/lyft/flyteplugins/go/tasks/errors"
	"github.com/lyft/flyteplugins/go/tasks/logs"
	pluginsCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	v1 "k8s.io/api/core/v1"
	"sort"
	"time"
)

const (
	TensorflowTaskType = "tensorflow"
	PytorchTaskType = "pytorch"
)

func ExtractCurrentCondition(jobConditions []commonOp.JobCondition) (commonOp.JobCondition, error) {
	sort.Slice(jobConditions[:], func(i, j int) bool {
		return jobConditions[i].LastTransitionTime.Time.After(jobConditions[j].LastTransitionTime.Time)
	})

	for _, jc := range jobConditions {
		if jc.Status == v1.ConditionTrue {
			return jc, nil
		}
	}

	return commonOp.JobCondition{}, fmt.Errorf("found no current condition. Conditions: %+v", jobConditions)
}

func GetPhaseInfo(currentCondition commonOp.JobCondition, occurredAt time.Time,
	taskPhaseInfo pluginsCore.TaskInfo) (pluginsCore.PhaseInfo, error){
	switch currentCondition.Type {
	case commonOp.JobCreated:
		return pluginsCore.PhaseInfoQueued(occurredAt, pluginsCore.DefaultPhaseVersion, "JobCreated"), nil
	case commonOp.JobRunning:
		return pluginsCore.PhaseInfoRunning(pluginsCore.DefaultPhaseVersion, &taskPhaseInfo), nil
	case commonOp.JobSucceeded:
		return pluginsCore.PhaseInfoSuccess(&taskPhaseInfo), nil
	case commonOp.JobFailed:
		details := fmt.Sprintf("Job failed:\n\t%v - %v", currentCondition.Reason, currentCondition.Message)
		return pluginsCore.PhaseInfoRetryableFailure(flyteerr.DownstreamSystemError, details, &taskPhaseInfo), nil
	case commonOp.JobRestarting:
		details := fmt.Sprintf("Job failed:\n\t%v - %v", currentCondition.Reason, currentCondition.Message)
		return pluginsCore.PhaseInfoRetryableFailure(flyteerr.RuntimeFailure, details, &taskPhaseInfo), nil
	}

	return pluginsCore.PhaseInfoUndefined, nil
}

func GetLogs(taskType string, name string, namespace string,
	workersCount int32, psReplicasCount int32, chiefReplicasCount int32) ([]*core.TaskLog, error) {
	// If kubeClient was available, it would be better to use
	// https://github.com/lyft/flyteplugins/blob/209c52d002b4e6a39be5d175bc1046b7e631c153/go/tasks/logs/logging_utils.go#L12
	makeTaskLog := func(appName, appNamespace, suffix, url string) (core.TaskLog, error) {
		return logUtils.NewKubernetesLogPlugin(url).GetTaskLog(
			appName+"-"+suffix,
			appNamespace,
			"",
			"",
			suffix+" logs (via Kubernetes)")
	}

	var taskLogs []*core.TaskLog

	logConfig := logs.GetLogConfig()
	if logConfig.IsKubernetesEnabled {

		if taskType == PytorchTaskType {
			masterTaskLog, masterErr := makeTaskLog(name, namespace, "master-0", logConfig.KubernetesURL)
			if masterErr != nil {
				return nil, masterErr
			}
			taskLogs = append(taskLogs, &masterTaskLog)
		}

		// get all workers log
		for workerIndex := int32(0); workerIndex < workersCount; workerIndex++ {
			workerLog, err := makeTaskLog(name, namespace, fmt.Sprintf("worker-%d", workerIndex), logConfig.KubernetesURL)
			if err != nil {
				return nil, err
			}
			taskLogs = append(taskLogs, &workerLog)
		}
		// get all parameter servers logs
		for psReplicaIndex := int32(0); psReplicaIndex < psReplicasCount; psReplicaIndex++ {
			psReplicaLog, err := makeTaskLog(name, namespace, fmt.Sprintf("psReplica-%d", psReplicaIndex), logConfig.KubernetesURL)
			if err != nil {
				return nil, err
			}
			taskLogs = append(taskLogs, &psReplicaLog)
		}
		if chiefReplicasCount != 0 {
			// get chief worker log, and the max number of chief worker is 1
			chiefReplicaLog, err := makeTaskLog(name, namespace, fmt.Sprintf("chiefReplica-%d", 0), logConfig.KubernetesURL)
			if err != nil {
				return nil, err
			}
			taskLogs = append(taskLogs, &chiefReplicaLog)
		}
	}
	return taskLogs, nil
}

func OverrideDefaultContainerName(taskCtx pluginsCore.TaskExecutionContext, podSpec *v1.PodSpec,
	defaultContainerName string) {
	// Pytorch operator forces pod to have container named 'pytorch'
	// https://github.com/kubeflow/pytorch-operator/blob/037cd1b18eb77f657f2a4bc8a8334f2a06324b57/pkg/apis/pytorch/validation/validation.go#L54-L62
	// Tensorflow operator forces pod to have container named 'tensorflow'
	// https://github.com/kubeflow/tf-operator/blob/984adc287e6fe82841e4ca282dc9a2cbb71e2d4a/pkg/apis/tensorflow/validation/validation.go#L55-L63
	// hence we have to override the name set here
	// https://github.com/lyft/flyteplugins/blob/209c52d002b4e6a39be5d175bc1046b7e631c153/go/tasks/pluginmachinery/flytek8s/container_helper.go#L116
	flyteDefaultContainerName := taskCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()
	for idx, c := range podSpec.Containers {
		if c.Name == flyteDefaultContainerName {
			podSpec.Containers[idx].Name = defaultContainerName
			return
		}
	}
}

