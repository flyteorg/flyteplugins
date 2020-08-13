package tensorflow

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"
	flyteerr "github.com/lyft/flyteplugins/go/tasks/errors"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/flytek8s"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"

	pluginsCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/k8s"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/utils"

	logUtils "github.com/lyft/flyteidl/clients/go/coreutils/logs"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteplugins/go/tasks/logs"

	//commonOp "github.com/kubeflow/common/pkg/apis/common/v1" // switch to real 'common' once https://github.com/kubeflow/pytorch-operator/issues/263 resolved
	commonOp "github.com/kubeflow/tf-operator/pkg/apis/common/v1"
	tfOp "github.com/kubeflow/tf-operator/pkg/apis/tensorflow/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	tensorflowTaskType = "tensorflow"
)

type tensorflowOperatorResourceHandler struct {
}

// Sanity test that the plugin implements method of k8s.Plugin
var _ k8s.Plugin = tensorflowOperatorResourceHandler{}

// Defines a func to create a query object (typically just object and type meta portions) that's used to query k8s
// resources.
func (tensorflowOperatorResourceHandler) BuildIdentityResource(ctx context.Context, taskCtx pluginsCore.TaskExecutionMetadata) (k8s.Resource, error) {
	return &tfOp.TFJob{
		TypeMeta: metav1.TypeMeta{
			Kind:       tfOp.Kind,
			APIVersion: tfOp.SchemeGroupVersion.String(),
		},
	}, nil
}

// Defines a func to create the full resource object that will be posted to k8s.
func (tensorflowOperatorResourceHandler) BuildResource(ctx context.Context, taskCtx pluginsCore.TaskExecutionContext) (k8s.Resource, error) {
	taskTemplate, err := taskCtx.TaskReader().Read(ctx)

	if err != nil {
		return nil, flyteerr.Errorf(flyteerr.BadTaskSpecification, "unable to fetch task specification [%v]", err.Error())
	} else if taskTemplate == nil {
		return nil, flyteerr.Errorf(flyteerr.BadTaskSpecification, "nil task specification")
	}

	tensorflowTaskExtraArgs := plugins.DistributedTensorflowTrainingTask{}
	err = utils.UnmarshalStruct(taskTemplate.GetCustom(), &tensorflowTaskExtraArgs)
	if err != nil {
		return nil, flyteerr.Errorf(flyteerr.BadTaskSpecification, "invalid TaskSpecification [%v], Err: [%v]", taskTemplate.GetCustom(), err.Error())
	}

	podSpec, err := flytek8s.ToK8sPodSpec(ctx, taskCtx.TaskExecutionMetadata(), taskCtx.TaskReader(), taskCtx.InputReader(), taskCtx.OutputWriter())
	if err != nil {
		return nil, flyteerr.Errorf(flyteerr.BadTaskSpecification, "Unable to create pod spec: [%v]", err.Error())
	}

	overrideDefaultContainerName(taskCtx, podSpec)

	workers := tensorflowTaskExtraArgs.GetWorkers()
	psReplicas := tensorflowTaskExtraArgs.GetPsReplicas()
	chiefReplicas := tensorflowTaskExtraArgs.GetChiefReplicas()

	jobSpec := tfOp.TFJobSpec{
		TTLSecondsAfterFinished: nil,
		TFReplicaSpecs: map[tfOp.TFReplicaType]*commonOp.ReplicaSpec{
			tfOp.TFReplicaTypePS: {
				Replicas: &psReplicas,
				Template: v1.PodTemplateSpec{
					Spec: *podSpec,
				},
				RestartPolicy: commonOp.RestartPolicyNever,
			},
			tfOp.TFReplicaTypeChief: {
				Replicas: &chiefReplicas,
				Template: v1.PodTemplateSpec{
					Spec: *podSpec,
				},
				RestartPolicy: commonOp.RestartPolicyNever,
			},
			tfOp.TFReplicaTypeWorker: {
				Replicas: &workers,
				Template: v1.PodTemplateSpec{
					Spec: *podSpec,
				},
				RestartPolicy: commonOp.RestartPolicyNever,
			},
		},
	}

	job := &tfOp.TFJob{
		TypeMeta: metav1.TypeMeta{
			Kind:       tfOp.Kind,
			APIVersion: tfOp.SchemeGroupVersion.String(),
		},
		Spec: jobSpec,
	}

	return job, nil
}

// Analyses the k8s resource and reports the status as TaskPhase. This call is expected to be relatively fast,
// any operations that might take a long time (limits are configured system-wide) should be offloaded to the
// background.
func (tensorflowOperatorResourceHandler) GetTaskPhase(ctx context.Context, pluginContext k8s.PluginContext, resource k8s.Resource) (pluginsCore.PhaseInfo, error) {
	app := resource.(*tfOp.TFJob)

	workersCount := app.Spec.TFReplicaSpecs[tfOp.TFReplicaTypeWorker].Replicas
	psReplicasCount := app.Spec.TFReplicaSpecs[tfOp.TFReplicaTypePS].Replicas
	chiefCount := app.Spec.TFReplicaSpecs[tfOp.TFReplicaTypeChief].Replicas

	taskLogs, err := getLogs(app, *workersCount, *psReplicasCount, *chiefCount)
	if err != nil {
		return pluginsCore.PhaseInfoUndefined, err
	}

	currentCondition, err := extractCurrentCondition(app.Status.Conditions)
	if err != nil {
		return pluginsCore.PhaseInfoUndefined, err
	}

	occurredAt := time.Now()
	statusDetails, _ := utils.MarshalObjToStruct(app.Status)
	taskPhaseInfo := pluginsCore.TaskInfo{
		Logs:       taskLogs,
		OccurredAt: &occurredAt,
		CustomInfo: statusDetails,
	}

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

func getLogs(app *tfOp.TFJob, workersCount int32, psReplicasCount int32, chiefReplicasCount int32) ([]*core.TaskLog, error) {
	// If kubeClient was available, it would be better to use
	// https://github.com/lyft/flyteplugins/blob/209c52d002b4e6a39be5d175bc1046b7e631c153/go/tasks/logs/logging_utils.go#L12
	makeTaskLog := func(appName, appNamespace, suffix, url string) (core.TaskLog, error) {
		return logUtils.NewKubernetesLogPlugin(url).GetTaskLog(
			appName+"-"+suffix,
			appNamespace,
			"",
			"",
			suffix+" logs (via Kubernetes)"), nil
	}

	var taskLogs []*core.TaskLog

	logConfig := logs.GetLogConfig()
	if logConfig.IsKubernetesEnabled {

		// get all workers log
		for workerIndex := int32(0); workerIndex < workersCount; workerIndex++ {
			workerLog, err := makeTaskLog(app.Name, app.Namespace, fmt.Sprintf("worker-%d", workerIndex), logConfig.KubernetesURL)
			if err != nil {
				return nil, err
			}
			taskLogs = append(taskLogs, &workerLog)
		}
		// get all parameter servers logs
		for psReplicaIndex := int32(0); psReplicaIndex < psReplicasCount; psReplicaIndex++ {
			psReplicaLog, err := makeTaskLog(app.Name, app.Namespace, fmt.Sprintf("psReplica-%d", psReplicaIndex), logConfig.KubernetesURL)
			if err != nil {
				return nil, err
			}
			taskLogs = append(taskLogs, &psReplicaLog)
		}
		if chiefReplicasCount != 0 {
			// get chief worker log, and the max number of chief worker is 1
			chiefReplicaLog, err := makeTaskLog(app.Name, app.Namespace, fmt.Sprintf("chiefReplica-%d", 0), logConfig.KubernetesURL)
			if err != nil {
				return nil, err
			}
			taskLogs = append(taskLogs, &chiefReplicaLog)
		}
	}
	return taskLogs, nil
}

func extractCurrentCondition(jobConditions []commonOp.JobCondition) (commonOp.JobCondition, error) {
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

func overrideDefaultContainerName(taskCtx pluginsCore.TaskExecutionContext, podSpec *v1.PodSpec) {
	// Pytorch operator forces pod to have container named 'pytorch'
	// https://github.com/kubeflow/pytorch-operator/blob/037cd1b18eb77f657f2a4bc8a8334f2a06324b57/pkg/apis/pytorch/validation/validation.go#L54-L62
	// hence we have to override the name set here
	// https://github.com/lyft/flyteplugins/blob/209c52d002b4e6a39be5d175bc1046b7e631c153/go/tasks/pluginmachinery/flytek8s/container_helper.go#L116
	flyteDefaultContainerName := taskCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()
	for idx, c := range podSpec.Containers {
		if c.Name == flyteDefaultContainerName {
			podSpec.Containers[idx].Name = tfOp.DefaultContainerName
			return
		}
	}
}

func init() {
	if err := tfOp.AddToScheme(scheme.Scheme); err != nil {
		panic(err)
	}

	pluginmachinery.PluginRegistry().RegisterK8sPlugin(
		k8s.PluginEntry{
			ID:                  tensorflowTaskType,
			RegisteredTaskTypes: []pluginsCore.TaskType{tensorflowTaskType},
			ResourceToWatch:     &tfOp.TFJob{},
			Plugin:              tensorflowOperatorResourceHandler{},
			IsDefault:           false,
		})
}
