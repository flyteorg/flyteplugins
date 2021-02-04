package tensorflow

import (
	"context"
	"fmt"

	"time"

	"github.com/lyft/flyteplugins/go/tasks/plugins/k8s/kfoperators/common"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"
	flyteerr "github.com/lyft/flyteplugins/go/tasks/errors"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/flytek8s"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"

	pluginsCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/k8s"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/utils"

	//commonOp "github.com/kubeflow/common/pkg/apis/common/v1" // switch to real 'common' once https://github.com/kubeflow/pytorch-operator/issues/263 resolved
	commonOp "github.com/kubeflow/tf-operator/pkg/apis/common/v1"
	tfOp "github.com/kubeflow/tf-operator/pkg/apis/tensorflow/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	common.OverrideDefaultContainerName(taskCtx, podSpec, tfOp.DefaultContainerName)

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
func (tensorflowOperatorResourceHandler) GetTaskPhase(_ context.Context, pluginContext k8s.PluginContext, resource k8s.Resource) (pluginsCore.PhaseInfo, error) {
	app := resource.(*tfOp.TFJob)

	workersCount := app.Spec.TFReplicaSpecs[tfOp.TFReplicaTypeWorker].Replicas
	psReplicasCount := app.Spec.TFReplicaSpecs[tfOp.TFReplicaTypePS].Replicas
	chiefCount := app.Spec.TFReplicaSpecs[tfOp.TFReplicaTypeChief].Replicas

	taskLogs, err := common.GetLogs(common.TensorflowTaskType, app.Name, app.Namespace,
		*workersCount, *psReplicasCount, *chiefCount)
	if err != nil {
		return pluginsCore.PhaseInfoUndefined, err
	}

	currentCondition, err := common.ExtractCurrentCondition(app.Status.Conditions)
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

func init() {
	if err := tfOp.AddToScheme(scheme.Scheme); err != nil {
		panic(err)
	}

	pluginmachinery.PluginRegistry().RegisterK8sPlugin(
		k8s.PluginEntry{
			ID:                  common.TensorflowTaskType,
			RegisteredTaskTypes: []pluginsCore.TaskType{common.TensorflowTaskType},
			ResourceToWatch:     &tfOp.TFJob{},
			Plugin:              tensorflowOperatorResourceHandler{},
			IsDefault:           false,
			DefaultForTaskTypes: []pluginsCore.TaskType{common.TensorflowTaskType},
		})
}
