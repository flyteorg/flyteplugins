package k8s

import (
	"context"
	"fmt"
	"strconv"

	"github.com/lyft/flyteplugins/go/tasks/v1/array/bitarray"

	k8sarray "github.com/lyft/flyteplugins/go/tasks/v1/array"
	"github.com/lyft/flyteplugins/go/tasks/v1/array/arraystatus"

	errors2 "github.com/lyft/flytestdlib/errors"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/utils"
	"github.com/lyft/flytestdlib/logger"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/core"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
)

const (
	ErrBuildPodTemplate       errors2.ErrorCode = "POD_TEMPLATE_FAILED"
	ErrReplaceCmdTemplate     errors2.ErrorCode = "CMD_TEMPLATE_FAILED"
	ErrSubmitJob              errors2.ErrorCode = "SUBMIT_JOB_FAILED"
	JobIndexVarName           string            = "BATCH_JOB_ARRAY_INDEX_VAR_NAME"
	FlyteK8sArrayIndexVarName string            = "FLYTE_K8S_ARRAY_INDEX"
)

var arrayJobEnvVars = []corev1.EnvVar{
	{
		Name:  JobIndexVarName,
		Value: FlyteK8sArrayIndexVarName,
	},
}

func formatSubTaskName(_ context.Context, parentName, suffix string) (subTaskName string) {
	return fmt.Sprintf("%v-%v", parentName, suffix)
}

func newStatusCompactArray(count uint) bitarray.CompactArray {
	a, err := bitarray.NewCompactArray(count, bitarray.Item(len(core.Phases)-1))
	if err != nil {
		return bitarray.CompactArray{}
	}

	return a
}

func ApplyPodPolicies(_ context.Context, cfg *Config, pod *corev1.Pod) *corev1.Pod {
	if len(cfg.DefaultScheduler) > 0 {
		pod.Spec.SchedulerName = cfg.DefaultScheduler
	}

	return pod
}

// Launches subtasks
func LaunchSubTasks(ctx context.Context, tCtx core.TaskExecutionContext, kubeClient core.KubeClient,
	config *Config, currentState k8sarray.State) (newState k8sarray.State, err error) {
	podTemplate, _, err := k8sarray.FlyteArrayJobToK8sPodTemplate(ctx, tCtx)
	if err != nil {
		return currentState, errors2.Wrapf(ErrBuildPodTemplate, err, "Failed to convert task template to a pod template for task")
	}

	var command []string
	if len(podTemplate.Spec.Containers) > 0 {
		command = append(podTemplate.Spec.Containers[0].Command, podTemplate.Spec.Containers[0].Args...)
		podTemplate.Spec.Containers[0].Args = []string{}
	}

	args := utils.CommandLineTemplateArgs{
		Input:        tCtx.InputReader().GetInputPrefixPath().String(),
		OutputPrefix: tCtx.OutputWriter().GetOutputPrefixPath().String(),
	}

	size := currentState.GetActualArraySize()
	// TODO: Respect parallelism param
	for i := 0; i < size; i++ {
		pod := podTemplate.DeepCopy()
		indexStr := strconv.Itoa(i)
		pod.Name = formatSubTaskName(ctx, tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), indexStr)
		pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env, corev1.EnvVar{
			Name:  FlyteK8sArrayIndexVarName,
			Value: indexStr,
		})

		pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env, arrayJobEnvVars...)

		pod.Spec.Containers[0].Command, err = utils.ReplaceTemplateCommandArgs(ctx, command, args)
		if err != nil {
			return currentState, errors2.Wrapf(ErrReplaceCmdTemplate, err, "Failed to replace cmd args")
		}

		pod = ApplyPodPolicies(ctx, config, pod)

		err = kubeClient.GetClient().Create(ctx, pod)
		if err != nil && !k8serrors.IsAlreadyExists(err) {
			return currentState, errors2.Wrapf(ErrSubmitJob, err, "Failed to submit job")
		}
	}

	logger.Infof(ctx, "Successfully submitted Job(s) with Prefix:[%v], Count:[%v]", tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), size)

	arrayStatus := arraystatus.ArrayStatus{
		Summary:  arraystatus.ArraySummary{},
		Detailed: newStatusCompactArray(uint(size)),
	}

	currentState.SetPhase(k8sarray.PhaseJobSubmitted)
	currentState.SetArrayStatus(arrayStatus)

	return currentState, nil
}
