package k8s

import (
	"context"
	"strconv"
	"strings"

	idlCore "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/utils"
	arrayCore "github.com/lyft/flyteplugins/go/tasks/plugins/array/core"
	errors2 "github.com/lyft/flytestdlib/errors"
	"github.com/lyft/flytestdlib/logger"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
)

type Task struct {
	LogLinks *[]idlCore.TaskLog
	State    *arrayCore.State
	Config   *Config
	ChildIdx int
}

type TaskLaunchStatus int8

const (
	Success TaskLaunchStatus = iota
	Error
	Skip
	ReturnState
)

func (t Task) Launch(ctx context.Context, tCtx core.TaskExecutionContext, kubeClient core.KubeClient) (TaskLaunchStatus, error) {
	newState := t.State
	podTemplate, _, err := FlyteArrayJobToK8sPodTemplate(ctx, tCtx)
	if err != nil {
		return Error, errors2.Wrapf(ErrBuildPodTemplate, err, "Failed to convert task template to a pod template for a task")
	}

	var args []string
	if len(podTemplate.Spec.Containers) > 0 {
		args = append(podTemplate.Spec.Containers[0].Command, podTemplate.Spec.Containers[0].Args...)
		podTemplate.Spec.Containers[0].Command = []string{}
	}

	indexStr := strconv.Itoa(t.ChildIdx)
	podName := formatSubTaskName(ctx, tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), indexStr)

	pod := podTemplate.DeepCopy()
	pod.Name = podName
	pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env, corev1.EnvVar{
		Name:  FlyteK8sArrayIndexVarName,
		Value: indexStr,
	})

	pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env, arrayJobEnvVars...)
	pod.Spec.Containers[0].Args, err = utils.ReplaceTemplateCommandArgs(ctx, args, arrayJobInputReader{tCtx.InputReader()}, tCtx.OutputWriter())
	if err != nil {
		return Error, errors2.Wrapf(ErrReplaceCmdTemplate, err, "Failed to replace cmd args")
	}

	pod = ApplyPodPolicies(ctx, t.Config, pod)

	succeeded, err := allocateResource(ctx, tCtx, t.Config, podName, t.ChildIdx, &newState.ArrayStatus)
	if err != nil {
		return Error, err
	}

	if !succeeded {
		return Skip, nil
	}

	err = kubeClient.GetClient().Create(ctx, pod)
	if err != nil && !k8serrors.IsAlreadyExists(err) {
		if k8serrors.IsForbidden(err) {
			if strings.Contains(err.Error(), "exceeded quota") {
				// TODO: Quota errors are retried forever, it would be good to have support for backoff strategy.
				logger.Infof(ctx, "Failed to launch  job, resource quota exceeded. Err: %v", err)
				newState = newState.SetPhase(arrayCore.PhaseWaitingForResources, 0).SetReason("Not enough resources to launch job")
			} else {
				newState = newState.SetPhase(arrayCore.PhaseRetryableFailure, 0).SetReason("Failed to launch job.")
			}

			newState = newState.SetReason(err.Error())
			return ReturnState, nil
		}

		return Error, errors2.Wrapf(ErrSubmitJob, err, "Failed to submit job.")
	}

	return Success, nil
}
