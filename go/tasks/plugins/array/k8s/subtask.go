package k8s

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/flyteorg/flyteplugins/go/tasks/errors"
	pluginsCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/flytek8s/config"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/tasklog"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/utils"
	podPlugin "github.com/flyteorg/flyteplugins/go/tasks/plugins/k8s/pod"

	stdErrors "github.com/flyteorg/flytestdlib/errors"
	"github.com/flyteorg/flytestdlib/logger"

	v1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
)

const (
	ErrBuildPodTemplate       stdErrors.ErrorCode = "POD_TEMPLATE_FAILED"
	ErrReplaceCmdTemplate     stdErrors.ErrorCode = "CMD_TEMPLATE_FAILED"
	FlyteK8sArrayIndexVarName string              = "FLYTE_K8S_ARRAY_INDEX"
	finalizer                 string              = "flyte/array"
	JobIndexVarName           string              = "BATCH_JOB_ARRAY_INDEX_VAR_NAME"
)

var (
	arrayJobEnvVars = []v1.EnvVar{
		{
			Name:  JobIndexVarName,
			Value: FlyteK8sArrayIndexVarName,
		},
	}
	namespaceRegex = regexp.MustCompile("(?i){{.namespace}}(?i)")
)

func addMetadata(stCtx SubTaskExecutionContext, cfg *Config, pod *v1.Pod) {
	k8sPluginCfg := config.GetK8sPluginConfig()
	taskExecutionMetadata := stCtx.TaskExecutionMetadata()

	// Default to parent namespace
	namespace := taskExecutionMetadata.GetNamespace()
	if cfg.NamespaceTemplate != "" {
		if namespaceRegex.MatchString(cfg.NamespaceTemplate) {
			namespace = namespaceRegex.ReplaceAllString(cfg.NamespaceTemplate, namespace)
		} else {
			namespace = cfg.NamespaceTemplate
		}
	}

	pod.SetNamespace(namespace)
	pod.SetAnnotations(utils.UnionMaps(k8sPluginCfg.DefaultAnnotations, pod.GetAnnotations(), utils.CopyMap(taskExecutionMetadata.GetAnnotations())))
	pod.SetLabels(utils.UnionMaps(pod.GetLabels(), utils.CopyMap(taskExecutionMetadata.GetLabels()), k8sPluginCfg.DefaultLabels))
	pod.SetName(taskExecutionMetadata.GetTaskExecutionID().GetGeneratedName())

	if !cfg.RemoteClusterConfig.Enabled {
		pod.OwnerReferences = []metav1.OwnerReference{taskExecutionMetadata.GetOwnerReference()}
	}

	if k8sPluginCfg.InjectFinalizer {
		f := append(pod.GetFinalizers(), finalizer)
		pod.SetFinalizers(f)
	}

	if len(cfg.DefaultScheduler) > 0 {
		pod.Spec.SchedulerName = cfg.DefaultScheduler
	}

	// TODO - should these be appends? or left as overrides?
	if len(cfg.NodeSelector) != 0 {
		pod.Spec.NodeSelector = cfg.NodeSelector
	}
	if len(cfg.Tolerations) != 0 {
		pod.Spec.Tolerations = cfg.Tolerations
	}
}

/*func abortSubtask() error {
	// TODO
	return nil
}*/

func launchSubtask(ctx context.Context, stCtx SubTaskExecutionContext, config *Config, kubeClient pluginsCore.KubeClient) (pluginsCore.PhaseInfo, error) {
	o, err := podPlugin.DefaultPodPlugin.BuildResource(ctx, stCtx)
	pod := o.(*v1.Pod)
	if err != nil {
		return pluginsCore.PhaseInfoUndefined, err
	}

	addMetadata(stCtx, config, pod)

	// inject maptask specific container environment variables
	if len(pod.Spec.Containers) == 0 {
		return pluginsCore.PhaseInfoUndefined, stdErrors.Wrapf(ErrReplaceCmdTemplate, err, "No containers found in podSpec.")
	}

	containerIndex, err := getTaskContainerIndex(pod)
	if err != nil {
		return pluginsCore.PhaseInfoUndefined, err
	}

	pod.Spec.Containers[containerIndex].Env = append(pod.Spec.Containers[containerIndex].Env, v1.EnvVar{
		Name: FlyteK8sArrayIndexVarName,
		// Use the OriginalIndex which represents the position of the subtask in the original user's map task before
		// compacting indexes caused by catalog-cache-check.
		Value: strconv.Itoa(stCtx.originalIndex),
	})

	pod.Spec.Containers[containerIndex].Env = append(pod.Spec.Containers[containerIndex].Env, arrayJobEnvVars...)

	logger.Infof(ctx, "Creating Object: Type:[%v], Object:[%v/%v]", pod.GetObjectKind().GroupVersionKind(), pod.GetNamespace(), pod.GetName())
	err = kubeClient.GetClient().Create(ctx, pod)
	if err != nil && !k8serrors.IsAlreadyExists(err) {
		if k8serrors.IsForbidden(err) {
			if strings.Contains(err.Error(), "exceeded quota") {
				logger.Warnf(ctx, "Failed to launch job, resource quota exceeded and the operation is not guarded by back-off. err: %v", err)
				return pluginsCore.PhaseInfoWaitingForResourcesInfo(time.Now(), pluginsCore.DefaultPhaseVersion, fmt.Sprintf("Exceeded resourcequota: %s", err.Error()), nil), nil
			}
			return pluginsCore.PhaseInfoRetryableFailure("RuntimeFailure", err.Error(), nil), nil
		} else if k8serrors.IsBadRequest(err) || k8serrors.IsInvalid(err) {
			logger.Errorf(ctx, "Badly formatted resource for plugin [%s], err %s", executorName, err)
			// return pluginsCore.DoTransition(pluginsCore.PhaseInfoFailure("BadTaskFormat", err.Error(), nil)), nil
		} else if k8serrors.IsRequestEntityTooLargeError(err) {
			logger.Errorf(ctx, "Badly formatted resource for plugin [%s], err %s", executorName, err)
			return pluginsCore.PhaseInfoFailure("EntityTooLarge", err.Error(), nil), nil
		}
		reason := k8serrors.ReasonForError(err)
		logger.Errorf(ctx, "Failed to launch job, system error. err: %v", err)
		return pluginsCore.PhaseInfoUndefined, errors.Wrapf(stdErrors.ErrorCode(reason), err, "failed to create resource")
	}

	return pluginsCore.PhaseInfoQueued(time.Now(), pluginsCore.DefaultPhaseVersion, "task submitted to K8s"), nil
}

/*func finalizeSubtask() error {
	// TODO
	return nil
}*/

func getSubtaskPhaseInfo(ctx context.Context, stCtx SubTaskExecutionContext, config *Config, kubeClient pluginsCore.KubeClient, logPlugin tasklog.Plugin) (pluginsCore.PhaseInfo, error) {
	o, err := podPlugin.DefaultPodPlugin.BuildIdentityResource(ctx, stCtx.TaskExecutionMetadata())
	if err != nil {
		logger.Errorf(ctx, "Failed to build the Resource with name: %v. Error: %v", stCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), err)
		return pluginsCore.PhaseInfoFailure("BadTaskDefinition", fmt.Sprintf("Failed to build resource, caused by: %s", err.Error()), nil), nil
	}

	pod := o.(*v1.Pod)
	addMetadata(stCtx, config, pod)

	// Attempt to get resource from informer cache, if not found, retrieve it from API server.
	nsName := k8stypes.NamespacedName{Name: pod.GetName(), Namespace: pod.GetNamespace()}
	if err := kubeClient.GetClient().Get(ctx, nsName, pod); err != nil {
		if isK8sObjectNotExists(err) {
			// This happens sometimes because a node gets removed and K8s deletes the pod. This will result in a
			// Pod does not exist error. This should be retried using the retry policy
			logger.Warnf(ctx, "Failed to find the Resource with name: %v. Error: %v", nsName, err)
			failureReason := fmt.Sprintf("resource not found, name [%s]. reason: %s", nsName.String(), err.Error())
			return pluginsCore.PhaseInfoSystemRetryableFailure("ResourceDeletedExternally", failureReason, nil), nil
		}

		logger.Warnf(ctx, "Failed to retrieve Resource Details with name: %v. Error: %v", nsName, err)
		return pluginsCore.PhaseInfoUndefined, err
	}

	stID, _ := stCtx.TaskExecutionMetadata().GetTaskExecutionID().(SubTaskExecutionID)
	phaseInfo, err := podPlugin.DefaultPodPlugin.GetTaskPhaseWithLogs(ctx, stCtx, pod, logPlugin, stID.GetLogSuffix())
	if err != nil {
		logger.Warnf(ctx, "failed to check status of resource in plugin [%s], with error: %s", executorName, err.Error())
		return pluginsCore.PhaseInfoUndefined, err
	}

	if phaseInfo.Info() != nil {
		// Append sub-job status in Log Name for viz.
		for _, log := range phaseInfo.Info().Logs {
			log.Name += fmt.Sprintf(" (%s)", phaseInfo.Phase().String())
		}
	}

	return phaseInfo, err
}

func getTaskContainerIndex(pod *v1.Pod) (int, error) {
	primaryContainerName, ok := pod.Annotations[podPlugin.PrimaryContainerKey]
	// For tasks with a Container target, we only ever build one container as part of the pod
	if !ok {
		if len(pod.Spec.Containers) == 1 {
			return 0, nil
		}
		// For tasks with a K8sPod task target, they may produce multiple containers but at least one must be the designated primary.
		return -1, stdErrors.Errorf(ErrBuildPodTemplate, "Expected a specified primary container key when building an array job with a K8sPod spec target")

	}

	for idx, container := range pod.Spec.Containers {
		if container.Name == primaryContainerName {
			return idx, nil
		}
	}
	return -1, stdErrors.Errorf(ErrBuildPodTemplate, "Couldn't find any container matching the primary container key when building an array job with a K8sPod spec target")
}

func isK8sObjectNotExists(err error) bool {
	return k8serrors.IsNotFound(err) || k8serrors.IsGone(err) || k8serrors.IsResourceExpired(err)
}
