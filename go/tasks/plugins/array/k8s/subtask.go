package k8s

import (
	"context"
	"fmt"
	"regexp"
	"strconv"

	pluginsCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/flytek8s/config"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/tasklog"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/utils"
	podPlugin "github.com/flyteorg/flyteplugins/go/tasks/plugins/k8s/pod"

	errors2 "github.com/flyteorg/flytestdlib/errors"

	v1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"

	//"github.com/flyteorg/flytestdlib/logger" // TODO hamersaw - remove
)

const (
	ErrBuildPodTemplate       errors2.ErrorCode = "POD_TEMPLATE_FAILED"
	ErrReplaceCmdTemplate     errors2.ErrorCode = "CMD_TEMPLATE_FAILED"
	FlyteK8sArrayIndexVarName string            = "FLYTE_K8S_ARRAY_INDEX"
	finalizer                 string            = "flyte/array"
	JobIndexVarName           string            = "BATCH_JOB_ARRAY_INDEX_VAR_NAME"
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

func abortSubtask() error {
	// TODO
	return nil
}

func launchSubtask(ctx context.Context, stCtx SubTaskExecutionContext, config *Config, kubeClient pluginsCore.KubeClient) error {
	o, err := podPlugin.DefaultPodPlugin.BuildResource(ctx, stCtx)
	pod := o.(*v1.Pod)
	if err != nil {
		return err
	}

	addMetadata(stCtx, config, pod)

	// inject maptask specific container environment variables
	if len(pod.Spec.Containers) == 0 {
		return errors2.Wrapf(ErrReplaceCmdTemplate, err, "No containers found in podSpec.")
	}

	containerIndex, err := getTaskContainerIndex(pod)
	if err != nil {
		return err
	}

	pod.Spec.Containers[containerIndex].Env = append(pod.Spec.Containers[containerIndex].Env, v1.EnvVar{
		Name: FlyteK8sArrayIndexVarName,
		// Use the OriginalIndex which represents the position of the subtask in the original user's map task before
		// compacting indexes caused by catalog-cache-check.
		Value: strconv.Itoa(stCtx.originalIndex),
	})

	pod.Spec.Containers[containerIndex].Env = append(pod.Spec.Containers[containerIndex].Env, arrayJobEnvVars...)

	return kubeClient.GetClient().Create(ctx, pod)
}

func finalizeSubtask() error {
	// TODO
	return nil
}

func getSubtaskPhaseInfo(ctx context.Context, stCtx SubTaskExecutionContext, config *Config, kubeClient pluginsCore.KubeClient, logPlugin tasklog.Plugin) (pluginsCore.PhaseInfo, error) {
	o, err := podPlugin.DefaultPodPlugin.BuildIdentityResource(ctx, stCtx.TaskExecutionMetadata())
	if err != nil {
		return pluginsCore.PhaseInfoUndefined, err
	}

	pod := o.(*v1.Pod)
	addMetadata(stCtx, config, pod)

	// Attempt to get resource from informer cache, if not found, retrieve it from API server.
	nsName := k8stypes.NamespacedName{Name: pod.GetName(), Namespace: pod.GetNamespace()}
	if err := kubeClient.GetClient().Get(ctx, nsName, pod); err != nil {
		if isK8sObjectNotExists(err) {
			// This happens sometimes because a node gets removed and K8s deletes the pod. This will result in a
			// Pod does not exist error. This should be retried using the retry policy
			//logger.Warningf(ctx, "Failed to find the Resource with name: %v. Error: %v", nsName, err)  // TODO - log
			failureReason := fmt.Sprintf("resource not found, name [%s]. reason: %s", nsName.String(), err.Error())
			//return pluginsCore.DoTransition(pluginsCore.PhaseInfoSystemRetryableFailure("ResourceDeletedExternally", failureReason, nil)), nil

			// TODO - validate?
			// return pluginsCore.PhaseInfoUndefined, err?
			return pluginsCore.PhaseInfoSystemRetryableFailure("ResourceDeletedExternally", failureReason, nil), nil
		}

		//logger.Warningf(ctx, "Failed to retrieve Resource Details with name: %v. Error: %v", nsName, err)
		// TODO - validate?
		return pluginsCore.PhaseInfoUndefined, err
	}

	stID, _ := stCtx.TaskExecutionMetadata().GetTaskExecutionID().(SubTaskExecutionID)
	phaseInfo, err := podPlugin.DefaultPodPlugin.GetTaskPhaseWithLogs(ctx, stCtx, pod, logPlugin, stID.GetLogSuffix())
	if err != nil {
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
		return -1, errors2.Errorf(ErrBuildPodTemplate, "Expected a specified primary container key when building an array job with a K8sPod spec target")

	}

	for idx, container := range pod.Spec.Containers {
		if container.Name == primaryContainerName {
			return idx, nil
		}
	}
	return -1, errors2.Errorf(ErrBuildPodTemplate, "Couldn't find any container matching the primary container key when building an array job with a K8sPod spec target")
}

func isK8sObjectNotExists(err error) bool {
	return k8serrors.IsNotFound(err) || k8serrors.IsGone(err) || k8serrors.IsResourceExpired(err)
}
