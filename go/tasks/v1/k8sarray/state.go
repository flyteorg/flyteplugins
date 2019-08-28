package k8sarray

import (
	"context"
	idlCore "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	idlPlugins "github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/utils"
	"github.com/lyft/flyteplugins/go/tasks/v1/errors"
	"github.com/lyft/flyteplugins/go/tasks/v1/flytek8s"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	)

const K8sPodKind = "pod"

type State struct {
	currentPhase Phase
}

type Phase uint8

const (
	NotStarted Phase = iota
	MappingFileCreated
	JobSubmitted
	JobsFinished
)

/*
  Discovery for sub-tasks
  Build mapping file
---
  submit jobs (either as a batch or individually)
---BestEffort
  Detect changes to individual job states
    - Check failure ratios
---
  Submit to discovery

*/


// Constructs a k8s pod to execute the generator task off Dynamic Job. Note that Name is not set on the result object.
// It's up to the caller to set the Name before creating the object in K8s.
func FlyteArrayJobToK8sPod(ctx context.Context, tCtx core.TaskExecutionContext, taskTemplate *idlCore.TaskTemplate, inputs *idlCore.LiteralMap) (
	podTemplate v1.Pod, job *plugins.ArrayJob, err error) {

	if taskTemplate.GetContainer() == nil {
		return v1.Pod{}, nil, errors.Errorf(errors.BadTaskSpecification,
			"Required value not set, taskTemplate Container")
	}

	var arrayJob *idlPlugins.ArrayJob
	if taskTemplate.GetCustom() != nil {
		arrayJob = &idlPlugins.ArrayJob{}
		err = utils.UnmarshalStruct(taskTemplate.GetCustom(), arrayJob)
		if err != nil {
			return v1.Pod{}, nil, errors.Wrapf(errors.BadTaskSpecification, err,
				"Could not unmarshal taskTemplate custom into ArrayJob plugin pb")
		}
	}

	/*
	func ToK8sPod(ctx context.Context, taskCtx pluginsCore.TaskExecutionMetadata, taskReader pluginsCore.TaskReader, inputs io.InputReader,
		outputPrefixPath string) (*v1.PodSpec, error) {
	*/
	podSpec, err := flytek8s.ToK8sPod(ctx, taskCtx, taskTemplate.GetContainer(), inputs)
	if err != nil {
		return corev1.Pod{}, nil, err
	}

	// TODO: This is a hack
	podSpec.Containers[0].Command = taskTemplate.GetContainer().Command
	podSpec.Containers[0].Args = taskTemplate.GetContainer().Args

	return corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       K8sPodKind,
			APIVersion: corev1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       tCtx.TaskExecutionMetadata().GetNamespace(),
			Labels:          tCtx.TaskExecutionMetadata().GetLabels(),
			Annotations:     tCtx.TaskExecutionMetadata().GetAnnotations(),
			OwnerReferences: []metav1.OwnerReference{tCtx.TaskExecutionMetadata().GetOwnerReference()},
		},
		Spec: *podSpec,
	}, arrayJob, nil
}

