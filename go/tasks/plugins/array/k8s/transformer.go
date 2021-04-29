package k8s

import (
	"context"
	"regexp"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/io"

	"github.com/flyteorg/flyteplugins/go/tasks/plugins/array"

	idlPlugins "github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/plugins"
	"github.com/flyteorg/flyteplugins/go/tasks/errors"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/flytek8s"
	core2 "github.com/flyteorg/flyteplugins/go/tasks/plugins/array/core"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "k8s.io/api/core/v1"
)

const PodKind = "pod"

var namespaceRegex = regexp.MustCompile("(?i){{.namespace}}(?i)")

type arrayTaskContext struct {
	core.TaskExecutionContext
	arrayInputReader io.InputReader
}

// Overrides the TaskExecutionContext from base and returns a specialized context for Array
func (a *arrayTaskContext) InputReader() io.InputReader {
	return a.arrayInputReader
}

func GetNamespaceForExecution(tCtx core.TaskExecutionContext) string {

	// Default to parent namespace
	namespace := tCtx.TaskExecutionMetadata().GetNamespace()

	namespacePattern := GetConfig().Namespace
	if namespacePattern != "" {
		if namespaceRegex.MatchString(namespacePattern) {
			namespace = namespaceRegex.ReplaceAllString(namespacePattern, namespace)
		} else {
			namespace = namespacePattern
		}
	}
	return namespace
}

// Note that Name is not set on the result object.
// It's up to the caller to set the Name before creating the object in K8s.
func FlyteArrayJobToK8sPodTemplate(ctx context.Context, tCtx core.TaskExecutionContext) (
	podTemplate v1.Pod, job *idlPlugins.ArrayJob, err error) {

	// Check that the taskTemplate is valid
	taskTemplate, err := tCtx.TaskReader().Read(ctx)
	if err != nil {
		return v1.Pod{}, nil, err
	} else if taskTemplate == nil {
		return v1.Pod{}, nil, errors.Errorf(errors.BadTaskSpecification, "Required value not set, taskTemplate is nil")
	}

	if taskTemplate.GetContainer() == nil {
		return v1.Pod{}, nil, errors.Errorf(errors.BadTaskSpecification,
			"Required value not set, taskTemplate Container")
	}

	arrTCtx := &arrayTaskContext{
		TaskExecutionContext: tCtx,
		arrayInputReader:     array.GetInputReader(tCtx, taskTemplate),
	}
	var arrayJob *idlPlugins.ArrayJob
	if taskTemplate.GetCustom() != nil {
		arrayJob, err = core2.ToArrayJob(taskTemplate.GetCustom(), taskTemplate.TaskTypeVersion)
		if err != nil {
			return v1.Pod{}, nil, err
		}
	}

	podSpec, err := flytek8s.ToK8sPodSpec(ctx, arrTCtx)
	if err != nil {
		return v1.Pod{}, nil, err
	}

	return v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       PodKind,
			APIVersion: v1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			// Note that name is missing here
			Namespace:       GetNamespaceForExecution(tCtx),
			Labels:          tCtx.TaskExecutionMetadata().GetLabels(),
			Annotations:     tCtx.TaskExecutionMetadata().GetAnnotations(),
			OwnerReferences: []metav1.OwnerReference{tCtx.TaskExecutionMetadata().GetOwnerReference()},
		},
		Spec: *podSpec,
	}, arrayJob, nil
}
