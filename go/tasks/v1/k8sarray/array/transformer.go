/*
 * Copyright (c) 2018 Lyft. All rights reserved.
 */

package array

import (
	"context"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"
	"github.com/lyft/flyteplugins/go/tasks/v1/flytek8s"
	"github.com/lyft/flyteplugins/go/tasks/v1/types"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/lyft/flytedynamicjoboperator/errors"
	"github.com/lyft/flytedynamicjoboperator/pkg/apis/futures/v1alpha1"
	"github.com/lyft/flytedynamicjoboperator/pkg/internal"
)

// Constructs a k8s pod to execute the generator task off Dynamic Job. Note that Name is not set on the result object.
// It's up to the caller to set the Name before creating the object in K8s.
func FlyteArrayJobToK8sPod(ctx context.Context, taskCtx types.TaskContext, taskTemplate *core.TaskTemplate, inputs *core.LiteralMap) (
	podTemplate v1.Pod, job *plugins.ArrayJob, err error) {

	if taskTemplate.GetContainer() == nil {
		return v1.Pod{}, nil, errors.NewRequiredValueNotSet("container")
	}

	var arrayJob *plugins.ArrayJob
	if taskTemplate.GetCustom() != nil {
		arrayJob = &plugins.ArrayJob{}
		err = internal.UnmarshalStruct(taskTemplate.GetCustom(), arrayJob)
		if err != nil {
			return v1.Pod{}, nil, errors.NewInvalidFormat("custom")
		}
	}

	podSpec, err := flytek8s.ToK8sPodSpec(ctx, taskCtx, taskTemplate.GetContainer(), inputs)
	if err != nil {
		return corev1.Pod{}, nil, err
	}

	// TODO: This is a hack
	podSpec.Containers[0].Command = taskTemplate.GetContainer().Command
	podSpec.Containers[0].Args = taskTemplate.GetContainer().Args

	return corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       v1alpha1.KindK8sPod,
			APIVersion: corev1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       taskCtx.GetNamespace(),
			Labels:          taskCtx.GetLabels(),
			Annotations:     taskCtx.GetAnnotations(),
			OwnerReferences: []metav1.OwnerReference{taskCtx.GetOwnerReference()},
		},
		Spec: *podSpec,
	}, arrayJob, nil
}
