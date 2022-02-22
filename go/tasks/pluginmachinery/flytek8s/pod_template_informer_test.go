package flytek8s

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

var (
	podTemplate = &v1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name: "defaultPodTemplate",
			Namespace: "defaultNamespace",
		},
		Template: v1.PodTemplateSpec{
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					v1.Container{
						Command: []string{"flytepropeller"},
						Args:    []string{"--config", "/etc/flyte/config/*.yaml"},
					},
				},
			},
		},
	}
)

func TestPodTemplateInformer(t *testing.T) {
	ctx := context.TODO()

	kubeClient := fake.NewSimpleClientset()
	informer := podTemplateInformer{
		kubeClient:           kubeClient,
		podTemplateName:      podTemplate.Name,
		podTemplateNamespace: podTemplate.Namespace,
	}

	assert.NoError(t, informer.start(ctx))

	// create the podTemplate
	_, err := kubeClient.CoreV1().PodTemplates(podTemplate.Namespace).Create(ctx, podTemplate, metav1.CreateOptions{})
	assert.NoError(t, err)

	time.Sleep(10 * time.Millisecond)
	assert.NotNil(t, informer.podTemplate)
	assert.True(t, reflect.DeepEqual(podTemplate, informer.podTemplate))

	// TODO update the podTemplate in the namespace
	// TODO validate update

	// delete the podTemplate in the namespace
	err = kubeClient.CoreV1().PodTemplates(podTemplate.Namespace).Delete(ctx, podTemplate.Name, metav1.DeleteOptions{})
	assert.NoError(t, err)

	time.Sleep(10 * time.Millisecond)
	assert.Nil(t, informer.podTemplate)

	assert.NoError(t, informer.stop(ctx))
}
