package flytek8s

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
)

func TestPodTemplateStore(t *testing.T) {
	ctx := context.TODO()

	podTemplate := &v1.PodTemplate{
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

	store := NewPodTemplateStore()

	kubeClient := fake.NewSimpleClientset()
	informerFactory := informers.NewSharedInformerFactoryWithOptions(kubeClient, 30*time.Second, informers.WithNamespace(podTemplate.Namespace))

	updateHandler := GetPodTemplateUpdatesHandler(&store, podTemplate.Name)
	informerFactory.Core().V1().PodTemplates().Informer().AddEventHandler(updateHandler)
	go informerFactory.Start(ctx.Done())

	// create the podTemplate
	_, err := kubeClient.CoreV1().PodTemplates(podTemplate.Namespace).Create(ctx, podTemplate, metav1.CreateOptions{})
	assert.NoError(t, err)

	time.Sleep(5 * time.Millisecond)
	assert.NotNil(t, store.Get())
	assert.True(t, reflect.DeepEqual(podTemplate, store.Get()))

	// update the podTemplate
	updatedPodTemplate := podTemplate.DeepCopy()
	updatedPodTemplate.Template.Spec.RestartPolicy = v1.RestartPolicyNever
	_, err = kubeClient.CoreV1().PodTemplates(podTemplate.Namespace).Update(ctx, updatedPodTemplate, metav1.UpdateOptions{})

	time.Sleep(5 * time.Millisecond)
	assert.NotNil(t, store.Get())
	assert.True(t, reflect.DeepEqual(updatedPodTemplate, store.Get()))

	// delete the podTemplate in the namespace
	err = kubeClient.CoreV1().PodTemplates(podTemplate.Namespace).Delete(ctx, podTemplate.Name, metav1.DeleteOptions{})
	assert.NoError(t, err)

	time.Sleep(5 * time.Millisecond)
	assert.Nil(t, store.Get())
}
