package k8s

import (
	"testing"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/flytek8s"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/flytek8s/config"
	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"
)

func TestGetTaskContainerTask(t *testing.T) {
	pod := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name: "PrimaryContainer",
				},
			},
		},
	}
	index, err := getTaskContainerIndex(pod)
	assert.NilError(t, err)
	assert.Equal(t, index, 0)

	pod.Spec.Containers = append(pod.Spec.Containers, v1.Container{Name: config.GetK8sPluginConfig().CoPilot.NamePrefix + flytek8s.Sidecar})
	index, err = getTaskContainerIndex(pod)
	assert.NilError(t, err)
	assert.Equal(t, index, 0)

	pod.Annotations = map[string]string{flytek8s.PrimaryContainerKey: "PrimaryContainer"}
	index, err = getTaskContainerIndex(pod)
	assert.NilError(t, err)
	assert.Equal(t, index, 0)

	pod.Spec.Containers[0].Name = "SecondaryContainer"
	_, err = getTaskContainerIndex(pod)
	assert.ErrorContains(t, err, "Couldn't find any container matching the primary container")

}
