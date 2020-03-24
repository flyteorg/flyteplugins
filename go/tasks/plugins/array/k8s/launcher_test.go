package k8s

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
)

func TestApplyNodeSelectorLabels(t *testing.T) {
	ctx := context.Background()
	cfg := &Config{
		NodeSelector: map[string]string{
			"disktype": "ssd",
		},
	}
	pod := &corev1.Pod{}

	pod = applyNodeSelectorLabels(ctx, cfg, pod)

	assert.Equal(t, pod.Spec.NodeSelector, cfg.NodeSelector)
}
