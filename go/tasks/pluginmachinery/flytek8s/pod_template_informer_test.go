package flytek8s

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPodTemplateInformer(t *testing.T) {
	ctx := context.TODO()

	informer := podTemplateInformer{}

	assert.NoError(t, informer.start(ctx))
	assert.NoError(t, informer.stop(ctx))
}
