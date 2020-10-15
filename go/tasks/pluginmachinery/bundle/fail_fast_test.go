package bundle

import (
	"context"
	"testing"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/stretchr/testify/assert"
)

var testHandler = failFastHandler{}

func TestFailFastGetID(t *testing.T) {
	assert.Equal(t, "fail-fast", testHandler.GetID())
}

func TestGetProperties(t *testing.T) {
	assert.Empty(t, testHandler.GetProperties())
}

func TestHandleAlwaysFails(t *testing.T) {
	transition, err := testHandler.Handle(context.TODO(), nil)
	assert.NoError(t, err)
	assert.Equal(t, core.PhasePermanentFailure, transition.Info().Phase())
}

func TestAbort(t *testing.T) {
	err := testHandler.Abort(context.TODO(), nil)
	assert.NoError(t, err)
}

func TestFinalize(t *testing.T) {
	err := testHandler.Finalize(context.TODO(), nil)
	assert.NoError(t, err)
}
