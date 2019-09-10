package core

import (
	"github.com/lyft/flytestdlib/promutils"
	"k8s.io/apimachinery/pkg/types"
)

// When a change is observed, the owning entity with id types.NamespacedName can be triggered for re-validation
type EnqueueOwner func(id types.NamespacedName) error

// Passed to the Loader function when setting up a plugin
type SetupContext interface {
	// returns a callback mechanism that indicates that (workflow, task) is ready to be re-evaluated
	EnqueueOwner() EnqueueOwner
	// provides a k8s specific owner kind
	OwnerKind() string
	// a metrics scope to publish stats under
	MetricsScope() promutils.Scope
	// A kubernetes client to the bound cluster
	KubeClient() KubeClient
}
