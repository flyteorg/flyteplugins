package core

import (
	"github.com/lyft/flytestdlib/promutils"
	"k8s.io/apimachinery/pkg/types"
)

type EnqueueOwner func(name types.NamespacedName) error

type SetupContext interface {

	EnqueueOwner() EnqueueOwner
	OwnerKind() string
	MetricsScope() promutils.Scope
	KubeClient() KubeClient

}
