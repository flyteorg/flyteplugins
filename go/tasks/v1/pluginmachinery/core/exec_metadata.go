package core

import (
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"k8s.io/api/core/v1"
	v12 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Interface to expose any overrides that have been set for this task (like resource overrides etc)
type TaskOverrides interface {
	GetResources() *v1.ResourceRequirements
	GetConfig() *v1.ConfigMap
}

// Simple Interface to expose the ExecutionID of the running Task
type TaskExecutionID interface {
	GetGeneratedName() string
	GetID() core.TaskExecutionIdentifier
}

// TaskContext represents any execution information for a Task. It is used to communicate meta information about the
// execution or any previously stored information
type TaskExecutionMetadata interface {
	GetOwnerID() types.NamespacedName
	GetTaskExecutionID() TaskExecutionID
	GetNamespace() string
	GetOwnerReference() v12.OwnerReference
	GetOverrides() TaskOverrides
	GetLabels() map[string]string
	GetAnnotations() map[string]string
	GetK8sServiceAccount() string
}

