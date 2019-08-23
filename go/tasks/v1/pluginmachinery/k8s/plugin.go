package k8s

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pluginsCore "github.com/lyft/flyteplugins/go/tasks/v1/pluginmachinery/core"
)

//go:generate mockery -all -case=underscore

type PluginEntry struct {
	ID                  pluginsCore.TaskType
	RegisteredTaskTypes []pluginsCore.TaskType
	ResourceToWatch     runtime.Object
	Plugin              Plugin
	IsDefault           bool
}

type Resource interface {
	runtime.Object
	metav1.Object
	schema.ObjectKind
}

type PluginContext interface {
}

// Defines an interface that deals with k8s resources. Combined with K8sTaskExecutor, this provides an easier and more
// consistent way to write TaskExecutors that create k8s resources.
type Plugin interface {

	// Defines a func to create a query object (typically just object and type meta portions) that's used to query k8s
	// resources.
	BuildIdentityResource(ctx context.Context, taskCtx pluginsCore.TaskExecutionMetadata) (Resource, error)

	// Defines a func to create the full resource object that will be posted to k8s.
	BuildResource(ctx context.Context, taskCtx pluginsCore.TaskExecutionContext) (Resource, error)

	// Analyses the k8s resource and reports the status as TaskPhase.
	GetTaskPhase(ctx context.Context, pluginContext PluginContext, resource Resource) (pluginsCore.PhaseInfo, error)
}
