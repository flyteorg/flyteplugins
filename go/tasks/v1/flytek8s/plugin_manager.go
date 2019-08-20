package flytek8s

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/lyft/flytestdlib/contextutils"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/promutils/labeled"

	"github.com/lyft/flyteplugins/go/tasks/v1/flytek8s/config"
	pluginsCore "github.com/lyft/flyteplugins/go/tasks/v1/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/v1/pluginmachinery/k8s"
	"github.com/lyft/flyteplugins/go/tasks/v1/utils"

	k8stypes "k8s.io/apimachinery/pkg/types"

	"github.com/lyft/flytestdlib/logger"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/lyft/flyteplugins/go/tasks/v1/errors"

	"sigs.k8s.io/controller-runtime/pkg/handler"
)

const pluginStateVersion = 1

type PluginPhase uint8

const (
	PluginPhaseNotStarted PluginPhase = iota
	PluginPhaseStarted
)

type PluginState struct {
	Phase PluginPhase
}

type PluginMetrics struct {
	Scope           promutils.Scope
	GetCacheMiss    labeled.StopWatch
	GetCacheHit     labeled.StopWatch
	GetAPILatency   labeled.StopWatch
	ResourceDeleted labeled.Counter
}

func newPluginMetrics(s promutils.Scope) PluginMetrics {
	return PluginMetrics{
		Scope: s,
		GetCacheMiss: labeled.NewStopWatch("get_cache_miss", "Cache miss on get resource calls.",
			time.Millisecond, s),
		GetCacheHit: labeled.NewStopWatch("get_cache_hit", "Cache miss on get resource calls.",
			time.Millisecond, s),
		GetAPILatency: labeled.NewStopWatch("get_api", "Latency for APIServer Get calls.",
			time.Millisecond, s),
		ResourceDeleted: labeled.NewCounter("pods_deleted", "Counts how many times CheckTaskStatus is"+
			" called with a deleted resource.", s),
	}
}

// A generic task executor for k8s-resource reliant tasks.
type PluginManager struct {
	id              string
	plugin          k8s.Plugin
	resourceToWatch runtime.Object

	// Supplied on Initialization
	// TODO decide the right place to put these interfaces or late-bind them?
	kubeClient   pluginsCore.KubeClient
	enqueueOwner pluginsCore.EnqueueOwner
	ownerKind    string
	metrics      PluginMetrics
}

func (e *PluginManager) GetProperties() pluginsCore.PluginProperties {
	return pluginsCore.PluginProperties{}
}

func AddObjectMetadata(taskCtx pluginsCore.TaskExecutionMetadata, o k8s.Resource, cfg *config.K8sPluginConfig) {
	o.SetNamespace(taskCtx.GetNamespace())
	o.SetAnnotations(utils.UnionMaps(cfg.DefaultAnnotations, o.GetAnnotations(), utils.CopyMap(taskCtx.GetAnnotations())))
	o.SetLabels(utils.UnionMaps(o.GetLabels(), utils.CopyMap(taskCtx.GetLabels()), cfg.DefaultLabels))
	o.SetOwnerReferences([]metav1.OwnerReference{taskCtx.GetOwnerReference()})
	o.SetName(taskCtx.GetTaskExecutionID().GetGeneratedName())
	if cfg.InjectFinalizer {
		f := append(o.GetFinalizers(), finalizer)
		o.SetFinalizers(f)
	}
}

func (e *PluginManager) Setup(ctx context.Context, iCtx pluginsCore.SetupContext) error {
	if iCtx.EnqueueOwner() == nil {
		return errors.Errorf(errors.PluginInitializationFailed, "Failed to initialize plugin, enqueue Owner cannot be nil or empty.")
	}

	metricScope := iCtx.MetricsScope().NewSubScope(e.GetID())
	e.metrics = newPluginMetrics(metricScope)

	e.kubeClient = iCtx.KubeClient()
	if e.kubeClient == nil {
		return errors.Errorf(errors.PluginInitializationFailed, "Failed to initialize K8sResource Plugin, Kubeclient cannot be nil!")
	}

	src := source.Kind{
		Type: e.resourceToWatch,
	}

	/*
		if _, err := inject.CacheInto(instance.informersCache, &src); err != nil {
			return err
		}*/

	// TODO: a more unique workqueue name
	q := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(),
		e.resourceToWatch.GetObjectKind().GroupVersionKind().Kind)

	workflowParentPredicate := func(o metav1.Object) bool {
		ownerReference := metav1.GetControllerOf(o)
		if ownerReference != nil {
			if ownerReference.Kind == e.ownerKind {
				return true
			}
		}
		return false
	}

	return src.Start(handler.Funcs{
		CreateFunc: func(evt event.CreateEvent, q2 workqueue.RateLimitingInterface) {
			if err := e.enqueueOwner(k8stypes.NamespacedName{Name: evt.Meta.GetName(), Namespace: evt.Meta.GetNamespace()}); err != nil {
				logger.Warnf(ctx, "Failed to handle Create event for object [%v]", evt.Meta.GetName())
			}
		},
		UpdateFunc: func(evt event.UpdateEvent, q2 workqueue.RateLimitingInterface) {
			if err := e.enqueueOwner(k8stypes.NamespacedName{Name: evt.MetaNew.GetName(), Namespace: evt.MetaNew.GetNamespace()}); err != nil {
				logger.Warnf(ctx, "Failed to handle Update event for object [%v]", evt.MetaNew.GetName())
			}
		},
		DeleteFunc: func(evt event.DeleteEvent, q2 workqueue.RateLimitingInterface) {
			if err := e.enqueueOwner(k8stypes.NamespacedName{Name: evt.Meta.GetName(), Namespace: evt.Meta.GetNamespace()}); err != nil {
				logger.Warnf(ctx, "Failed to handle Delete event for object [%v]", evt.Meta.GetName())
			}
		},
		GenericFunc: func(evt event.GenericEvent, q2 workqueue.RateLimitingInterface) {
			if err := e.enqueueOwner(k8stypes.NamespacedName{Name: evt.Meta.GetName(), Namespace: evt.Meta.GetNamespace()}); err != nil {
				logger.Warnf(ctx, "Failed to handle Generic event for object [%v]", evt.Meta.GetName())
			}
		},
	}, q, predicate.Funcs{
		CreateFunc: func(createEvent event.CreateEvent) bool {
			return workflowParentPredicate(createEvent.Meta)
		},
		UpdateFunc: func(updateEvent event.UpdateEvent) bool {
			// TODO we should filter out events in case there are no updates observed between the old and new?
			return workflowParentPredicate(updateEvent.MetaNew)
		},
		DeleteFunc: func(deleteEvent event.DeleteEvent) bool {
			return workflowParentPredicate(deleteEvent.Meta)
		},
		GenericFunc: func(genericEvent event.GenericEvent) bool {
			return workflowParentPredicate(genericEvent.Meta)
		},
	})
}

func (e *PluginManager) GetID() string {
	return e.id
}

func (e *PluginManager) LaunchResource(ctx context.Context, tCtx pluginsCore.TaskExecutionContext) (pluginsCore.Transition, error) {

	o, err := e.plugin.BuildResource(ctx, tCtx)
	if err != nil {
		return pluginsCore.UnknownTransition, err
	}

	AddObjectMetadata(tCtx.TaskExecutionMetadata(), o, config.GetK8sPluginConfig())
	logger.Infof(ctx, "Creating Object: Type:[%v], Object:[%v/%v]", o.GroupVersionKind(), o.GetNamespace(), o.GetName())

	err = e.kubeClient.GetClient().Create(ctx, o)
	if err != nil && !k8serrors.IsAlreadyExists(err) {
		if k8serrors.IsForbidden(err) {
			if strings.Contains(err.Error(), "exceeded quota") {
				// TODO: Quota errors are retried forever, it would be good to have support for backoff strategy.
				logger.Warnf(ctx, "Failed to launch job, resource quota exceeded. Err: %v", err)
				return pluginsCore.DoTransition(pluginsCore.PhaseInfoQueued(time.Now(), pluginsCore.DefaultPhaseVersion, "failed to launch job, resource quota exceeded.")), nil
			}
			return pluginsCore.DoTransition(pluginsCore.PhaseInfoRetryableFailure("RuntimeFailure", err.Error(), nil)), nil
		}
		logger.Errorf(ctx, "Failed to launch job, system error. Err: %v", err)
		return pluginsCore.UnknownTransition, err
	}

	return pluginsCore.DoTransition(pluginsCore.PhaseInfoQueued(time.Now(), pluginsCore.DefaultPhaseVersion+1, "task submitted to K8s")), nil
}

func (e *PluginManager) CheckResourcePhase(ctx context.Context, tCtx pluginsCore.TaskExecutionContext) (pluginsCore.Transition, error) {

	o, err := e.plugin.BuildIdentityResource(ctx, tCtx.TaskExecutionMetadata())
	if err != nil {
		logger.Errorf(ctx, "Failed to build the Resource with name: %v. Error: %v", tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), err)
		return pluginsCore.DoTransition(pluginsCore.PhaseInfoFailure("BadTaskDefinition", fmt.Sprintf("Failed to build resource, caused by: %s", err.Error()), nil)), nil
	}

	AddObjectMetadata(tCtx.TaskExecutionMetadata(), o, config.GetK8sPluginConfig())
	nsName := k8stypes.NamespacedName{Namespace: o.GetNamespace(), Name: o.GetName()}
	// Attempt to get resource from informer cache, if not found, retrieve it from API server.
	if err := e.kubeClient.GetClient().Get(ctx, nsName, o); err != nil {
		if IsK8sObjectNotExists(err) {
			// This happens sometimes because a node gets removed and K8s deletes the pod. This will result in a
			// Pod does not exist error. This should be retried using the retry policy
			logger.Warningf(ctx, "Failed to find the Resource with name: %v. Error: %v", nsName, err)
			failureReason := fmt.Sprintf("resource not found, name [%s]. reason: %s", nsName.String(), err.Error())
			return pluginsCore.DoTransition(pluginsCore.PhaseInfoRetryableFailure("Tachycardia", failureReason, nil)), nil
		}

		logger.Warningf(ctx, "Failed to retrieve Resource Details with name: %v. Error: %v", nsName, err)
		return pluginsCore.UnknownTransition, err
	}
	if o.GetDeletionTimestamp() != nil {
		e.metrics.ResourceDeleted.Inc(ctx)
	}

	p, err := e.plugin.GetTaskPhase(ctx, tCtx, o)
	if err != nil {
		logger.Warnf(ctx, "failed to check status of resource in plugin [%s], with error: %s", e.GetID(), err.Error())
		return pluginsCore.UnknownTransition, err
	}

	if p.Phase() == pluginsCore.PhaseSuccess {
		opReader := NewRemoteFileOutputReader(ctx, tCtx.DataStore(), tCtx.OutputWriter(), tCtx.MaxDatasetSizeBytes())
		err := tCtx.OutputWriter().Put(ctx, opReader)
		if err != nil {
			return pluginsCore.UnknownTransition, err
		}
		return pluginsCore.DoTransition(p), nil
	}

	if !p.Phase().IsTerminal() && o.GetDeletionTimestamp() != nil {
		// If the object has been deleted, that is, it has a deletion timestamp, but is not in a terminal state, we should
		// mark the task as a retryable failure.  We've seen this happen when a kubelet disappears - all pods running on
		// the node are marked with a deletionTimestamp, but our finalizers prevent the pod from being deleted.
		// This can also happen when a user deletes a Pod directly.
		failureReason := fmt.Sprintf("object [%s] terminated in the background, manually", nsName.String())
		return pluginsCore.DoTransition(pluginsCore.PhaseInfoRetryableFailure("tachycardia", failureReason, nil)), nil
	}

	return pluginsCore.DoTransition(p), nil
}

func (e PluginManager) Handle(ctx context.Context, tCtx pluginsCore.TaskExecutionContext) (pluginsCore.Transition, error) {
	ps := PluginState{}
	if v, err := tCtx.PluginStateReader().Get(&ps); err != nil {
		if v != pluginStateVersion {
			return pluginsCore.DoTransition(pluginsCore.PhaseInfoRetryableFailure(errors.CorruptedPluginState, fmt.Sprintf("plugin state version mismatch expected [%d] got [%d]", pluginStateVersion, v), nil)), nil
		}
		return pluginsCore.UnknownTransition, errors.Wrapf(errors.CorruptedPluginState, err, "Failed to read unmarshal custom state")
	}
	if ps.Phase == PluginPhaseNotStarted {
		t, err := e.LaunchResource(ctx, tCtx)
		if err == nil {
			if err := tCtx.PluginStateWriter().Put(pluginStateVersion, &PluginState{Phase: PluginPhaseStarted}); err != nil {
				return pluginsCore.UnknownTransition, err
			}
		}
		return t, err
	}
	return e.CheckResourcePhase(ctx, tCtx)
}

func (e PluginManager) Abort(ctx context.Context, tCtx pluginsCore.TaskExecutionContext) error {
	logger.Infof(ctx, "KillTask invoked for %v, nothing to be done.", tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName())
	return nil
}

func (e *PluginManager) ClearFinalizers(ctx context.Context, o k8s.Resource) error {
	if len(o.GetFinalizers()) > 0 {
		o.SetFinalizers([]string{})
		err := e.kubeClient.GetClient().Update(ctx, o)
		if err != nil && !IsK8sObjectNotExists(err) {
			logger.Warningf(ctx, "Failed to clear finalizers for Resource with name: %v/%v. Error: %v",
				o.GetNamespace(), o.GetName(), err)
			return err
		}
	} else {
		logger.Debugf(ctx, "Finalizers are already empty for Resource with name: %v/%v",
			o.GetNamespace(), o.GetName())
	}
	return nil
}

func (e *PluginManager) Finalize(ctx context.Context, tCtx pluginsCore.TaskExecutionContext) error {
	// If you change InjectFinalizer on the
	if config.GetK8sPluginConfig().InjectFinalizer {
		o, err := e.plugin.BuildIdentityResource(ctx, tCtx.TaskExecutionMetadata())
		if err != nil {
			// This will recurrent, so we will skip further finalize
			logger.Errorf(ctx, "Failed to build the Resource with name: %v. Error: %v, when finalizing.", tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), err)
			return nil
		}
		AddObjectMetadata(tCtx.TaskExecutionMetadata(), o, config.GetK8sPluginConfig())
		nsName := k8stypes.NamespacedName{Namespace: o.GetNamespace(), Name: o.GetName()}
		// Attempt to get resource from informer cache, if not found, retrieve it from API server.
		if err := e.kubeClient.GetClient().Get(ctx, nsName, o); err != nil {
			if IsK8sObjectNotExists(err) {
				return nil
			}
			// This happens sometimes because a node gets removed and K8s deletes the pod. This will result in a
			// Pod does not exist error. This should be retried using the retry policy
			logger.Warningf(ctx, "Failed in finalizing get Resource with name: %v. Error: %v", nsName, err)
			return err
		}

		// This must happen after sending admin event. It's safe against partial failures because if the event failed, we will
		// simply retry in the next round. If the event succeeded but this failed, we will try again the next round to send
		// the same event (idempotent) and then come here again...
		err = e.ClearFinalizers(ctx, o)
		if err != nil {
			return err
		}
	}
	return nil
}

// Creates a K8s generic task executor. This provides an easier way to build task executors that create K8s resources.
func NewPluginManager(entry k8s.PluginEntry) *PluginManager {

	return &PluginManager{
		id:              entry.ID,
		plugin:          entry.Plugin,
		resourceToWatch: entry.ResourceToWatch,
	}
}

func init() {
	labeled.SetMetricKeys(contextutils.ProjectKey, contextutils.DomainKey, contextutils.WorkflowIDKey, contextutils.TaskIDKey)
}
