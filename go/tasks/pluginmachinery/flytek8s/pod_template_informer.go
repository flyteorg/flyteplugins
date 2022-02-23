package flytek8s

import (
	"context"
	"time"

	"github.com/flyteorg/flytestdlib/logger"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

var informer podTemplateInformer = podTemplateInformer{}

// podTemplateInformer manages a watch over the specified PodTemplate (defined by podTemplateName
// and podTemplateNamespace) using the provided provided kubeClient.
type podTemplateInformer struct {
	kubeClient           kubernetes.Interface
	podTemplate          *v1.PodTemplate
	podTemplateName      string
	podTemplateNamespace string
	resync               time.Duration
	stopChan             chan struct{}
}

// start creates and launches a new informer to watch the specified PodTemplate. This includes adding,
// updating, or deleting the PodTemplate metadata as required.
func (p *podTemplateInformer) start(ctx context.Context) error {
	if p.stopChan == nil && p.kubeClient != nil && p.podTemplateName != "" && p.podTemplateNamespace != "" {
		logger.Infof(ctx, "starting informer for default PodTemplate '%s' in namespace '%s'", p.podTemplateName, p.podTemplateNamespace)
		informerFactory := informers.NewSharedInformerFactoryWithOptions(p.kubeClient, informer.resync, informers.WithNamespace(p.podTemplateNamespace))

		informerFactory.Core().V1().PodTemplates().Informer().AddEventHandler(
			cache.ResourceEventHandlerFuncs{
				AddFunc: func(obj interface{}) {
					podTemplate, ok := obj.(*v1.PodTemplate)
					if ok && podTemplate.Name == p.podTemplateName {
						p.podTemplate = podTemplate
						logger.Infof(context.TODO(), "added default PodTemplate '%s' for namespace '%s'", podTemplate.Name, podTemplate.Namespace)
					}
				},
				UpdateFunc: func(old, new interface{}) {
					podTemplate, ok := new.(*v1.PodTemplate)
					if ok && podTemplate.Name == p.podTemplateName {
						p.podTemplate = podTemplate
						logger.Infof(context.TODO(), "updated default PodTemplate '%s' for namespace '%s'", podTemplate.Name, podTemplate.Namespace)
					}
				},
				DeleteFunc: func(obj interface{}) {
					podTemplate, ok := obj.(*v1.PodTemplate)
					if ok && podTemplate.Name == p.podTemplateName {
						p.podTemplate = nil
						logger.Infof(context.TODO(), "deleted default PodTemplate '%s' for namespace '%s'", podTemplate.Name, podTemplate.Namespace)
					}
				},
			})

		p.stopChan = make(chan struct{})
		go informerFactory.Start(p.stopChan)
	}

	return nil
}

// stop closes the PodTemplate informer to halt the watch.
func (p *podTemplateInformer) stop(ctx context.Context) error {
	logger.Infof(ctx, "stopping informer for default PodTemplate '%s' in namespace '%s'", p.podTemplateName, p.podTemplateNamespace)
	if p.stopChan != nil {
		close(p.stopChan)
		p.stopChan = nil
	}

	return nil
}

// InitDefaultPodTemplateInformer sets the k8s client and namespace for the default PodTemplate
// informer.
func InitDefaultPodTemplateInformer(ctx context.Context, kubeclient kubernetes.Interface, namespace string) error {
	err := informer.stop(ctx)
	if err != nil {
		return err
	}

	informer.kubeClient = kubeclient
	informer.podTemplateNamespace = namespace

	return informer.start(ctx)
}

// GetDefaultPodTemplate retrieves the current default PodTemplate.
func GetDefaultPodTemplate() *v1.PodTemplate {
	return informer.podTemplate
}

// onConfigUpdated updates the PodTemplate informer when configuration parameters change.
func onConfigUpdated(ctx context.Context, cfg K8sPluginConfig) {
	if cfg.DefaultPodTemplateName != informer.podTemplateName || cfg.DefaultPodTemplateResync.Duration != informer.resync {
		if err := informer.stop(ctx); err != nil {
			logger.Warnf(ctx, "failed to stop the default PodTemplate informer")
			return
		}

		informer.podTemplate = nil
		informer.podTemplateName = cfg.DefaultPodTemplateName
		informer.resync = cfg.DefaultPodTemplateResync.Duration

		if err := informer.start(ctx); err != nil {
			logger.Warnf(ctx, "failed to start the default PodTemplate informer")
		}
	}
}
