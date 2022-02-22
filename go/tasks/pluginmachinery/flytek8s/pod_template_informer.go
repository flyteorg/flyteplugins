package flytek8s

import (
	"context"
	"time"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/flytek8s/config"

	"github.com/flyteorg/flytestdlib/logger"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

var informer podTemplateInformer = podTemplateInformer{
	//podTemplateName: config.GetK8sPluginConfig().DefaultPodTemplateName,
	podTemplateName: "flyte-default-template",
}

type podTemplateInformer struct {
	kubeClient           kubernetes.Interface
	podTemplate          *v1.PodTemplate
	podTemplateName      string
	podTemplateNamespace string
	stopChan             chan struct{}
}

func (p *podTemplateInformer) start(ctx context.Context) error {
	logger.Infof(ctx, "podTemplateInformer %s %s", p.podTemplateName, p.podTemplateNamespace)
	if p.stopChan == nil && p.kubeClient != nil && p.podTemplateName != "" && p.podTemplateNamespace != "" {
		logger.Infof(ctx, "starting podTemplateInformer for podTemplate '%s' in namespace '%s'", p.podTemplateName, p.podTemplateNamespace)
		// TODO hamersaw - parameterize defaultResync
		informerFactory := informers.NewSharedInformerFactoryWithOptions(p.kubeClient, 30*time.Second, informers.WithNamespace(p.podTemplateNamespace))

		informerFactory.Core().V1().PodTemplates().Informer().AddEventHandler(
			cache.ResourceEventHandlerFuncs{
				AddFunc: func(obj interface{}) {
					podTemplate, ok := obj.(*v1.PodTemplate)
					if ok && podTemplate.Name == p.podTemplateName {
						p.podTemplate = podTemplate
						logger.Infof(context.TODO(), "added defaultPodTemplate '%s' from namespace '%s'", podTemplate.Name, podTemplate.Namespace)
					}
				},
				UpdateFunc: func(old, new interface{}) {
					podTemplate, ok := new.(*v1.PodTemplate)
					if ok && podTemplate.Name == p.podTemplateName {
						p.podTemplate = podTemplate
						logger.Infof(context.TODO(), "updated defaultPodTemplate '%s' from namespace '%s'", podTemplate.Name, podTemplate.Namespace)
					}
				},
				DeleteFunc: func(obj interface{}) {
					podTemplate, ok := obj.(*v1.PodTemplate)
					if ok && podTemplate.Name == p.podTemplateName {
						p.podTemplate = nil
						logger.Infof(context.TODO(), "deleted defaultPodTemplate '%s' from namespace '%s'", podTemplate.Name, podTemplate.Namespace)
					}
				},
			})

		p.stopChan = make(chan struct{})
		go informerFactory.Start(p.stopChan)
	}

	return nil
}

func (p *podTemplateInformer) stop(ctx context.Context) error {
	logger.Debugf(ctx, "stopping podTemplateInformer for podTemplate '%s' in namespace '%s'", p.podTemplateName, p.podTemplateNamespace)
	if p.stopChan != nil {
		close(p.stopChan)
		p.stopChan = nil
	}

	return nil
}

func InitDefaultPodTemplateInformer(ctx context.Context, kubeclient kubernetes.Interface, namespace string) error {
	err := informer.stop(ctx)
	if err != nil {
		return err
	}

	informer.kubeClient = kubeclient
	informer.podTemplateNamespace = namespace

	return informer.start(ctx)
}

func GetDefaultPodSpec() *v1.PodSpec {
	if informer.podTemplate != nil {
		return &informer.podTemplate.Template.Spec
	}

	return &v1.PodSpec{}
}

// TODO - link up OnConfigUpdate
func OnConfigUpdate(cfg config.K8sPluginConfig) {
	if cfg.DefaultPodTemplateName != informer.podTemplateName {
		ctx := context.Background()

		// TODO hamersaw - catch errors
		informer.stop(ctx)
		informer.podTemplate = nil
		informer.podTemplateName = cfg.DefaultPodTemplateName
		informer.start(ctx)
	}
}
