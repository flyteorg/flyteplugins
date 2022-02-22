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

/*var podTemplateInformer := podTemplateInformer{
	eh?
}*/

type podTemplateInformer struct {
	kubeClient           kubernetes.Interface
	podTemplate          *v1.PodTemplate
	podTemplateName      string
	podTemplateNamespace string
	stopChan             chan struct{}
}

func (p *podTemplateInformer) start(ctx context.Context) error {
	if p.stopChan == nil && p.kubeClient != nil && p.podTemplateName != "" && p.podTemplateNamespace != "" {
		logger.Infof(ctx, "STARTING")
		// TODO hamersaw - parameterize defaultResync
		informerFactory := informers.NewSharedInformerFactoryWithOptions(p.kubeClient, 1*time.Second, informers.WithNamespace(p.podTemplateNamespace))

		informerFactory.Core().V1().PodTemplates().Informer().AddEventHandler(
			cache.ResourceEventHandlerFuncs{
				AddFunc: func(obj interface{}) {
					podTemplate, ok := obj.(*v1.PodTemplate)
					logger.Infof(context.TODO(), "ADDED OBJECT %v '%s'", ok, podTemplate.Name)
					if ok && podTemplate.Name == p.podTemplateName {
						p.podTemplate = podTemplate
						logger.Infof(context.TODO(), "SET OBJECT %v '%s'", ok, podTemplate.Name)
						//logger.Infof(context.TODO(), "Deletion triggered for %v", name)
					}
				},
				UpdateFunc: func(old, new interface{}) {
					podTemplate, ok := new.(*v1.PodTemplate)
					logger.Infof(context.TODO(), "UPDATED OBJECT '%s'", podTemplate.Name)
					if ok && podTemplate.Name == p.podTemplateName {
						p.podTemplate = podTemplate
						logger.Infof(context.TODO(), "SET OBJECT '%s'", podTemplate.Name)
						//logger.Infof(context.TODO(), "Deletion triggered for %v", name)
					}
				},
				DeleteFunc: func(obj interface{}) {
					podTemplate, ok := obj.(*v1.PodTemplate)
					logger.Infof(context.TODO(), "DELETED OBJECT")
					if ok && podTemplate.Name == p.podTemplateName {
						p.podTemplate = nil
						//logger.Infof(context.TODO(), "Deletion triggered for %v", name)
					}
				},
			})

		p.stopChan = make(chan struct{})
		go informerFactory.Start(p.stopChan)
	}

	return nil
}

func (p *podTemplateInformer) stop(ctx context.Context) error {
	logger.Infof(ctx, "STOPPING")
	if p.stopChan != nil {
		close(p.stopChan)
		p.stopChan = nil
	}

	return nil
}

/*func SetPodTemplateInformerClient(context ctx, kubeclient) error {
	podTemplateInformer.stop()
	podTemplateInformer.KubeClient = kubeclient
	podTemplateInformer.start()
}

func GetDefaultPodSpec() (PodSpec, error) {
	return podTemplateInformer.podTemplate.PodSpec
}

func onConfigUpdate(context, ctx) {
	if config.DefaultPodTemplate != podTemplateInformer.podTemplateName {
		podTemplateInformer.stop(ctx)
		podTemplateInformer.podTemplateName = config.DefaultPodTemplate
		podTemplateInformer.start(ctx)
	}
}*/
