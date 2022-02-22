package flytek8s

import (
	"context"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"

	v1 "k8s.io/api/core/v1"
)

/*var podTemplateInformer := podTemplateInformer{
	eh?
}*/

type podTemplateInformer struct {
	kubeClient      core.KubeClient
	podTemplate     *v1.PodTemplate
	podTemplateName string
	stopChan        chan struct{}
}

func (p podTemplateInformer) start(ctx context.Context) error {
	if p.kubeClient != nil && p.podTemplateName != "" {
		//p.stopChan = chan
		//go informerFactory.Start(p.stopChan)
	}

	return nil
}

func (p podTemplateInformer) stop(ctx context.Context) error {
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
