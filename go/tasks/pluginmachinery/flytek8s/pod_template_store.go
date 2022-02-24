package flytek8s

import (
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
)

var DefaultPodTemplateStore podTemplateStore = NewPodTemplateStore()

// TODO designed as standalone to extend to support tracking namespace specific default PodTemplates
type podTemplateStore struct {
	mutex      sync.Mutex
	podTemplate *v1.PodTemplate
}

func NewPodTemplateStore() podTemplateStore {
	return podTemplateStore {
		mutex: sync.Mutex{},
	}
}

func (p *podTemplateStore) Get() *v1.PodTemplate {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.podTemplate
}

func (p *podTemplateStore) Set(podTemplate *v1.PodTemplate) {
	p.mutex.Lock()
	p.podTemplate = podTemplate
	p.mutex.Unlock()
}

func GetPodTemplateUpdatesHandler(store *podTemplateStore, podTemplateName string) cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			podTemplate, ok := obj.(*v1.PodTemplate)
			if ok && podTemplate.Name == podTemplateName {
				store.Set(podTemplate)
			}
		},
		UpdateFunc: func(old, new interface{}) {
			podTemplate, ok := new.(*v1.PodTemplate)
			if ok && podTemplate.Name == podTemplateName {
				store.Set(podTemplate)
			}
		},
		DeleteFunc: func(obj interface{}) {
			podTemplate, ok := obj.(*v1.PodTemplate)
			if ok && podTemplate.Name == podTemplateName {
				store.Set(nil)
			}
		},
	}
}
