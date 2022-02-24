package flytek8s

import (
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
)

var DefaultPodTemplateStore PodTemplateStore = NewPodTemplateStore()

type PodTemplateStore struct {
	defaultNamespace      string
	mutex                 sync.Mutex
	namespacePodTemplates map[string]*v1.PodTemplate
}

func NewPodTemplateStore() PodTemplateStore {
	return PodTemplateStore{
		mutex:                 sync.Mutex{},
		namespacePodTemplates: make(map[string]*v1.PodTemplate),
	}
}

func (p *PodTemplateStore) Get(namespace string) *v1.PodTemplate {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if podTemplate, ok := p.namespacePodTemplates[namespace]; ok {
		return podTemplate
	}

	return p.namespacePodTemplates[p.defaultNamespace]
}

func (p *PodTemplateStore) Set(podTemplate *v1.PodTemplate) {
	p.mutex.Lock()
	p.namespacePodTemplates[podTemplate.Namespace] = podTemplate
	p.mutex.Unlock()
}

func (p *PodTemplateStore) SetDefaultNamespace(namespace string) {
	p.defaultNamespace = namespace
}

func (p *PodTemplateStore) Remove(podTemplate *v1.PodTemplate) {
	p.mutex.Lock()
	delete(p.namespacePodTemplates, podTemplate.Namespace)
	p.mutex.Unlock()
}

func GetPodTemplateUpdatesHandler(store *PodTemplateStore, podTemplateName string) cache.ResourceEventHandler {
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
				store.Remove(podTemplate)
			}
		},
	}
}
