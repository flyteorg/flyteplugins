package flytek8s

import (
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
)

var DefaultPodTemplateStore PodTemplateStore = NewPodTemplateStore()

// PodTemplateStore maintains a thread-safe mapping of active PodTemplates with their associated
// namespaces.
type PodTemplateStore struct {
	defaultNamespace      string
	mutex                 sync.Mutex
	namespacePodTemplates map[string]*v1.PodTemplate
}

// NewPodTemplateStore initializes a new PodTemplateStore
func NewPodTemplateStore() PodTemplateStore {
	return PodTemplateStore{
		mutex:                 sync.Mutex{},
		namespacePodTemplates: make(map[string]*v1.PodTemplate),
	}
}

// Get returns the PodTemplate associated with the given namespace. If one does not exist, it
// attempts to retrieve the one associated with the defaultNamespace parameter.
func (p *PodTemplateStore) Get(namespace string) *v1.PodTemplate {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if podTemplate, ok := p.namespacePodTemplates[namespace]; ok {
		return podTemplate
	}

	return p.namespacePodTemplates[p.defaultNamespace]
}

// Set stores the provided PodTemplate, overwritting a previously stored instance for the specified
// namespace.
func (p *PodTemplateStore) Set(podTemplate *v1.PodTemplate) {
	p.mutex.Lock()
	p.namespacePodTemplates[podTemplate.Namespace] = podTemplate
	p.mutex.Unlock()
}

// SetDefaultNamespace sets the default namespace for the PodTemplateStore.
func (p *PodTemplateStore) SetDefaultNamespace(namespace string) {
	p.defaultNamespace = namespace
}

// Remove deletes the PodTemplate from the PodTemplateStore.
func (p *PodTemplateStore) Remove(podTemplate *v1.PodTemplate) {
	p.mutex.Lock()
	delete(p.namespacePodTemplates, podTemplate.Namespace)
	p.mutex.Unlock()
}

// GetPodTemplateUpdatesHandler returns a new ResourceEventHandler which adds / removes
// PodTemplates with the associated podTemplateName to / from the provided PodTemplateStore.
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
