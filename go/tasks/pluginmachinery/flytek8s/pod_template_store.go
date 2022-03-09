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
	namespacePodTemplates sync.Map
}

// NewPodTemplateStore initializes a new PodTemplateStore
func NewPodTemplateStore() PodTemplateStore {
	return PodTemplateStore{
		namespacePodTemplates: sync.Map{},
	}
}

// Get returns the PodTemplate associated with the given namespace. If one does not exist, it
// attempts to retrieve the one associated with the defaultNamespace parameter.
func (p *PodTemplateStore) Get(namespace string) *v1.PodTemplate {
	if podTemplate, ok := p.namespacePodTemplates.Load(namespace); ok {
		return podTemplate.(*v1.PodTemplate)
	}

	if podTemplate, ok := p.namespacePodTemplates.Load(p.defaultNamespace); ok {
		return podTemplate.(*v1.PodTemplate)
	}

	return nil
}

// Set stores the provided PodTemplate, overwritting a previously stored instance for the specified
// namespace.
func (p *PodTemplateStore) Set(podTemplate *v1.PodTemplate) {
	p.namespacePodTemplates.Store(podTemplate.Namespace, podTemplate)
}

// SetDefaultNamespace sets the default namespace for the PodTemplateStore.
func (p *PodTemplateStore) SetDefaultNamespace(namespace string) {
	p.defaultNamespace = namespace
}

// Remove deletes the PodTemplate from the PodTemplateStore.
func (p *PodTemplateStore) Remove(podTemplate *v1.PodTemplate) {
	p.namespacePodTemplates.Delete(podTemplate.Namespace)
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
