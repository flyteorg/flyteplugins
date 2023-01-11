package flytek8s

import (
	"context"
	"fmt"
	"sync"

	pluginsCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/utils"

	"github.com/flyteorg/flytestdlib/logger"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
)

// TODO @hamersaw - do not export
var DefaultPodTemplateStore PodTemplateStore = NewPodTemplateStore()

// PodTemplateStore maintains a thread-safe mapping of active PodTemplates with their associated
// namespaces.
type PodTemplateStore struct {
	sync.Map
	defaultNamespace       string
	defaultPodTemplateName string
}

// NewPodTemplateStore initializes a new PodTemplateStore
func NewPodTemplateStore() PodTemplateStore {
	return PodTemplateStore{}
}

// TODO @hamersaw - update docs
// LoadOrDefault returns the PodTemplate associated with the given namespace. If one does not exist
// it attempts to retrieve the one associated with the defaultNamespace parameter.
func (p *PodTemplateStore) LoadOrDefaultName(namespace string, podTemplateName string) *v1.PodTemplate {
	fmt.Println("HAMERSAW - searching for '%s:%s'", namespace, podTemplateName)
	if value, ok := p.Load(podTemplateName); ok {
		fmt.Println("HAMERSAW - found name '%s'", podTemplateName)
		podTemplates := value.(*sync.Map)
		if podTemplate, ok := podTemplates.Load(namespace); ok {
			fmt.Println("HAMERSAW - found namespace '%s'", namespace)
			return podTemplate.(*v1.PodTemplate)
		}

		if podTemplate, ok := podTemplates.Load(p.defaultNamespace); ok {
			fmt.Println("HAMERSAW - found namespace '%s'", p.defaultNamespace)
			return podTemplate.(*v1.PodTemplate)
		}
	}

	return nil
}

// TODO @hamersaw - remove
func (p *PodTemplateStore) LoadOrDefault(namespace string) *v1.PodTemplate {
	if value, ok := p.Load(p.defaultPodTemplateName); ok {
		podTemplates := value.(*sync.Map)
		if podTemplate, ok := podTemplates.Load(namespace); ok {
			return podTemplate.(*v1.PodTemplate)
		}

		if podTemplate, ok := podTemplates.Load(p.defaultNamespace); ok {
			return podTemplate.(*v1.PodTemplate)
		}
	}

	return nil
}

// SetDefaults sets the default namespace and PodTemplate name for the PodTemplateStore.
func (p *PodTemplateStore) SetDefaults(namespace string, podTemplateName string) {
	fmt.Println("HAMERSAW - setting defaults '%s:%s'", namespace, podTemplateName)
	p.defaultNamespace = namespace
	p.defaultPodTemplateName = podTemplateName
}

// TODO @hamersaw - fix documentation
// GetPodTemplateUpdatesHandler returns a new ResourceEventHandler which adds / removes
// PodTemplates with the associated podTemplateName to / from the provided PodTemplateStore.
func GetPodTemplateUpdatesHandler(store *PodTemplateStore) cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if podTemplate, ok := obj.(*v1.PodTemplate); ok {
				value, _ := store.LoadOrStore(podTemplate.Name, &sync.Map{})
				podTemplates := value.(*sync.Map)
				podTemplates.Store(podTemplate.Namespace, podTemplate)
				logger.Debugf(context.Background(), "added pod template '%s:%s'", podTemplate.Namespace, podTemplate.Name)
				// TODO @hamersaw - add logger message
			}
			
			/*podTemplate, ok := obj.(*v1.PodTemplate)
			if ok && podTemplate.Name == podTemplateName {
				store.Store(podTemplate.Namespace, podTemplate)
			}*/
		},
		UpdateFunc: func(old, new interface{}) {
			if podTemplate, ok := new.(*v1.PodTemplate); ok {
				value, _ := store.LoadOrStore(podTemplate.Name, &sync.Map{})
				podTemplates := value.(*sync.Map)
				podTemplates.Store(podTemplate.Namespace, podTemplate)
				logger.Debugf(context.Background(), "added pod template '%s:%s'", podTemplate.Namespace, podTemplate.Name)
				// TODO @hamersaw - add logger message
			}

			/*podTemplate, ok := new.(*v1.PodTemplate)
			if ok && podTemplate.Name == podTemplateName {
				store.Store(podTemplate.Namespace, podTemplate)
			}*/
		},
		DeleteFunc: func(obj interface{}) {
			if podTemplate, ok := obj.(*v1.PodTemplate); ok {
				if value, ok := store.Load(podTemplate.Name); ok{
					podTemplates := value.(sync.Map)
					podTemplates.Delete(podTemplate.Namespace)
					// TODO @hamersaw - add logger message
					// TODO - doc: specifically not deleting empty maps from store because then there may be race conditions
				}
			}

			/*podTemplate, ok := obj.(*v1.PodTemplate)
			if ok && podTemplate.Name == podTemplateName {
				store.Delete(podTemplate.Namespace)
			}*/
		},
	}
}

// TODO @hamersaw - document
func getPodTemplate(ctx context.Context, tCtx pluginsCore.TaskExecutionContext) (*v1.PodTemplate, error) {
	taskTemplate, err := tCtx.TaskReader().Read(ctx)
	if err != nil {
		return nil, err
		//return nil, errors.Errorf(errors.BadTaskSpecification,
		//	"TaskSpecification cannot be read, Err: [%v]", err.Error())
	}

	var podTemplate *v1.PodTemplate
	if taskTemplate.GetPodTemplateName() != "" {
		// retrieve PodTemplate by name from PodTemplateStore
		podTemplate = DefaultPodTemplateStore.LoadOrDefaultName(tCtx.TaskExecutionMetadata().GetNamespace(), taskTemplate.GetPodTemplateName())
	} else if taskTemplate.GetPodTemplate() != nil {
		// parse PodTemplate from struct
		podTemplate = &v1.PodTemplate{}
		err := utils.UnmarshalStructToObj(taskTemplate.GetPodTemplate(), podTemplate)
		if err != nil {
			return nil, err
			//return nil, errors.Errorf(errors.BadTaskSpecification,
			//	"invalid TaskSpecification [%v], Err: [%v]", task.GetCustom(), err.Error())
		}
	} else {
		// check for default PodTemplate
		podTemplate = DefaultPodTemplateStore.LoadOrDefaultName(tCtx.TaskExecutionMetadata().GetNamespace(), DefaultPodTemplateStore.defaultPodTemplateName)
	}

	return podTemplate, nil
}
