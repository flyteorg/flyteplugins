package webapi

import (
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/webapi"
)

type pluginContext struct {
	webapi.TaskExecutionContext

	resourceMeta webapi.ResourceMeta
	resource     webapi.Resource
	reason       string
}

func (p pluginContext) Reason() string {
	return p.reason
}

func (p pluginContext) Resource() webapi.Resource {
	return p.resource
}

func (p pluginContext) ResourceMeta() webapi.ResourceMeta {
	return p.resourceMeta
}

func newPluginContext(resource webapi.Resource, resourceMeta webapi.ResourceMeta, reason string) pluginContext {
	return pluginContext{
		resourceMeta: resourceMeta,
		resource:     resource,
		reason:       reason,
	}
}
