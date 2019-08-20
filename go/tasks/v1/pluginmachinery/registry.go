package pluginmachinery

import (
	"context"
	"sync"

	"github.com/lyft/flytestdlib/logger"

	"github.com/lyft/flyteplugins/go/tasks/v1/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/v1/pluginmachinery/k8s"
)

type taskPluginRegistry struct {
	m         sync.Mutex
	k8sPlugin []k8s.PluginEntry
	corePlugin []core.PluginEntry
}

var PluginRegistry = taskPluginRegistry{}

func (p *taskPluginRegistry) RegisterK8sPlugin(info k8s.PluginEntry) {
	if info.ID == "" {
		logger.Panicf(context.TODO(), "ID is required attribute for k8s plugin")
	}
	if len(info.RegisteredTaskTypes) == 0 {
		logger.Panicf(context.TODO(), "K8s Plugin should be registered to handle atleast one task type")
	}
	if info.Plugin == nil {
		logger.Panicf(context.TODO(), "K8s Plugin cannot be nil")
	}
	if info.ResourceToWatch == nil {
		logger.Panicf(context.TODO(), "The framework requires a K8s resource to watch, for valid plugin registration")
	}
	p.m.Lock()
	defer p.m.Unlock()
	p.k8sPlugin = append(p.k8sPlugin, info)
}

func (p *taskPluginRegistry) RegisterCorePlugin(info core.PluginEntry) {
	if info.ID == "" {
		logger.Panicf(context.TODO(), "ID is required attribute for k8s plugin")
	}
	if len(info.RegisteredTaskTypes) == 0 {
		logger.Panicf(context.TODO(), "K8s Plugin should be registered to handle atleast one task type")
	}
	/*if info.Plugin == nil {
		logger.Panicf(context.TODO(), "K8s Plugin cannot be nil")
	}*/
	p.m.Lock()
	defer p.m.Unlock()
	p.corePlugin = append(p.corePlugin, info)
}

func (p taskPluginRegistry) GetCorePlugins() []core.PluginEntry {
	p.m.Lock()
	defer p.m.Unlock()
	return append(p.corePlugin[:0:0], p.corePlugin...)
}

func (p taskPluginRegistry) GetK8sPlugins() []k8s.PluginEntry {
	p.m.Lock()
	defer p.m.Unlock()
	return append(p.k8sPlugin[:0:0], p.k8sPlugin...)
}