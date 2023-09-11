package pluginmachinery

import (
	"context"
	"runtime"
	"sync"

	internalRemote "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/internal/webapi"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/webapi"

	"github.com/flyteorg/flytestdlib/logger"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/k8s"
)

type taskPluginRegistry struct {
	m          sync.Mutex
	k8sPlugin  []k8s.PluginEntry
	corePlugin []core.PluginEntry
}

// A singleton variable that maintains a registry of all plugins. The framework uses this to access all plugins
var pluginRegistry = &taskPluginRegistry{}

func PluginRegistry() TaskPluginRegistry {
	return pluginRegistry
}

func (p *taskPluginRegistry) RegisterRemotePlugin(info webapi.PluginEntry) {
	pc, file, line, _ := runtime.Caller(1)
	funcName := runtime.FuncForPC(pc).Name()
	logger.Infof(context.TODO(), "@@@ RegisterRemotePlugin was called by file [%v] [%v]:[%v]", file, funcName, line)

	logger.Infof(context.TODO(), "@@@ Registering Remote plugin Info ID [%v]", info.ID)
	logger.Infof(context.TODO(), "@@@ Registering Remote plugin Info SupportedTaskTypes [%v]", info.SupportedTaskTypes)
	logger.Infof(context.TODO(), "@@@ Registering Remote plugin Info PluginLoader [%v]", info.PluginLoader)
	logger.Infof(context.TODO(), "@@@ Registering Remote plugin Info [%v]", info)
	ctx := context.Background()
	if info.ID == "" {
		logger.Panicf(ctx, "ID is required attribute for k8s plugin")
	}

	if len(info.SupportedTaskTypes) == 0 {
		logger.Panicf(ctx, "AsyncPlugin should be registered to handle at least one task type")
	}

	if info.PluginLoader == nil {
		logger.Panicf(ctx, "PluginLoader cannot be nil")
	}

	p.m.Lock()
	defer p.m.Unlock()
	p.corePlugin = append(p.corePlugin, internalRemote.CreateRemotePlugin(info))
	pluginEntry := internalRemote.CreateRemotePlugin(info)
	logger.Infof(context.TODO(), "@@@ CreateRemotePlugin ID: %s", pluginEntry.ID)
	logger.Infof(context.TODO(), "@@@ CreateRemotePlugin RegisteredTaskTypes: %v", pluginEntry.RegisteredTaskTypes)
	// logger.Infof(context.TODO(), "@@@ internalRemote.CreateRemotePlugin(info) [%v]", internalRemote.CreateRemotePlugin(info))
}

func CreateRemotePlugin(pluginEntry webapi.PluginEntry) core.PluginEntry {
	return internalRemote.CreateRemotePlugin(pluginEntry)
}

// Use this method to register Kubernetes Plugins
func (p *taskPluginRegistry) RegisterK8sPlugin(info k8s.PluginEntry) {
	pc, file, line, _ := runtime.Caller(1)
	funcName := runtime.FuncForPC(pc).Name()
	logger.Infof(context.TODO(), "@@@ RegisterK8sPlugin was called by file [%v] [%v]:[%v]", file, funcName, line)

	logger.Infof(context.TODO(), "@@@ Registering K8s plugin Info ID [%v]", info.ID)
	logger.Infof(context.TODO(), "@@@ Registering K8s plugin Info RegisteredTaskTypes [%v]", info.RegisteredTaskTypes)
	logger.Infof(context.TODO(), "@@@ Registering K8s plugin Info ResourceToWatch [%v]", info.ResourceToWatch)
	logger.Infof(context.TODO(), "@@@ Registering K8s plugin Info [%v]", info)
	if info.ID == "" {
		logger.Panicf(context.TODO(), "ID is required attribute for k8s plugin")
	}

	if len(info.RegisteredTaskTypes) == 0 {
		logger.Panicf(context.TODO(), "K8s AsyncPlugin should be registered to handle atleast one task type")
	}

	if info.Plugin == nil {
		logger.Panicf(context.TODO(), "K8s AsyncPlugin cannot be nil")
	}

	if info.ResourceToWatch == nil {
		logger.Panicf(context.TODO(), "The framework requires a K8s resource to watch, for valid plugin registration")
	}

	p.m.Lock()
	defer p.m.Unlock()
	p.k8sPlugin = append(p.k8sPlugin, info)
	logger.Infof(context.TODO(), "@@@ p.k8sPlugin [%v]", p.corePlugin)
}

// Use this method to register core plugins
func (p *taskPluginRegistry) RegisterCorePlugin(info core.PluginEntry) {
	pc, file, line, _ := runtime.Caller(1)
	funcName := runtime.FuncForPC(pc).Name()
	logger.Infof(context.TODO(), "@@@ RegisterCorePlugin was called by file [%v] [%v]:[%v]", file, funcName, line)

	logger.Infof(context.TODO(), "@@@ Registering core plugin Info ID [%v]", info.ID)
	logger.Infof(context.TODO(), "@@@ Registering core plugin Info RegisteredTaskTypes [%v]", info.RegisteredTaskTypes)
	logger.Infof(context.TODO(), "@@@ Registering core plugin Info LoadPlugin [%v]", info.LoadPlugin)
	logger.Infof(context.TODO(), "@@@ Registering core plugin Info [%v]", info)
	if info.ID == "" {
		logger.Panicf(context.TODO(), "ID is required attribute for k8s plugin")
	}
	if len(info.RegisteredTaskTypes) == 0 {
		logger.Panicf(context.TODO(), "AsyncPlugin should be registered to handle atleast one task type")
	}
	if info.LoadPlugin == nil {
		logger.Panicf(context.TODO(), "PluginLoader cannot be nil")
	}

	p.m.Lock()
	defer p.m.Unlock()
	p.corePlugin = append(p.corePlugin, info)
	logger.Infof(context.TODO(), "@@@ p.corePlugin [%v]", p.corePlugin)
}

// Returns a snapshot of all the registered core plugins.
func (p *taskPluginRegistry) GetCorePlugins() []core.PluginEntry {
	p.m.Lock()
	defer p.m.Unlock()
	return append(p.corePlugin[:0:0], p.corePlugin...)
}

// Returns a snapshot of all registered K8s plugins
func (p *taskPluginRegistry) GetK8sPlugins() []k8s.PluginEntry {
	p.m.Lock()
	defer p.m.Unlock()
	return append(p.k8sPlugin[:0:0], p.k8sPlugin...)
}

type TaskPluginRegistry interface {
	RegisterK8sPlugin(info k8s.PluginEntry)
	RegisterCorePlugin(info core.PluginEntry)
	RegisterRemotePlugin(info webapi.PluginEntry)
	// RegisterRemoteSyncPlugin(info webapi.PluginEntry)
	GetCorePlugins() []core.PluginEntry
	GetK8sPlugins() []k8s.PluginEntry
}
