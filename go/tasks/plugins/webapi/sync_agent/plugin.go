package sync_agent

import (
	"context"
	"crypto/x509"
	"encoding/gob"
	"fmt"
	"runtime"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/admin"

	flyteIdl "github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/service"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core/template"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/webapi"
	"github.com/flyteorg/flytestdlib/config"
	"github.com/flyteorg/flytestdlib/logger"
	"github.com/flyteorg/flytestdlib/promutils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/grpclog"
)

type GetClientFunc func(ctx context.Context, agent *Agent, connectionCache map[*Agent]*grpc.ClientConn) (service.AsyncAgentServiceClient, error)
type TaskType = string
type SupportedTaskTypes []TaskType

type Plugin struct {
	metricScope     promutils.Scope
	cfg             *Config
	getClient       GetClientFunc
	connectionCache map[*Agent]*grpc.ClientConn
}

// The return value of a plugin's Do method.
type ResourceWrapper struct {
	State   admin.State
	Outputs *flyteIdl.LiteralMap
}

type ResourceMetaWrapper struct {
	OutputPrefix      string
	Token             string
	AgentResourceMeta []byte
	TaskType          string
}

// Not Sure if we need this
func (p Plugin) GetConfig() webapi.PluginConfig {
	return GetConfig().WebAPI
}

func (p Plugin) Do(ctx context.Context, taskCtx webapi.TaskExecutionContextReader) (latest webapi.Resource, err error) {

	// write the resource here
	taskTemplate, err := taskCtx.TaskReader().Read(ctx)
	if err != nil {
		return nil, err
	}

	inputs, err := taskCtx.InputReader().Get(ctx)
	if err != nil {
		return nil, err
	}

	if taskTemplate.GetContainer() != nil {
		templateParameters := template.Parameters{
			TaskExecMetadata: taskCtx.TaskExecutionMetadata(),
			Inputs:           taskCtx.InputReader(),
			OutputPath:       taskCtx.OutputWriter(),
			Task:             taskCtx.TaskReader(),
		}
		modifiedArgs, err := template.Render(ctx, taskTemplate.GetContainer().Args, templateParameters)
		if err != nil {
			return nil, err
		}
		taskTemplate.GetContainer().Args = modifiedArgs
	}

	outputPrefix := taskCtx.OutputWriter().GetOutputPrefixPath().String()
	agent, err := getFinalAgent(taskTemplate.Type, p.cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to find agent agent with error: %v", err)
	}

	client, err := p.getClient(ctx, agent, p.connectionCache)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to agent with error: %v", err)
	}

	finalCtx, cancel := getFinalContext(ctx, "DoTask", agent)

	defer cancel()

	// taskExecutionMetadata := buildTaskExecutionMetadata(taskCtx.TaskExecutionMetadata())
	// write it in agent?

	res, err := client.DoTask(finalCtx, &admin.DoTaskRequest{Inputs: inputs, Template: taskTemplate, OutputPrefix: outputPrefix})

	if err != nil {
		return nil, err
	}

	return &ResourceWrapper{
		State:   res.Resource.State,
		Outputs: res.Resource.Outputs,
	}, nil

}

func getClientFunc(ctx context.Context, agent *Agent, connectionCache map[*Agent]*grpc.ClientConn) (service.AsyncAgentServiceClient, error) {
	pc, file, line, _ := runtime.Caller(1)
	funcName := runtime.FuncForPC(pc).Name()
	logger.Infof(ctx, "@@@ getClientFunc was called by [%v] [%v]:[%v]", file, funcName, line)

	conn, ok := connectionCache[agent]
	if ok {
		return service.NewAsyncAgentServiceClient(conn), nil
	}

	var opts []grpc.DialOption

	if agent.Insecure {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		pool, err := x509.SystemCertPool()
		if err != nil {
			return nil, err
		}

		creds := credentials.NewClientTLSFromCert(pool, "")
		opts = append(opts, grpc.WithTransportCredentials(creds))
	}

	if len(agent.DefaultServiceConfig) != 0 {
		opts = append(opts, grpc.WithDefaultServiceConfig(agent.DefaultServiceConfig))
	}

	var err error
	conn, err = grpc.Dial(agent.Endpoint, opts...)
	if err != nil {
		return nil, err
	}
	connectionCache[agent] = conn
	defer func() {
		if err != nil {
			if cerr := conn.Close(); cerr != nil {
				grpclog.Infof("Failed to close conn to %s: %v", agent, cerr)
			}
			return
		}
		go func() {
			<-ctx.Done()
			if cerr := conn.Close(); cerr != nil {
				grpclog.Infof("Failed to close conn to %s: %v", agent, cerr)
			}
		}()
	}()
	return service.NewAsyncAgentServiceClient(conn), nil
}

func getFinalAgent(taskType string, cfg *Config) (*Agent, error) {
	if id, exists := cfg.AgentForTaskTypes[taskType]; exists {
		if agent, exists := cfg.Agents[id]; exists {
			return agent, nil
		}
		return nil, fmt.Errorf("no agent definition found for ID %s that matches task type %s", id, taskType)
	}

	return &cfg.DefaultAgent, nil
}

func getFinalTimeout(operation string, agent *Agent) config.Duration {
	if t, exists := agent.Timeouts[operation]; exists {
		return t
	}

	return agent.DefaultTimeout
}

func getFinalContext(ctx context.Context, operation string, agent *Agent) (context.Context, context.CancelFunc) {
	timeout := getFinalTimeout(operation, agent).Duration
	if timeout == 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func newSyncAgentPlugin(supportedTaskTypes SupportedTaskTypes) webapi.SyncPluginEntry {
	pc, file, line, _ := runtime.Caller(1)
	funcName := runtime.FuncForPC(pc).Name()
	logger.Infof(context.TODO(), "@@@ newSyncAgentPlugin was called by file [%v] [%v]:[%v]", file, funcName, line)
	/*
		TODO: USE tasks: default-for-task-types: INSTEAD
	*/

	logger.Infof(context.TODO(), "@@@ newSyncAgentPlugin():[%v]", GetConfig())
	logger.Infof(context.TODO(), "@@@ supportedTaskTypes:[%v]", supportedTaskTypes)

	if len(supportedTaskTypes) == 0 {
		supportedTaskTypes = SupportedTaskTypes{"default_supported_task_type"}
	}

	return webapi.PluginEntry{
		ID:                 "agent-service",
		SupportedTaskTypes: supportedTaskTypes,
		PluginLoader: func(ctx context.Context, iCtx webapi.PluginSetupContext) (webapi.AsyncPlugin, webapi.SyncPlugin, error) {
			return nil, &Plugin{
				metricScope:     iCtx.MetricsScope(),
				cfg:             GetConfig(),
				getClient:       getClientFunc,
				connectionCache: make(map[*Agent]*grpc.ClientConn),
			}, nil
		},
	}
}

func RegisterSyncAgentPlugin(supportedTaskTypes SupportedTaskTypes) {
	pc, file, line, _ := runtime.Caller(1)
	funcName := runtime.FuncForPC(pc).Name()
	logger.Infof(context.TODO(), "@@@ RegisterSyncAgentPlugin was called by file [%v] [%v]:[%v]", file, funcName, line)

	gob.Register(ResourceMetaWrapper{})
	gob.Register(ResourceWrapper{})

	pluginmachinery.PluginRegistry().RegisterRemotePlugin(newSyncAgentPlugin(supportedTaskTypes))
}
