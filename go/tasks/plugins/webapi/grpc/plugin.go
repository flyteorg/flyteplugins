package grpc

import (
	"context"
	"encoding/gob"
	"fmt"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/ioutils"

	flyteIdl "github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/service"
	pluginErrors "github.com/flyteorg/flyteplugins/go/tasks/errors"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	pluginsCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/webapi"
	"github.com/flyteorg/flytestdlib/promutils"
	"google.golang.org/grpc"
)

type GetClientFunc func(endpoint string) (service.BackendPluginServiceClient, *grpc.ClientConn, error)

type Plugin struct {
	metricScope promutils.Scope
	cfg         *Config
	getClient   GetClientFunc
}

type ResourceWrapper struct {
	State   service.State
	Message string
	Outputs *flyteIdl.LiteralMap
}

type ResourceMetaWrapper struct {
	OutputPrefix string
	Token        string
	JobID        string
	TaskType     string
}

func (p Plugin) GetConfig() webapi.PluginConfig {
	return GetConfig().WebAPI
}

func (p Plugin) ResourceRequirements(_ context.Context, _ webapi.TaskExecutionContextReader) (
	namespace core.ResourceNamespace, constraints core.ResourceConstraintsSpec, err error) {

	// Resource requirements are assumed to be the same.
	return "default", p.cfg.ResourceConstraints, nil
}

func (p Plugin) Create(ctx context.Context, taskCtx webapi.TaskExecutionContextReader) (webapi.ResourceMeta,
	webapi.Resource, error) {
	taskTemplate, err := taskCtx.TaskReader().Read(ctx)
	if err != nil {
		return nil, nil, err
	}
	inputs, err := taskCtx.InputReader().Get(ctx)
	if err != nil {
		return nil, nil, err
	}

	outputPrefix := taskCtx.OutputWriter().GetOutputPrefixPath().String()

	client, conn, err := p.getClient(getFinalEndpoint(taskTemplate.Type, p.cfg.DefaultGrpcEndpoint, p.cfg.EndpointForTaskTypes))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect flyteplugins service")
	}
	if conn != nil {
		defer conn.Close()
	}

	res, err := client.CreateTask(ctx, &service.TaskCreateRequest{Inputs: inputs, Template: taskTemplate, OutputPrefix: outputPrefix})
	if err != nil {
		return nil, nil, err
	}

	return &ResourceMetaWrapper{
		OutputPrefix: outputPrefix,
		JobID:        res.JobId,
		Token:        "",
		TaskType:     taskTemplate.Type,
	}, &ResourceWrapper{State: service.State_RUNNING}, nil
}

func (p Plugin) Get(ctx context.Context, taskCtx webapi.GetContext) (latest webapi.Resource, err error) {
	metadata := taskCtx.ResourceMeta().(*ResourceMetaWrapper)

	client, conn, err := p.getClient(getFinalEndpoint(metadata.TaskType, p.cfg.DefaultGrpcEndpoint, p.cfg.EndpointForTaskTypes))
	if err != nil {
		return nil, fmt.Errorf("failed to connect flyteplugins service")
	}
	if conn != nil {
		defer conn.Close()
	}

	res, err := client.GetTask(ctx, &service.TaskGetRequest{TaskType: metadata.TaskType, JobId: metadata.JobID})
	if err != nil {
		return nil, err
	}

	return &ResourceWrapper{
		State:   res.State,
		Message: res.Message,
		Outputs: res.Outputs,
	}, nil
}

func (p Plugin) Delete(ctx context.Context, taskCtx webapi.DeleteContext) error {
	if taskCtx.ResourceMeta() == nil {
		return nil
	}
	metadata := taskCtx.ResourceMeta().(ResourceMetaWrapper)

	client, conn, err := p.getClient(getFinalEndpoint(metadata.TaskType, p.cfg.DefaultGrpcEndpoint, p.cfg.EndpointForTaskTypes))
	if err != nil {
		return fmt.Errorf("failed to connect flyteplugins service")
	}
	if conn != nil {
		defer conn.Close()
	}
	_, err = client.DeleteTask(ctx, &service.TaskDeleteRequest{TaskType: metadata.TaskType, JobId: metadata.JobID})
	return err
}

func (p Plugin) Status(ctx context.Context, taskCtx webapi.StatusContext) (phase core.PhaseInfo, err error) {
	resource := taskCtx.Resource().(*ResourceWrapper)
	taskInfo := &core.TaskInfo{}

	switch resource.State {
	case service.State_RUNNING:
		return core.PhaseInfoRunning(pluginsCore.DefaultPhaseVersion, taskInfo), nil
	case service.State_FAILED:
		return core.PhaseInfoFailure(resource.Message, "failed to run the job", taskInfo), nil
	case service.State_SUCCEEDED:
		if resource.Outputs != nil {
			err := taskCtx.OutputWriter().Put(ctx, ioutils.NewInMemoryOutputReader(resource.Outputs, nil, nil))
			if err != nil {
				return core.PhaseInfoUndefined, err
			}
		}
		return core.PhaseInfoSuccess(taskInfo), nil
	}
	return core.PhaseInfoUndefined, pluginErrors.Errorf(pluginsCore.SystemErrorCode, "unknown execution phase [%v].", resource.Message)
}

func getFinalEndpoint(taskType, defaultEndpoint string, endpointForTaskTypes map[string]string) string {
	if t, exists := endpointForTaskTypes[taskType]; exists {
		return t
	}

	return defaultEndpoint
}

func getClientFunc(endpoint string) (service.BackendPluginServiceClient, *grpc.ClientConn, error) {
	// for mocking/testing purposes
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(endpoint, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect flyteplugins service")
	}
	return service.NewBackendPluginServiceClient(conn), conn, nil
}

func newGrpcPlugin() webapi.PluginEntry {
	return webapi.PluginEntry{
		ID:                 "flyteplugins-service",
		SupportedTaskTypes: []core.TaskType{"bigquery_query_job_task"},
		PluginLoader: func(ctx context.Context, iCtx webapi.PluginSetupContext) (webapi.AsyncPlugin, error) {
			return &Plugin{
				metricScope: iCtx.MetricsScope(),
				cfg:         GetConfig(),
				getClient:   getClientFunc,
			}, nil
		},
	}
}

func init() {
	gob.Register(ResourceMetaWrapper{})
	gob.Register(ResourceWrapper{})

	pluginmachinery.PluginRegistry().RegisterRemotePlugin(newGrpcPlugin())
}
