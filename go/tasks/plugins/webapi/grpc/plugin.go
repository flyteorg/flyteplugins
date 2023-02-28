package grpc

import (
	"context"
	"encoding/gob"
	"fmt"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/service"
	pluginErrors "github.com/flyteorg/flyteplugins/go/tasks/errors"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	pluginsCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/webapi"
	"github.com/flyteorg/flytestdlib/promutils"
	"google.golang.org/grpc"
)

type Plugin struct {
	metricScope promutils.Scope
	cfg         *Config
}

type ResourceWrapper struct {
	State   service.State
	Message string
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

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(p.cfg.grpcEndpoint, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect backend plugin system")
	}
	defer conn.Close()

	client := service.NewBackendPluginServiceClient(conn)
	t := taskTemplate.Type
	taskTemplate.Type = "dummy" // Dummy plugin is used to test performance
	res, err := client.CreateTask(ctx, &service.TaskCreateRequest{Inputs: inputs, Template: taskTemplate, OutputPrefix: outputPrefix})
	taskTemplate.Type = t
	if err != nil {
		return nil, nil, err
	}

	return &ResourceMetaWrapper{
		OutputPrefix: outputPrefix,
		JobID:        res.JobId,
		Token:        "",
		TaskType:     "dummy",
	}, &ResourceWrapper{State: service.State_RUNNING}, nil
}

func (p Plugin) Get(ctx context.Context, taskCtx webapi.GetContext) (latest webapi.Resource, err error) {
	metadata := taskCtx.ResourceMeta().(*ResourceMetaWrapper)
	prevState := service.State_RUNNING
	if taskCtx.Resource() != nil {
		resource := taskCtx.Resource().(*ResourceWrapper)
		prevState = resource.State
	}

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(p.cfg.grpcEndpoint, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect backend plugin system")
	}
	defer conn.Close()

	client := service.NewBackendPluginServiceClient(conn)
	res, err := client.GetTask(ctx, &service.TaskGetRequest{TaskType: metadata.TaskType, JobId: metadata.JobID, OutputPrefix: metadata.OutputPrefix, PrevState: prevState})
	if err != nil {
		return nil, err
	}

	return &ResourceWrapper{
		State:   res.State,
		Message: res.Message,
	}, nil
}

func (p Plugin) Delete(ctx context.Context, taskCtx webapi.DeleteContext) error {
	if taskCtx.ResourceMeta() == nil {
		return nil
	}
	metadata := taskCtx.ResourceMeta().(ResourceMetaWrapper)

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(p.cfg.grpcEndpoint, opts...)
	if err != nil {
		return fmt.Errorf("failed to  connect backend plugin system")
	}
	defer conn.Close()
	client := service.NewBackendPluginServiceClient(conn)
	_, err = client.DeleteTask(ctx, &service.TaskDeleteRequest{TaskType: metadata.TaskType, JobId: metadata.JobID})
	return err
}

func (p Plugin) Status(_ context.Context, taskCtx webapi.StatusContext) (phase core.PhaseInfo, err error) {
	resource := taskCtx.Resource().(*ResourceWrapper)
	taskInfo := &core.TaskInfo{}

	switch resource.State {
	case service.State_RUNNING:
		return core.PhaseInfoRunning(pluginsCore.DefaultPhaseVersion, taskInfo), nil
	case service.State_FAILED:
		return core.PhaseInfoFailure(resource.Message, "failed to run the job", taskInfo), nil
	case service.State_SUCCEEDED:
		return core.PhaseInfoSuccess(taskInfo), nil
	}
	return core.PhaseInfoUndefined, pluginErrors.Errorf(pluginsCore.SystemErrorCode, "unknown execution phase [%v].", resource.Message)
}

func newGrpcPlugin() webapi.PluginEntry {
	return webapi.PluginEntry{
		ID:                 "grpc",
		SupportedTaskTypes: []core.TaskType{"bigquery_query_job_task", "snowflake", "spark"},
		PluginLoader: func(ctx context.Context, iCtx webapi.PluginSetupContext) (webapi.AsyncPlugin, error) {
			return &Plugin{
				metricScope: iCtx.MetricsScope(),
				cfg:         GetConfig(),
			}, nil
		},
	}
}

func init() {
	gob.Register(ResourceMetaWrapper{})
	gob.Register(ResourceWrapper{})

	pluginmachinery.PluginRegistry().RegisterRemotePlugin(newGrpcPlugin())
}
