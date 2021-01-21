package example

import (
	"context"
	"fmt"
	"time"

	core2 "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"

	admin2 "github.com/lyft/flyteidl/gen/pb-go/flyteidl/admin"
	"github.com/lyft/flytestdlib/errors"
	"github.com/lyft/flytestdlib/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lyft/flyteidl/clients/go/admin"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/service"
	"github.com/lyft/flytestdlib/logger"

	"github.com/lyft/flytestdlib/promutils"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery"
	webapi2 "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/internal/webapi"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/webapi"
)

const (
	ErrRemoteSystem errors.ErrorCode = "RemoteSystem"
	ErrRemoteUser   errors.ErrorCode = "RemoteUser"
	ErrSystem       errors.ErrorCode = "System"
)

type Plugin struct {
	metricScope promutils.Scope
	client      service.AdminServiceClient
	cfg         *Config
}

func (p Plugin) GetConfig() webapi.PluginConfig {
	return GetConfig().WebAPI
}

func (p Plugin) ResourceRequirements(_ context.Context, _ webapi.TaskExecutionContext) (
	namespace core.ResourceNamespace, constraints core.ResourceConstraintsSpec, err error) {

	// Resource requirements are assumed to be the same.
	return "default", p.cfg.ResourceConstraints, nil
}

func (p Plugin) Create(ctx context.Context, tCtx webapi.TaskExecutionContext) (resource webapi.ResourceMeta, err error) {
	task, err := tCtx.TaskReader().Read(ctx)
	if err != nil {
		return nil, err
	}

	custom := task.GetCustom()
	execCreateRequest := &admin2.ExecutionCreateRequest{}
	err = utils.UnmarshalStructToPb(custom, execCreateRequest)
	if err != nil {
		return nil, err
	}

	execID := tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetID().NodeExecutionId.GetExecutionId()
	execCreateRequest.Project = execID.Project
	execCreateRequest.Domain = execID.Domain
	execCreateRequest.Name = tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()

	lpExec, err := p.client.CreateExecution(ctx, execCreateRequest)
	if err != nil {
		statusCode := status.Code(err)
		switch statusCode {
		case codes.AlreadyExists:
			return &admin2.Execution{Id: lpExec.Id}, nil
		case codes.DataLoss, codes.DeadlineExceeded, codes.Internal, codes.Unknown, codes.Canceled:
			return nil, errors.Wrapf(ErrRemoteSystem, err, "failed to execute Launch Plan [%s], system error", execCreateRequest.Spec.LaunchPlan)
		default:
			return nil, errors.Wrapf(ErrRemoteUser, err, "failed to execute Launch Plan [%s].", execCreateRequest.Spec.LaunchPlan)
		}
	}

	return &admin2.Execution{Id: lpExec.Id}, nil
}

func (p Plugin) Get(ctx context.Context, cached webapi.ResourceMeta) (latest webapi.ResourceMeta, err error) {
	exec := cached.(*admin2.Execution)
	newExec, err := p.client.GetExecution(ctx, &admin2.WorkflowExecutionGetRequest{
		Id: exec.Id,
	})

	if err != nil {
		return nil, err
	}

	// Only cache fields we want to keep in memory instead of the potentially huge execution closure.
	exec.Closure = &admin2.ExecutionClosure{
		Phase: newExec.Closure.Phase,
	}

	return exec, nil
}

func (p Plugin) Delete(ctx context.Context, cached webapi.ResourceMeta) error {
	exec := cached.(*admin2.Execution)
	_, err := p.client.TerminateExecution(ctx, &admin2.ExecutionTerminateRequest{
		Id:    exec.Id,
		Cause: "WebAPI Called Delete",
	})

	return err
}

func (p Plugin) Status(ctx context.Context, resource webapi.ResourceMeta) (phase core.PhaseInfo, err error) {
	exec := resource.(*admin2.Execution)
	if exec.Closure == nil {
		return core.PhaseInfoUndefined, nil
	}

	switch exec.Closure.Phase {
	case core2.WorkflowExecution_UNDEFINED:
		return core.PhaseInfoUndefined, nil
	case core2.WorkflowExecution_QUEUED:
		return core.PhaseInfoQueued(time.Now(), 0, "Queued"), nil
	case core2.WorkflowExecution_FAILED:
		return core.PhaseInfoRetryableFailure("FAILED", "Remote execution failed", createTaskInfo(exec, p.cfg.AdminProtocolAndHost)), nil
	case core2.WorkflowExecution_SUCCEEDED:
		return core.PhaseInfoSuccess(createTaskInfo(exec, p.cfg.AdminProtocolAndHost)), nil
	case core2.WorkflowExecution_FAILING:
		fallthrough
	case core2.WorkflowExecution_SUCCEEDING:
		fallthrough
	case core2.WorkflowExecution_RUNNING:
		return core.PhaseInfoRunning(0, createTaskInfo(exec, p.cfg.AdminProtocolAndHost)), nil
	}

	return core.PhaseInfoUndefined, errors.Errorf(ErrSystem, "Unknown execution phase [%v].", exec.Closure.Phase)
}

func createTaskInfo(exec *admin2.Execution, protocolAndHost string) *core.TaskInfo {
	return &core.TaskInfo{
		Logs: []*core2.TaskLog{
			{
				Uri:  fmt.Sprintf("%v/projects/%v/domains/%v/executions/%v", protocolAndHost, exec.Id.Project, exec.Id.Domain, exec.Id.Name),
				Name: "Remote Execution",
			},
		},
	}
}

func NewPlugin(ctx context.Context, cfg *Config, metricScope promutils.Scope) (Plugin, error) {
	adminClient, err := admin.InitializeAdminClientFromConfig(ctx)
	if err != nil {
		logger.Errorf(ctx, "failed to initialize Admin client, err :%s", err.Error())
		return Plugin{}, err
	}

	return Plugin{
		metricScope: metricScope,
		client:      adminClient,
		cfg:         cfg,
	}, nil
}

func init() {
	pluginmachinery.PluginRegistry().RegisterCorePlugin(
		webapi2.CreateRemotePlugin(webapi.PluginEntry{
			ID:                 "flyteadmin",
			SupportedTaskTypes: []core.TaskType{"flytetask", "flytelaunchplan"},
			PluginLoader: func(ctx context.Context, iCtx webapi.PluginSetupContext) (webapi.Plugin, error) {
				return NewPlugin(ctx, GetConfig(), iCtx.MetricsScope())
			},
			IsDefault:           false,
			DefaultForTaskTypes: []core.TaskType{"flytetask", "flytelaunchplan"},
		}))
}
