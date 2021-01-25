package example

import (
	"context"
	"fmt"
	"time"

	"github.com/lyft/flytestdlib/storage"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/ioutils"

	idlCore "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"

	idlAdmin "github.com/lyft/flyteidl/gen/pb-go/flyteidl/admin"
	"github.com/lyft/flytestdlib/errors"
	"github.com/lyft/flytestdlib/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lyft/flyteidl/clients/go/admin"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/service"
	"github.com/lyft/flytestdlib/logger"

	"github.com/lyft/flytestdlib/promutils"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery"
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

func (p Plugin) ResourceRequirements(_ context.Context, _ webapi.TaskExecutionContextReader) (
	namespace core.ResourceNamespace, constraints core.ResourceConstraintsSpec, err error) {

	// Resource requirements are assumed to be the same.
	return "default", p.cfg.ResourceConstraints, nil
}

func (p Plugin) Create(ctx context.Context, tCtx webapi.TaskExecutionContextReader) (resourceMeta webapi.ResourceMeta,
	resource webapi.Resource, err error) {

	// TODO: explain what this block does... and why...
	// TODO: open an issue to add ReadCustom()
	task, err := tCtx.TaskReader().Read(ctx)
	if err != nil {
		return nil, nil, err
	}

	custom := task.GetCustom()
	execCreateRequest := &idlAdmin.ExecutionCreateRequest{}
	err = utils.UnmarshalStructToPb(custom, execCreateRequest)
	if err != nil {
		return nil, nil, err
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
			return &idlAdmin.Execution{Id: lpExec.Id}, nil, nil
		case codes.DataLoss, codes.DeadlineExceeded, codes.Internal, codes.Unknown, codes.Canceled:
			return nil, nil, errors.Wrapf(ErrRemoteSystem, err, "failed to execute Launch Plan [%s], system error", execCreateRequest.Spec.LaunchPlan)
		default:
			return nil, nil, errors.Wrapf(ErrRemoteUser, err, "failed to execute Launch Plan [%s].", execCreateRequest.Spec.LaunchPlan)
		}
	}

	return &idlAdmin.Execution{Id: lpExec.Id}, nil, nil
}

func (p Plugin) Get(ctx context.Context, tCtx webapi.GetContext) (latest webapi.Resource, err error) {
	exec := tCtx.ResourceMeta().(*idlAdmin.Execution)
	newExec, err := p.client.GetExecution(ctx, &idlAdmin.WorkflowExecutionGetRequest{
		Id: exec.Id,
	})

	if err != nil {
		return nil, err
	}

	// Only cache fields we want to keep in memory instead of the potentially huge execution closure.
	exec.Closure = &idlAdmin.ExecutionClosure{
		Phase: newExec.Closure.Phase,
	}

	return exec, nil
}

func (p Plugin) Delete(ctx context.Context, tCtx webapi.DeleteContext) error {
	exec := tCtx.ResourceMeta().(*idlAdmin.Execution)
	_, err := p.client.TerminateExecution(ctx, &idlAdmin.ExecutionTerminateRequest{
		Id:    exec.Id,
		Cause: tCtx.Reason(),
	})

	return err
}

func (p Plugin) Status(ctx context.Context, tCtx webapi.StatusContext) (phase core.PhaseInfo, err error) {
	exec := tCtx.Resource().(*idlAdmin.Execution)
	if exec.Closure == nil {
		return core.PhaseInfoUndefined, nil
	}

	switch exec.Closure.Phase {
	case idlCore.WorkflowExecution_UNDEFINED:
		return core.PhaseInfoUndefined, nil
	case idlCore.WorkflowExecution_QUEUED:
		return core.PhaseInfoQueued(time.Now(), 0, "Queued"), nil
	case idlCore.WorkflowExecution_ABORTED:
		if remoteErr := exec.Closure.GetAbortMetadata(); remoteErr != nil {
			return core.PhaseInfoRetryableFailure("ABORTED", remoteErr.Cause, createTaskInfo(exec, p.cfg.AdminProtocolAndHost)), nil
		}

		return core.PhaseInfoRetryableFailure("ABORTED", "Remote execution was aborted.", createTaskInfo(exec, p.cfg.AdminProtocolAndHost)), nil
	case idlCore.WorkflowExecution_FAILED:
		if remoteErr := exec.Closure.GetError(); remoteErr != nil {
			switch remoteErr.Kind {
			case idlCore.ExecutionError_SYSTEM:
				return core.PhaseInfoSystemRetryableFailure(remoteErr.Code, remoteErr.Message, createTaskInfo(exec, p.cfg.AdminProtocolAndHost)), nil
			default:
				return core.PhaseInfoRetryableFailure(remoteErr.Code, remoteErr.Message, createTaskInfo(exec, p.cfg.AdminProtocolAndHost)), nil
			}
		}

		return core.PhaseInfoRetryableFailure("FAILED", "Remote execution failed", createTaskInfo(exec, p.cfg.AdminProtocolAndHost)), nil
	case idlCore.WorkflowExecution_SUCCEEDED:
		o := exec.Closure.GetOutputs()
		if o.GetValues() != nil {
			if err := tCtx.OutputWriter().Put(ctx, ioutils.NewInMemoryOutputReader(o.GetValues(), nil)); err != nil {
				return core.PhaseInfoUndefined, err
			}

			return core.PhaseInfoSuccess(createTaskInfo(exec, p.cfg.AdminProtocolAndHost)), nil
		} else if len(o.GetUri()) > 0 {
			uri := o.GetUri()
			store := tCtx.DataStore()
			err := store.CopyRaw(ctx, storage.DataReference(uri), tCtx.OutputWriter().GetOutputPath(), storage.Options{})
			if err != nil {
				logger.Warnf(ctx, "Failed to remote output for launchplan execution was not found, uri [%s], err %s", uri, err.Error())
				return core.PhaseInfoUndefined, err
			}

			if err := tCtx.OutputWriter().Put(ctx, ioutils.NewRemoteFileOutputReader(ctx, tCtx.DataStore(),
				tCtx.OutputWriter(), tCtx.MaxDatasetSizeBytes())); err != nil {
				return core.PhaseInfoUndefined, err
			}

			return core.PhaseInfoSuccess(createTaskInfo(exec, p.cfg.AdminProtocolAndHost)), nil
		}

		// Execution succeeded but didn't produce outputs.
		return core.PhaseInfoSuccess(createTaskInfo(exec, p.cfg.AdminProtocolAndHost)), nil
	case idlCore.WorkflowExecution_FAILING:
		fallthrough
	case idlCore.WorkflowExecution_SUCCEEDING:
		fallthrough
	case idlCore.WorkflowExecution_RUNNING:
		return core.PhaseInfoRunning(0, createTaskInfo(exec, p.cfg.AdminProtocolAndHost)), nil
	}

	return core.PhaseInfoUndefined, errors.Errorf(ErrSystem, "Unknown execution phase [%v].", exec.Closure.Phase)
}

func createTaskInfo(exec *idlAdmin.Execution, protocolAndHost string) *core.TaskInfo {
	return &core.TaskInfo{
		Logs: []*idlCore.TaskLog{
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
	pluginmachinery.PluginRegistry().RegisterRemotePlugin(webapi.PluginEntry{
		ID:                 "flyteadmin",
		SupportedTaskTypes: []core.TaskType{"flytetask", "flytelaunchplan"},
		PluginLoader: func(ctx context.Context, iCtx webapi.PluginSetupContext) (webapi.AsyncPlugin, error) {
			return NewPlugin(ctx, GetConfig(), iCtx.MetricsScope())
		},
		IsDefault:           false,
		DefaultForTaskTypes: []core.TaskType{"flytetask", "flytelaunchplan"},
	})
}

type remoteOutputPaths struct {
	io.OutputFilePaths
}
