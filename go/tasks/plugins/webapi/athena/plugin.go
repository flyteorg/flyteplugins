package athena

import (
	"context"
	"fmt"

	pluginsIdl "github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"

	awsSdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/lyft/flyteplugins/go/tasks/aws"

	"github.com/lyft/flytestdlib/storage"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/ioutils"

	idlCore "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/lyft/flytestdlib/errors"
	"github.com/lyft/flytestdlib/utils"

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
	client      *athena.Client
	cfg         *Config
	awsConfig   *aws.Config
}

type ResourceWrapper struct {
	Status               *athena.QueryExecutionStatus
	ResultsConfiguration *athena.ResultConfiguration
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
	hiveQuery := &pluginsIdl.QuboleHiveJob{}
	err = utils.UnmarshalStructToPb(custom, hiveQuery)
	if err != nil {
		return nil, nil, err
	}

	if hiveQuery.Query == nil {
		return "", "", nil
	}

	execID := tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetID().NodeExecutionId.GetExecutionId()
	request := p.client.CreateNamedQueryRequest(&athena.CreateNamedQueryInput{
		ClientRequestToken: awsSdk.String(tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()),
		Database:           awsSdk.String(hiveQuery.ClusterLabel),
		Description:        awsSdk.String(fmt.Sprintf("Launched query through Athena Plugin for execution [%v]", execID)),
		Name:               awsSdk.String(tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()),
		QueryString:        awsSdk.String(hiveQuery.Query.Query),
	})

	resp, err := request.Send(ctx)
	if err != nil {
		return "", "", err
	}

	if resp.NamedQueryId == nil {
		return "", "", errors.Errorf(ErrRemoteSystem, "Service created an empty query id")
	}

	return *resp.NamedQueryId, nil, nil
}

func (p Plugin) Get(ctx context.Context, tCtx webapi.GetContext) (latest webapi.Resource, err error) {
	exec := tCtx.ResourceMeta().(string)
	request := p.client.GetQueryExecutionRequest(&athena.GetQueryExecutionInput{
		QueryExecutionId: awsSdk.String(exec),
	})

	resp, err := request.Send(ctx)
	if err != nil {
		return nil, err
	}

	// Only cache fields we want to keep in memory instead of the potentially huge execution closure.
	return ResourceWrapper{
		Status:               resp.QueryExecution.Status,
		ResultsConfiguration: resp.QueryExecution.ResultConfiguration,
	}, nil
}

func (p Plugin) Delete(ctx context.Context, tCtx webapi.DeleteContext) error {
	request := p.client.DeleteNamedQueryRequest(&athena.DeleteNamedQueryInput{
		NamedQueryId: awsSdk.String(tCtx.ResourceMeta().(string)),
	})

	resp, err := request.Send(ctx)
	if err != nil {
		return err
	}

	logger.Info(ctx, "Deleted named query [%v]", resp.String())

	return nil
}

func (p Plugin) Status(ctx context.Context, tCtx webapi.StatusContext) (phase core.PhaseInfo, err error) {
	execID := tCtx.ResourceMeta().(string)
	exec := tCtx.Resource().(ResourceWrapper)
	if exec.Status == nil {
		return core.PhaseInfoUndefined, errors.Errorf(ErrSystem, "No Status field set.")
	}

	switch exec.Status.State {
	case athena.QueryExecutionStateQueued:
		fallthrough
	case athena.QueryExecutionStateRunning:
		return core.PhaseInfoRunning(0, createTaskInfo(execID, p.awsConfig.GetSdkConfig())), nil
	case athena.QueryExecutionStateCancelled:
		reason := "Remote execution was aborted."
		if reasonPtr := exec.Status.StateChangeReason; reasonPtr != nil {
			reason = *reasonPtr
		}

		return core.PhaseInfoRetryableFailure("ABORTED", reason, createTaskInfo(execID, p.awsConfig.GetSdkConfig())), nil
	case athena.QueryExecutionStateFailed:
		reason := "Remote execution failed"
		if reasonPtr := exec.Status.StateChangeReason; reasonPtr != nil {
			reason = *reasonPtr
		}

		return core.PhaseInfoRetryableFailure("FAILED", reason, createTaskInfo(execID, p.awsConfig.GetSdkConfig())), nil
	case athena.QueryExecutionStateSucceeded:
		if outputLocation := exec.ResultsConfiguration.OutputLocation; outputLocation != nil {
			// If WorkGroup settings overrode the client settings, the location submitted in the request might have been
			// ignored.
			if *outputLocation != tCtx.OutputWriter().GetOutputPath().String() {
				uri := *outputLocation
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
			}
		}

		return core.PhaseInfoSuccess(createTaskInfo(execID, p.awsConfig.GetSdkConfig())), nil
	}

	return core.PhaseInfoUndefined, errors.Errorf(ErrSystem, "Unknown execution phase [%v].", exec.Status.State)
}

func createTaskInfo(queryID string, cfg awsSdk.Config) *core.TaskInfo {
	return &core.TaskInfo{
		Logs: []*idlCore.TaskLog{
			{
				Uri: fmt.Sprintf("https://%v.console.aws.amazon.com/athena/home?force&region=%v#query/history/%v",
					cfg.Region,
					cfg.Region,
					queryID),
				Name: "Athena Query History",
			},
		},
	}
}

func NewPlugin(_ context.Context, cfg *Config, awsConfig *aws.Config, metricScope promutils.Scope) (Plugin, error) {
	return Plugin{
		metricScope: metricScope,
		client:      athena.New(awsConfig.GetSdkConfig()),
		cfg:         cfg,
		awsConfig:   awsConfig,
	}, nil
}

func init() {
	pluginmachinery.PluginRegistry().RegisterRemotePlugin(webapi.PluginEntry{
		ID:                 "athena",
		SupportedTaskTypes: []core.TaskType{"hive"},
		PluginLoader: func(ctx context.Context, iCtx webapi.PluginSetupContext) (webapi.AsyncPlugin, error) {
			return NewPlugin(ctx, GetConfig(), aws.GetConfig(), iCtx.MetricsScope())
		},
	})
}
