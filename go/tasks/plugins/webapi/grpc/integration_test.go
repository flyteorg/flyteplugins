package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	ioMocks "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/io/mocks"
	"github.com/flyteorg/flytestdlib/storage"
	"k8s.io/apimachinery/pkg/util/rand"

	flyteIdlCore "github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	pluginCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	pluginCoreMocks "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core/mocks"

	"github.com/flyteorg/flyteidl/clients/go/coreutils"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/plugins"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/service"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/webapi"
	"github.com/flyteorg/flyteplugins/tests"
	"github.com/flyteorg/flytestdlib/contextutils"
	"github.com/flyteorg/flytestdlib/promutils"
	"github.com/flyteorg/flytestdlib/promutils/labeled"
	"github.com/flyteorg/flytestdlib/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

type MockPlugin struct {
	Plugin
}

type MockClient struct {
}

func (m *MockClient) CreateTask(_ context.Context, _ *service.TaskCreateRequest, _ ...grpc.CallOption) (*service.TaskCreateResponse, error) {
	return &service.TaskCreateResponse{JobId: "job-id"}, nil
}

func (m *MockClient) GetTask(_ context.Context, _ *service.TaskGetRequest, _ ...grpc.CallOption) (*service.TaskGetResponse, error) {
	return &service.TaskGetResponse{State: service.State_SUCCEEDED, Outputs: &flyteIdlCore.LiteralMap{
		Literals: map[string]*flyteIdlCore.Literal{
			"arr": coreutils.MustMakeLiteral([]interface{}{[]interface{}{"a", "b"}, []interface{}{1, 2}}),
		},
	}}, nil
}

func (m *MockClient) DeleteTask(_ context.Context, _ *service.TaskDeleteRequest, _ ...grpc.CallOption) (*service.TaskDeleteResponse, error) {
	return &service.TaskDeleteResponse{}, nil
}

func mockGetClientFunc(_ context.Context, _ string, _ map[string]*grpc.ClientConn) (service.ExternalPluginServiceClient, error) {
	return &MockClient{}, nil
}

func mockGetBadClientFunc(_ context.Context, _ string, _ map[string]*grpc.ClientConn) (service.ExternalPluginServiceClient, error) {
	return nil, fmt.Errorf("error")
}

func TestEndToEnd(t *testing.T) {
	iter := func(ctx context.Context, tCtx pluginCore.TaskExecutionContext) error {
		return nil
	}

	cfg := defaultConfig
	cfg.WebAPI.Caching.Workers = 1
	cfg.WebAPI.Caching.ResyncInterval.Duration = 5 * time.Second
	err := SetConfig(&cfg)
	assert.NoError(t, err)

	databricksConfDict := map[string]interface{}{
		"name": "flytekit databricks plugin example",
		"new_cluster": map[string]string{
			"spark_version": "11.0.x-scala2.12",
			"node_type_id":  "r3.xlarge",
			"num_workers":   "4",
		},
		"timeout_seconds": 3600,
		"max_retries":     1,
	}
	databricksConfig, err := utils.MarshalObjToStruct(databricksConfDict)
	assert.NoError(t, err)
	sparkJob := plugins.SparkJob{DatabricksConf: databricksConfig, DatabricksToken: "token", SparkConf: map[string]string{"spark.driver.bindAddress": "127.0.0.1"}}
	st, err := utils.MarshalPbToStruct(&sparkJob)
	assert.NoError(t, err)

	inputs, _ := coreutils.MakeLiteralMap(map[string]interface{}{"x": 1})
	template := flyteIdlCore.TaskTemplate{
		Type:   "bigquery_query_job_task",
		Custom: st,
	}

	t.Run("run a job", func(t *testing.T) {
		pluginEntry := pluginmachinery.CreateRemotePlugin(newMockGrpcPlugin())
		plugin, err := pluginEntry.LoadPlugin(context.TODO(), newFakeSetupContext())
		assert.NoError(t, err)

		phase := tests.RunPluginEndToEndTest(t, plugin, &template, inputs, nil, nil, iter)
		assert.Equal(t, true, phase.Phase().IsSuccess())
	})

	t.Run("failed to create a job", func(t *testing.T) {
		grpcPlugin := newMockGrpcPlugin()
		grpcPlugin.PluginLoader = func(ctx context.Context, iCtx webapi.PluginSetupContext) (webapi.AsyncPlugin, error) {
			return &MockPlugin{
				Plugin{
					metricScope: iCtx.MetricsScope(),
					cfg:         GetConfig(),
					getClient:   mockGetBadClientFunc,
				},
			}, nil
		}
		grpcPlugin.ID = "bad-plugin"
		pluginEntry := pluginmachinery.CreateRemotePlugin(grpcPlugin)
		plugin, err := pluginEntry.LoadPlugin(context.TODO(), newFakeSetupContext())
		assert.NoError(t, err)

		latestKnownState := atomic.Value{}
		pluginStateReader := &pluginCoreMocks.PluginStateReader{}
		pluginStateReader.OnGetMatch(mock.Anything).Return(0, nil).Run(func(args mock.Arguments) {
			o := args.Get(0)
			x, err := json.Marshal(latestKnownState.Load())
			assert.NoError(t, err)
			assert.NoError(t, json.Unmarshal(x, &o))
		})
		pluginStateWriter := &pluginCoreMocks.PluginStateWriter{}
		pluginStateWriter.OnPutMatch(mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
			latestKnownState.Store(args.Get(1))
		})

		pluginStateWriter.OnReset().Return(nil).Run(func(args mock.Arguments) {
			latestKnownState.Store(nil)
		})

		execID := rand.String(3)
		tID := &pluginCoreMocks.TaskExecutionID{}
		tID.OnGetGeneratedName().Return(execID + "-my-task-1")
		tMeta := &pluginCoreMocks.TaskExecutionMetadata{}
		tMeta.OnGetTaskExecutionID().Return(tID)
		resourceManager := &pluginCoreMocks.ResourceManager{}
		resourceManager.OnAllocateResourceMatch(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(pluginCore.AllocationStatusGranted, nil)
		resourceManager.OnReleaseResourceMatch(mock.Anything, mock.Anything, mock.Anything).Return(nil)
		tr := &pluginCoreMocks.TaskReader{}
		tr.OnRead(context.Background()).Return(&template, nil)

		basePrefix := storage.DataReference("fake://bucket/prefix/" + execID)
		inputReader := &ioMocks.InputReader{}
		inputReader.OnGetInputPrefixPath().Return(basePrefix)
		inputReader.OnGetInputPath().Return(basePrefix + "/inputs.pb")
		inputReader.OnGetMatch(mock.Anything).Return(inputs, nil)

		outputWriter := &ioMocks.OutputWriter{}
		outputWriter.OnGetRawOutputPrefix().Return("/sandbox/")
		outputWriter.OnGetOutputPrefixPath().Return(basePrefix)
		outputWriter.OnGetErrorPath().Return(basePrefix + "/error.pb")
		outputWriter.OnGetOutputPath().Return(basePrefix + "/outputs.pb")
		outputWriter.OnGetCheckpointPrefix().Return("/checkpoint")
		outputWriter.OnGetPreviousCheckpointsPrefix().Return("/prev")

		tCtx := &pluginCoreMocks.TaskExecutionContext{}
		tCtx.OnInputReader().Return(inputReader)
		tCtx.OnOutputWriter().Return(outputWriter)
		tCtx.OnResourceManager().Return(resourceManager)
		tCtx.OnPluginStateReader().Return(pluginStateReader)
		tCtx.OnPluginStateWriter().Return(pluginStateWriter)
		tCtx.OnTaskExecutionMetadata().Return(tMeta)
		tCtx.OnTaskReader().Return(tr)

		tID.OnGetID().Return(flyteIdlCore.TaskExecutionIdentifier{
			TaskId: &flyteIdlCore.Identifier{
				ResourceType: flyteIdlCore.ResourceType_TASK,
				Project:      "a",
				Domain:       "d",
				Name:         "n",
				Version:      "abc",
			},
			NodeExecutionId: &flyteIdlCore.NodeExecutionIdentifier{
				NodeId: "node1",
				ExecutionId: &flyteIdlCore.WorkflowExecutionIdentifier{
					Project: "a",
					Domain:  "d",
					Name:    "exec",
				},
			},
			RetryAttempt: 0,
		})

		trns, err := plugin.Handle(context.Background(), tCtx)
		assert.Nil(t, err)
		assert.NotNil(t, trns)
		trns, err = plugin.Handle(context.Background(), tCtx)
		assert.Error(t, err)
		assert.Equal(t, trns.Info().Phase(), core.PhaseUndefined)
		err = plugin.Abort(context.Background(), tCtx)
		assert.Nil(t, err)
	})
}

func newMockGrpcPlugin() webapi.PluginEntry {
	return webapi.PluginEntry{
		ID:                 "external-plugin-service",
		SupportedTaskTypes: []core.TaskType{"bigquery_query_job_task"},
		PluginLoader: func(ctx context.Context, iCtx webapi.PluginSetupContext) (webapi.AsyncPlugin, error) {
			return &MockPlugin{
				Plugin{
					metricScope: iCtx.MetricsScope(),
					cfg:         GetConfig(),
					getClient:   mockGetClientFunc,
				},
			}, nil
		},
	}
}

func newFakeSetupContext() *pluginCoreMocks.SetupContext {
	fakeResourceRegistrar := pluginCoreMocks.ResourceRegistrar{}
	fakeResourceRegistrar.On("RegisterResourceQuota", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	labeled.SetMetricKeys(contextutils.NamespaceKey)

	fakeSetupContext := pluginCoreMocks.SetupContext{}
	fakeSetupContext.OnMetricsScope().Return(promutils.NewScope("test"))
	fakeSetupContext.OnResourceRegistrar().Return(&fakeResourceRegistrar)

	return &fakeSetupContext
}
