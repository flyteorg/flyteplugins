package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/flyteorg/flyteidl/clients/go/coreutils"
	flyteIdlCore "github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/plugins"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/service"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	pluginCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	pluginCoreMocks "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core/mocks"
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
	return &service.TaskCreateResponse{JobId: "job-id", Message: "succeed"}, nil
}

func (m *MockClient) GetTask(_ context.Context, _ *service.TaskGetRequest, _ ...grpc.CallOption) (*service.TaskGetResponse, error) {
	return &service.TaskGetResponse{State: service.State_SUCCEEDED, Message: "succeed"}, nil
}

func (m *MockClient) DeleteTask(_ context.Context, _ *service.TaskDeleteRequest, _ ...grpc.CallOption) (*service.TaskDeleteResponse, error) {
	return &service.TaskDeleteResponse{}, nil
}

func mockGetClientFunc(_ string) (service.BackendPluginServiceClient, *grpc.ClientConn, error) {
	return &MockClient{}, nil, nil
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

	pluginEntry := pluginmachinery.CreateRemotePlugin(newMockGrpcPlugin())
	plugin, err := pluginEntry.LoadPlugin(context.TODO(), newFakeSetupContext())
	assert.NoError(t, err)

	t.Run("run a job", func(t *testing.T) {
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

		phase := tests.RunPluginEndToEndTest(t, plugin, &template, inputs, nil, nil, iter)
		assert.Equal(t, true, phase.Phase().IsSuccess())
	})
}

func newMockGrpcPlugin() webapi.PluginEntry {
	return webapi.PluginEntry{
		ID:                 "flyteplugins-service",
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
