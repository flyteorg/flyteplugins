package agent

import (
	"context"
	"testing"
	"time"

	"github.com/flyteorg/flytestdlib/config"

	"google.golang.org/grpc"

	pluginsCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	pluginCoreMocks "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	"github.com/flyteorg/flytestdlib/promutils"
	"github.com/stretchr/testify/assert"
)

func TestPlugin(t *testing.T) {
	fakeSetupContext := pluginCoreMocks.SetupContext{}
	fakeSetupContext.OnMetricsScope().Return(promutils.NewScope("test"))

	plugin := Plugin{
		metricScope: fakeSetupContext.MetricsScope(),
		cfg:         GetConfig(),
	}
	t.Run("get config", func(t *testing.T) {
		cfg := defaultConfig
		cfg.WebAPI.Caching.Workers = 1
		cfg.WebAPI.Caching.ResyncInterval.Duration = 5 * time.Second
		cfg.DefaultGrpcEndpoint = GrpcEndpoint{Endpoint: "test-agent.flyte.svc.cluster.local:80"}
		cfg.EndpointForTaskTypes = map[string]GrpcEndpoint{"spark": {Endpoint: "localhost:80"}}
		err := SetConfig(&cfg)
		assert.NoError(t, err)
		assert.Equal(t, cfg.WebAPI, plugin.GetConfig())
	})
	t.Run("get ResourceRequirements", func(t *testing.T) {
		namespace, constraints, err := plugin.ResourceRequirements(context.TODO(), nil)
		assert.NoError(t, err)
		assert.Equal(t, pluginsCore.ResourceNamespace("default"), namespace)
		assert.Equal(t, plugin.cfg.ResourceConstraints, constraints)
	})

	t.Run("tet newAgentPlugin", func(t *testing.T) {
		p := newAgentPlugin()
		assert.NotNil(t, p)
		assert.Equal(t, p.ID, "agent-service")
		assert.NotNil(t, p.PluginLoader)
	})

	t.Run("test getFinalEndpoint", func(t *testing.T) {
		defaultGrpcEndpoint := GrpcEndpoint{Endpoint: "localhost:8080"}
		endpoint := getFinalEndpoint("spark", defaultGrpcEndpoint, map[string]GrpcEndpoint{"spark": {Endpoint: "localhost:80"}})
		assert.Equal(t, endpoint.Endpoint, "localhost:80")
		endpoint = getFinalEndpoint("spark", defaultGrpcEndpoint, map[string]GrpcEndpoint{})
		assert.Equal(t, endpoint.Endpoint, "localhost:8080")
	})

	t.Run("test getClientFunc", func(t *testing.T) {
		client, err := getClientFunc(context.Background(), GrpcEndpoint{Endpoint: "localhost:80"}, map[string]*grpc.ClientConn{})
		assert.NoError(t, err)
		assert.NotNil(t, client)
	})

	t.Run("test getClientFunc more config", func(t *testing.T) {
		client, err := getClientFunc(context.Background(), GrpcEndpoint{Endpoint: "localhost:80", Insecure: true, DefaultServiceConfig: "{\"loadBalancingConfig\": [{\"round_robin\":{}}]}"}, map[string]*grpc.ClientConn{})
		assert.NoError(t, err)
		assert.NotNil(t, client)
	})

	t.Run("test getFinalTimeout", func(t *testing.T) {
		timeout := getFinalTimeout("CreateTask", GrpcEndpoint{Endpoint: "localhost:8080", Timeouts: map[string]config.Duration{"CreateTask": {Duration: 1 * time.Millisecond}}})
		assert.Equal(t, timeout.Duration, 1*time.Millisecond)
		timeout = getFinalTimeout("DeleteTask", GrpcEndpoint{Endpoint: "localhost:8080", DefaultTimeout: config.Duration{Duration: 10 * time.Second}})
		assert.Equal(t, timeout.Duration, 10*time.Second)
	})
}
