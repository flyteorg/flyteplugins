package grpc

import (
	"time"

	pluginsConfig "github.com/flyteorg/flyteplugins/go/tasks/config"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/webapi"
	"github.com/flyteorg/flytestdlib/config"
)

var (
	grpcTokenKey = "FLYTE_GRPC_TOKEN" // nolint: gosec

	defaultConfig = Config{
		WebAPI: webapi.PluginConfig{
			ResourceQuotas: map[core.ResourceNamespace]int{
				"default": 1000,
			},
			ReadRateLimiter: webapi.RateLimiterConfig{
				Burst: 100,
				QPS:   10,
			},
			WriteRateLimiter: webapi.RateLimiterConfig{
				Burst: 100,
				QPS:   10,
			},
			Caching: webapi.CachingConfig{
				Size:              500000,
				ResyncInterval:    config.Duration{Duration: 30 * time.Second},
				Workers:           10,
				MaxSystemFailures: 5,
			},
			ResourceMeta: nil,
		},
		ResourceConstraints: core.ResourceConstraintsSpec{
			ProjectScopeResourceConstraint: &core.ResourceConstraint{
				Value: 100,
			},
			NamespaceScopeResourceConstraint: &core.ResourceConstraint{
				Value: 50,
			},
		},
		GrpcTokenKey:        grpcTokenKey,
		DefaultGrpcEndpoint: "flyteplugins-service.flyte.svc.cluster.local:8000",
	}

	configSection = pluginsConfig.MustRegisterSubSection("flyteplugins-service", &defaultConfig)
)

// Config is config for 'databricks' plugin
type Config struct {
	// WebAPI defines config for the base WebAPI plugin
	WebAPI webapi.PluginConfig `json:"webApi" pflag:",Defines config for the base WebAPI plugin."`

	// ResourceConstraints defines resource constraints on how many executions to be created per project/overall at any given time
	ResourceConstraints core.ResourceConstraintsSpec `json:"resourceConstraints" pflag:"-,Defines resource constraints on how many executions to be created per project/overall at any given time."`

	GrpcTokenKey string `json:"grpcTokenKey" pflag:",Name of the key where to find grpc access token in the secret manager."`

	DefaultGrpcEndpoint string `json:"defaultGrpcEndpoint" pflag:",The default grpc endpoint of flyteplugins service."`

	// Maps endpoint to their plugin handler. {TaskType: Endpoint}
	EndpointForTaskTypes map[string]string `json:"endpointForTaskTypes" pflag:"-,"`
}

func GetConfig() *Config {
	return configSection.GetConfig().(*Config)
}

func SetConfig(cfg *Config) error {
	return configSection.SetConfig(cfg)
}