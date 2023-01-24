package databricks

import (
	"time"

	pluginsConfig "github.com/flyteorg/flyteplugins/go/tasks/config"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/webapi"
	"github.com/flyteorg/flytestdlib/config"
)

var (
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
	}

	configSection = pluginsConfig.MustRegisterSubSection("dummy", &defaultConfig)
)

// Config is config for 'databricks' plugin
type Config struct {
	// WebAPI defines config for the base WebAPI plugin
	WebAPI webapi.PluginConfig `json:"webApi" pflag:",Defines config for the base WebAPI plugin."`

	// ResourceConstraints defines resource constraints on how many executions to be created per project/overall at any given time
	ResourceConstraints core.ResourceConstraintsSpec `json:"resourceConstraints" pflag:"-,Defines resource constraints on how many executions to be created per project/overall at any given time."`
}

func GetConfig() *Config {
	return configSection.GetConfig().(*Config)
}

func SetConfig(cfg *Config) error {
	return configSection.SetConfig(cfg)
}
