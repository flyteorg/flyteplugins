package example

import (
	"time"

	pluginsConfig "github.com/lyft/flyteplugins/go/tasks/config"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/webapi"
	"github.com/lyft/flytestdlib/config"
)

//go:generate pflags Config --default-var=defaultConfig

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
				Size:           500000,
				ResyncInterval: config.Duration{Duration: 30 * time.Second},
				Workers:        10,
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

		AdminProtocolAndHost: "https://localhost:30084/console/",
	}

	configSection = pluginsConfig.MustRegisterSubSection("admin", &defaultConfig)
)

// TODO: see if this can work with minimal changes
/*
plugins:
  admin:
    resourceQuotas: ...
    adminUrlTemplate: ...
*/

type Config struct {
	WebAPI               webapi.PluginConfig          `json:",inline" pflag:",Defines config for the base WebAPI plugin."`
	ResourceConstraints  core.ResourceConstraintsSpec `json:"resourceConstraints" pflag:"-,Defines resource constraints on how many executions to be created per project/overall at any given time."`
	AdminProtocolAndHost string                       `json:"adminUrlTemplate" pflag:",Defines the user facing url for admin service (e.g. https://myadmin/)"`
}

func GetConfig() *Config {
	return configSection.GetConfig().(*Config)
}
