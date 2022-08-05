package ray

import (
	pluginsConfig "github.com/flyteorg/flyteplugins/go/tasks/config"
)

//go:generate pflags Config --default-var=defaultConfig

var (
	defaultConfig = Config{
		ShutdownAfterJobFinishes: true,
		TTLSecondsAfterFinished:  3600,
	}

	configSection = pluginsConfig.MustRegisterSubSection("ray", &defaultConfig)
)

// Config is config for 'ray' plugin
type Config struct {
	// ShutdownAfterJobFinishes will determine whether to delete the ray cluster once rayJob succeed or failed
	ShutdownAfterJobFinishes bool `json:"shutdownAfterJobFinishes,omitempty"`

	// TTLSecondsAfterFinished is the TTL to clean up RayCluster.
	// It's only working when ShutdownAfterJobFinishes set to true.
	TTLSecondsAfterFinished int32 `json:"ttlSecondsAfterFinished,omitempty"`
}

func GetConfig() *Config {
	return configSection.GetConfig().(*Config)
}

func SetConfig(cfg *Config) error {
	return configSection.SetConfig(cfg)
}
