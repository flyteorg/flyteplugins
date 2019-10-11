package config

//go:generate pflags Config

import (
	pluginsConfig "github.com/lyft/flyteplugins/go/tasks/config"
)

const quboleConfigSectionKey = "qubole"

var (
	defaultConfig = Config{
		QuboleTokenKey: "FLYTE_QUBOLE_CLIENT_TOKEN",
		QuboleLimit:    100,
		LruCacheSize:   2000,
	}

	quboleConfigSection = pluginsConfig.MustRegisterSubSection(quboleConfigSectionKey, &defaultConfig)
)

// Qubole plugin configs
type Config struct {
	QuboleTokenKey string `json:"quboleTokenKey" pflag:",Name of the key where to find Qubole token in the secret manager."`
	QuboleLimit    int    `json:"quboleLimit" pflag:",Global limit for concurrent Qubole queries"`
	LruCacheSize   int    `json:"lruCacheSize" pflag:",Size of the AutoRefreshCache"`
}

// Retrieves the current config value or default.
func GetQuboleConfig() *Config {
	return quboleConfigSection.GetConfig().(*Config)
}

func SetQuboleConfig(cfg *Config) error {
	return quboleConfigSection.SetConfig(cfg)
}
