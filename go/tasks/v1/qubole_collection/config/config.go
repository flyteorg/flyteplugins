package config

//go:generate pflags Config

import (
	pluginsConfig "github.com/lyft/flyteplugins/go/tasks/v1/config"
)

const quboleConfigSectionKey = "qubole_collection"

var (
	defaultConfig = Config{
		QuboleLimit:  50,
		LruCacheSize: 1000,
	}

	quboleConfigSection = pluginsConfig.MustRegisterSubSection(quboleConfigSectionKey, &defaultConfig)
)

// Qubole plugin configs
type Config struct {
	QuboleTokenPath string `json:"quboleTokenPath" pflag:",Where to find the Qubole secret"`
	QuboleLimit     int    `json:"quboleLimit" pflag:",Global limit for concurrent Qubole queries"`
	LruCacheSize    int    `json:"lruCacheSize" pflag:",Size of the AutoRefreshCache"`
}

// Retrieves the current config value or default.
func GetQuboleConfig() *Config {
	return quboleConfigSection.GetConfig().(*Config)
}

func SetQuboleConfig(cfg *Config) error {
	return quboleConfigSection.SetConfig(cfg)
}
