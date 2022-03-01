package config

import (
	"github.com/flyteorg/flytestdlib/config"
)

//go:generate pflags Config

const configSectionKey = "plugins"

var (
	defaultConfig = &Config{
		EnabledPlugins: []string{"*"},
	}

	// Root config section. If you are a plugin developer and your plugin needs a config, you should register
	// your config as a subsection for this root section.
	rootSection = config.MustRegisterSection(configSectionKey, defaultConfig)
)

// Top level plugins config.
type Config struct {
	EnabledPlugins []string `json:"enabled-plugins" pflag:",Deprecated - List of enabled plugins, default value is to enable all plugins."`
}

// Retrieves the current config value or default.
func GetConfig() *Config {
	return rootSection.GetConfig().(*Config)
}

func MustRegisterSubSection(subSectionKey string, section config.Config) config.Section {
	return rootSection.MustRegisterSection(subSectionKey, section)
}
