package config

import (
	"github.com/lyft/flytestdlib/config"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/utils"
)

//go:generate pflags Config

const configSectionKey = "plugins"

var (
	// Root config section. If you are a plugin developer and your plugin needs a config, you should register
	// your config as a subsection for this root section.
	rootSection = config.MustRegisterSection(configSectionKey, &Config{})
)

// Top level plugins config.
type Config struct {
	EnabledPlugins []string `json:"enabled-plugins" pflag:"[]string{\"*\"},List of enabled plugins, default value is to enable all plugins."`
}

func (c Config) IsEnabled(pluginToCheck string) bool {
	return c.EnabledPlugins != nil && len(c.EnabledPlugins) >= 1 &&
		(c.EnabledPlugins[0] == "*" || utils.Contains(c.EnabledPlugins, pluginToCheck))
}

// Retrieves the current config value or default.
func GetConfig() *Config {
	return rootSection.GetConfig().(*Config)
}

func MustRegisterSubSection(subSectionKey string, section config.Config) config.Section {
	return rootSection.MustRegisterSection(subSectionKey, section)
}
