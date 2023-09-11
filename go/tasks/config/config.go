package config

import (
	"context"

	"github.com/flyteorg/flytestdlib/config"
	"github.com/flyteorg/flytestdlib/logger"
)

const configSectionKey = "plugins"

var (
	// Root config section. If you are a plugin developer and your plugin needs a config, you should register
	// your config as a subsection for this root section.
	rootSection = config.MustRegisterSection(configSectionKey, &Config{})
)

// Top level plugins config.
type Config struct {
}

// Retrieves the current config value or default.
func GetConfig() *Config {
	return rootSection.GetConfig().(*Config)
}

/*
resourcemanager&{noop 1000 {[]    0}}
admin-launcher&{100 10 10000 10}
workflowStore&{ResourceVersionCache}
*/
func MustRegisterSubSection(subSectionKey string, section config.Config) config.Section {
	logger.Error(context.TODO(), "@@@ MustRegisterSubSection subSectionKey->[%v], section->[%v]", subSectionKey, section)
	return rootSection.MustRegisterSection(subSectionKey, section)
}
