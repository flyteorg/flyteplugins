package spark

import (
	pluginsConfig "github.com/lyft/flyteplugins/go/tasks/config"
	"github.com/lyft/flyteplugins/go/tasks/logs"
)

//go:generate pflags Config --default-var=defaultConfig

var (
	defaultConfig = &Config{
		LogConfig: LogConfig{
			UserLogs: logs.LogConfig{
				IsKubernetesEnabled:   true,
				KubernetesTemplateURI: "http://localhost:30084/#!/log/{{ .namespace }}/{{ .podName }}/pod?namespace={{ .namespace }}",
			},
		},
	}

	sparkConfigSection = pluginsConfig.MustRegisterSubSection("spark", defaultConfig)
)

// Spark-specific configs
type Config struct {
	DefaultSparkConfig    map[string]string `json:"spark-config-default" pflag:"-,Key value pairs of default spark configuration that should be applied to every SparkJob"`
	SparkHistoryServerURL string            `json:"spark-history-server-url" pflag:",URL for SparkHistory Server that each job will publish the execution history to."`
	Features              []Feature         `json:"features" pflag:"-,List of optional features supported."`
	LogConfig             LogConfig         `json:"logs" pflag:",Config for log links for spark applications."`
}

type LogConfig struct {
	UserLogs    logs.LogConfig
	SystemLogs  logs.LogConfig
	AllUserLogs logs.LogConfig
}

// Optional feature with name and corresponding spark-config to use.
type Feature struct {
	Name        string            `json:"name"`
	SparkConfig map[string]string `json:"spark-config"`
}

func GetSparkConfig() *Config {
	return sparkConfigSection.GetConfig().(*Config)
}

// This method should be used for unit testing only
func setSparkConfig(cfg *Config) error {
	return sparkConfigSection.SetConfig(cfg)
}
