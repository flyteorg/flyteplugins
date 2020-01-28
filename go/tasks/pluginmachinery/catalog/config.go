package catalog

import (
	"github.com/lyft/flyteplugins/go/tasks/config"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/workqueue"
)

//go:generate pflags Config --default-var=defaultConfig

var cfgSection = config.MustRegisterSubSection("catalogCache", defaultConfig)

type Config struct {
	Reader ProcessorConfig `json:"reader" pflag:",Catalog reader processor config."`
	Writer ProcessorConfig `json:"writer" pflag:",Catalog writer processor config."`
}

type ProcessorConfig struct {
	Workqueue        workqueue.Config `json:"queue" pflag:",Workqueue config. Make sure the index cache must be big enough to accommodate the biggest array task allowed to run on the system."`
	MaxItemsPerRound int              `json:"itemsPerRound" pflag:",Max number of items to process in each round. Under load, this ensures fairness between different array jobs and avoid head-of-line blocking."`
}

var defaultConfig = &Config{
	Reader: ProcessorConfig{
		Workqueue: workqueue.Config{
			MaxRetries:         3,
			Workers:            10,
			IndexCacheMaxItems: 1000,
		},
		MaxItemsPerRound: 100,
	},
	Writer: ProcessorConfig{
		Workqueue: workqueue.Config{
			MaxRetries:         3,
			Workers:            10,
			IndexCacheMaxItems: 1000,
		},
		MaxItemsPerRound: 100,
	},
}

func GetConfig() *Config {
	return cfgSection.GetConfig().(*Config)
}
