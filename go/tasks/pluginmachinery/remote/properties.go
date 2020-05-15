package remote

import (
	"time"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flytestdlib/config"
)

//go:generate enumer -type=MissingResourcePolicy -trimprefix=MissingResourcePolicy -json

//go:generate pflags PluginProperties --default-var=DefaultPluginProperties

var (
	DefaultPluginProperties = PluginProperties{
		Caching: CachingProperties{
			Size:           100000,
			ResyncInterval: config.Duration{Duration: 20 * time.Second},
			Workers:        10,
		},
		ReadRateLimiter: RateLimiterProperties{
			QPS:   30,
			Burst: 300,
		},
		WriteRateLimiter: RateLimiterProperties{
			QPS:   20,
			Burst: 200,
		},
	}
)

// The plugin manager automatically queries the remote API
type RateLimiterProperties struct {
	// Queries per second from one process to the remote service
	QPS int `json:"qps" pflag:",Defines the max rate of calls per second."`

	// Maximum burst size
	Burst int `json:"burst" pflag:",Defines the maximum burst size."`
}

type CachingProperties struct {
	// Max number of Resource's to be stored in the local cache
	Size int `json:"size" pflag:",Defines the maximum number of items to cache."`

	// How often to query for objects in remote service.
	ResyncInterval config.Duration `json:"resyncInterval" pflag:",Defines the sync interval."`

	// Workers control how many parallel workers should start up to retrieve updates
	// about resources.
	Workers int `json:"workers" pflag:",Defines the number of workers to start up to process items."`
}

type ResourceQuotas map[core.ResourceNamespace]int

// Properties that help the system optimize itself to handle the specific plugin
type PluginProperties struct {
	// ResourceQuotas allows the plgin to register resources' quotas to ensure the system
	// comply with restrictions in the remote service.
	ResourceQuotas   ResourceQuotas        `json:"resourceQuotas" pflag:"-,Defines resource quotas."`
	ReadRateLimiter  RateLimiterProperties `json:"readRateLimiter" pflag:",Defines rate limiter properties for read actions (e.g. retrieve status)."`
	WriteRateLimiter RateLimiterProperties `json:"writeRateLimiter" pflag:",Defines rate limiter properties for write actions."`
	Caching          CachingProperties     `json:"caching" pflag:",Defines caching characteristics."`
	// Gets an empty copy for the custom state that can be used in ResourceMeta when
	// interacting with the remote service.
	ResourceMeta ResourceMeta `json:"resourceMeta"`
}
