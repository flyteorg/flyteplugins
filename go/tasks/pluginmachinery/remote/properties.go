package remote

import (
	"time"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
)

//go:generate enumer -type=MissingResourcePolicy -trimprefix=MissingResourcePolicy -json

// The plugin manager automatically queries the remote API
type RateLimiterProperties struct {
	// Queries per second from one process to the remote service
	QPS int

	// Maximum burst size
	Burst int
}

type CachingProperties struct {
	// Max number of Resource's to be stored in the local cache
	Size int

	// How often to query for objects in remote service.
	ResyncInterval time.Duration

	// Workers control how many parallel workers should start up to retrieve updates
	// about resources.
	Workers int
}

type MissingResourcePolicy uint8

const (
	// MissingResourceFail indicate the task should fail if any of the resources are missing
	MissingResourceFail MissingResourcePolicy = iota

	// MissingResourceRetry indicate the task should keep retrying in case any of the resources were not found
	MissingResourceRetry
)

type ResourceQuotas map[core.ResourceNamespace]int

// Properties that help the system optimize itself to handle the specific plugin
type PluginProperties struct {
	// ResourceQuotas allows the plgin to register resources' quotas to ensure the system
	// comply with restrictions in the remote service.
	ResourceQuotas   ResourceQuotas
	ReadRateLimiter  RateLimiterProperties
	WriteRateLimiter RateLimiterProperties
	Caching          CachingProperties
	//MissingResourcePolicy MissingResourcePolicy
}
