package service

import "time"

// The plugin manager automatically queries the remote API
type RateLimiterProperties struct {
	// Queries per second from one process to the remote service
	QPS int
	// Maximum burst size
	Burst int
}

type CachingProperties struct {
	// Max number of RemoteResource's to be stored in the local cache
	Size int
	// How often to query for objects in remote service.
	ResyncInterval time.Duration
}

type MissingResourceSemantics uint8

const (
	MissingResourceFailIfMissing MissingResourceSemantics = iota
	MissingResourceRetryIfMissing
)

// Properties that indicate if the Batch API can be invoked. This indicates that GetBatch() can be used instead of Get
type BatchingProperties struct {
	BatchAPISupported        bool
	MaxBatchSize             int
	MissingResourceSemantics MissingResourceSemantics
}

// Properties that help the system optimize itself to handle the specific plugin
type PluginProperties struct {
	RateLimiterProperties RateLimiterProperties
	CachingProperties     CachingProperties
	BatchingProperties    BatchingProperties
}
