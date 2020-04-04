package config

//go:generate pflags Config --default-var=defaultConfig

import (
	"context"
	"net/url"
	"time"

	"github.com/lyft/flytestdlib/config"
	"github.com/lyft/flytestdlib/logger"

	pluginsConfig "github.com/lyft/flyteplugins/go/tasks/config"
)

const prestoConfigSectionKey = "presto"

func URLMustParse(s string) config.URL {
	r, err := url.Parse(s)
	if err != nil {
		logger.Panicf(context.TODO(), "Bad Presto URL Specified as default, error: %s", err)
	}
	if r == nil {
		logger.Panicf(context.TODO(), "Nil Presto URL specified.", err)
	}
	return config.URL{URL: *r}
}

type RoutingGroupConfig struct {
	Name                             string  `json:"name" pflag:",The name of a given Presto routing group"`
	Limit                            int     `json:"limit" pflag:",Resource quota (in the number of outstanding requests) of the routing group"`
	ProjectScopeQuotaProportionCap   float64 `json:"projectScopeQuotaProportionCap" pflag:",A floating point number between 0 and 1, specifying the maximum proportion of quotas allowed to allocate to a project in the routing group"`
	NamespaceScopeQuotaProportionCap float64 `json:"namespaceScopeQuotaProportionCap" pflag:",A floating point number between 0 and 1, specifying the maximum proportion of quotas allowed to allocate to a namespace in the routing group"`
}

type RefreshCacheConfig struct {
	Name         string          `json:"name" pflag:",The name of the rate limiter"`
	SyncPeriod   config.Duration `json:"syncPeriod" pflag:",The duration to wait before the cache is refreshed again"`
	Workers      int             `json:"workers" pflag:",Number of parallel workers to refresh the cache"`
	LruCacheSize int             `json:"lruCacheSize" pflag:",Size of the cache"`
}

// To execute a single Presto query from a user's point of view, we actually need to send 5 different
// requests to Presto. Together these requests (i.e. queries) take care of retrieving the data, saving
// it to an external table, and performing cleanup.
//
// The Presto plugin currently uses a single allocation token for each set of 5 requests which
// correspond to a single user query. These means that in total, Flyte is able to work on
// 'PrestoConfig.RoutingGroups[routing_group_name].Limit' user queries at a time as configured in the
// configurations for the Presto plugin. This means means that at most, Flyte will be working on this
// number of user Presto queries at a time for each of the configured Presto routing groups.
//
// This rate limiter will control the rate at which requests are sent to Presto for ALL requests coming
// from this client. This includes requests to execute queries, requests to get the status of a query
// (through the auto-refresh cache), and even requests to cancel queries. Together with allocation
// tokens, this rate limiter will ensure that the rate of requests and the number of concurrent requests
// going to Presto don't overload the cluster.
type RateLimiterConfig struct {
	Rate  int64 `json:"rate" pflag:",Allowed rate of calls per second."`
	Burst int   `json:"burst" pflag:",Allowed burst rate of calls per second."`
}

var (
	defaultConfig = Config{
		Environment:         URLMustParse(""),
		DefaultRoutingGroup: "adhoc",
		DefaultUser:         "flyte-default-user",
		RoutingGroupConfigs: []RoutingGroupConfig{{Name: "adhoc", Limit: 100}, {Name: "etl", Limit: 25}},
		RefreshCacheConfig: RefreshCacheConfig{
			Name:         "presto",
			SyncPeriod:   config.Duration{Duration: 5 * time.Second},
			Workers:      15,
			LruCacheSize: 10000,
		},
		ReadRateLimiterConfig: RateLimiterConfig{
			Rate:  10,
			Burst: 10,
		},
		WriteRateLimiterConfig: RateLimiterConfig{
			Rate:  5,
			Burst: 10,
		},
	}

	prestoConfigSection = pluginsConfig.MustRegisterSubSection(prestoConfigSectionKey, &defaultConfig)
)

// Presto plugin configs
type Config struct {
	Environment            config.URL           `json:"environment" pflag:",Environment endpoint for Presto to use"`
	DefaultRoutingGroup    string               `json:"defaultRoutingGroup" pflag:",Default Presto routing group"`
	DefaultUser            string               `json:"defaultUser" pflag:",Default Presto user"`
	RoutingGroupConfigs    []RoutingGroupConfig `json:"routingGroupConfigs" pflag:"-,A list of cluster configs. Each of the configs corresponds to a service cluster"`
	RefreshCacheConfig     RefreshCacheConfig   `json:"refreshCacheConfig" pflag:"Refresh cache config"`
	ReadRateLimiterConfig  RateLimiterConfig    `json:"readRateLimiterConfig" pflag:"Rate limiter config for read requests going to Presto"`
	WriteRateLimiterConfig RateLimiterConfig    `json:"writeRateLimiterConfig" pflag:"Rate limiter config for write requests going to Presto"`
}

// Retrieves the current config value or default.
func GetPrestoConfig() *Config {
	return prestoConfigSection.GetConfig().(*Config)
}

func SetPrestoConfig(cfg *Config) error {
	return prestoConfigSection.SetConfig(cfg)
}
