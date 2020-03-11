package config

//go:generate pflags Config --default-var=defaultConfig

import (
	"context"
	"net/url"

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
	Name                             string  `json:"primaryLabel" pflag:",The name of a given Presto routing group"`
	Limit                            int     `json:"limit" pflag:",Resource quota (in the number of outstanding requests) of the routing group"`
	ProjectScopeQuotaProportionCap   float64 `json:"projectScopeQuotaProportionCap" pflag:",A floating point number between 0 and 1, specifying the maximum proportion of quotas allowed to allocate to a project in the routing group"`
	NamespaceScopeQuotaProportionCap float64 `json:"namespaceScopeQuotaProportionCap" pflag:",A floating point number between 0 and 1, specifying the maximum proportion of quotas allowed to allocate to a namespace in the routing group"`
}

var (
	defaultConfig = Config{
		Environment:         URLMustParse("https://prestoproxy-internal.lyft.net:443"),
		DefaultRoutingGroup: "adhoc",
		Workers:             15,
		LruCacheSize:        2000,
		AwsS3ShardFormatter: "s3://lyft-modelbuilder/{}/",
		AwsS3ShardCount:     2,
		RoutingGroupConfigs: []RoutingGroupConfig{{Name: "adhoc", Limit: 250}},
	}

	prestoConfigSection = pluginsConfig.MustRegisterSubSection(prestoConfigSectionKey, &defaultConfig)
)

// Presto plugin configs
type Config struct {
	Environment         config.URL           `json:"endpoint" pflag:",Endpoint for Presto to use"`
	DefaultRoutingGroup string               `json:"defaultRoutingGroup" pflag:",Default Presto routing group"`
	Workers             int                  `json:"workers" pflag:",Number of parallel workers to refresh the cache"`
	LruCacheSize        int                  `json:"lruCacheSize" pflag:",Size of the AutoRefreshCache"`
	AwsS3ShardFormatter string               `json:"awsS3ShardFormatter" pflag:", S3 bucket prefix where Presto results will be stored"`
	AwsS3ShardCount     int                  `json:"awsS3ShardStringLength" pflag:", Number of characters for the S3 bucket shard prefix"`
	RoutingGroupConfigs []RoutingGroupConfig `json:"clusterConfigs" pflag:"-,A list of cluster configs. Each of the configs corresponds to a service cluster"`
}

// Retrieves the current config value or default.
func GetPrestoConfig() *Config {
	return prestoConfigSection.GetConfig().(*Config)
}

func SetPrestoConfig(cfg *Config) error {
	return prestoConfigSection.SetConfig(cfg)
}
