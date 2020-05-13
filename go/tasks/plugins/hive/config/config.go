package config

//go:generate pflags Config --default-var=defaultConfig

import (
	"context"
	"net/url"
	"time"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/remote"

	"github.com/lyft/flytestdlib/config"
	"github.com/lyft/flytestdlib/logger"

	pluginsConfig "github.com/lyft/flyteplugins/go/tasks/config"
)

const quboleConfigSectionKey = "qubole"

func MustParse(s string) config.URL {
	r, err := url.Parse(s)
	if err != nil {
		logger.Panicf(context.TODO(), "Bad Qubole URL Specified as default, error: %s", err)
	}
	if r == nil {
		logger.Panicf(context.TODO(), "Nil Qubole URL specified.", err)
	}
	return config.URL{URL: *r}
}

type ClusterConfig struct {
	PrimaryLabel                     string   `json:"primaryLabel" pflag:",The primary label of a given service cluster"`
	Labels                           []string `json:"labels" pflag:",Labels of a given service cluster"`
	Limit                            int      `json:"limit" pflag:",Resource quota (in the number of outstanding requests) of the service cluster"`
	ProjectScopeQuotaProportionCap   float64  `json:"projectScopeQuotaProportionCap" pflag:",A floating point number between 0 and 1, specifying the maximum proportion of quotas allowed to allocate to a project in the service cluster"`
	NamespaceScopeQuotaProportionCap float64  `json:"namespaceScopeQuotaProportionCap" pflag:",A floating point number between 0 and 1, specifying the maximum proportion of quotas allowed to allocate to a namespace in the service cluster"`
}

type DestinationClusterConfig struct {
	Project      string `json:"project" pflag:",Project of the task which the query belongs to"`
	Domain       string `json:"domain" pflag:",Domain of the task which the query belongs to"`
	ClusterLabel string `json:"clusterLabel" pflag:",The label of the destination cluster this query to be submitted to"`
}

var (
	defaultConfig = Config{
		Endpoint:                  MustParse("https://wellness.qubole.com"),
		CommandAPIPath:            MustParse("/api/v1.2/commands/"),
		AnalyzeLinkPath:           MustParse("/v2/analyze"),
		TokenKey:                  "FLYTE_QUBOLE_CLIENT_TOKEN",
		ClusterConfigs:            []ClusterConfig{{PrimaryLabel: "default", Labels: []string{"default"}, Limit: 100, ProjectScopeQuotaProportionCap: 0.7, NamespaceScopeQuotaProportionCap: 0.7}},
		DestinationClusterConfigs: []DestinationClusterConfig{},
		Caching: remote.CachingProperties{
			Size:           2000,
			ResyncInterval: config.Duration{Duration: 20 * time.Second},
			Workers:        10,
		},
		WriteRateLimiter: remote.RateLimiterProperties{
			QPS:   100,
			Burst: 200,
		},
		ReadRateLimiter: remote.RateLimiterProperties{
			QPS:   100,
			Burst: 200,
		},
	}

	quboleConfigSection = pluginsConfig.MustRegisterSubSection(quboleConfigSectionKey, &defaultConfig)
)

// Qubole plugin configs
type Config struct {
	Endpoint                  config.URL                   `json:"endpoint" pflag:",Endpoint for qubole to use"`
	CommandAPIPath            config.URL                   `json:"commandApiPath" pflag:",API Path where commands can be launched on Qubole. Should be a valid url."`
	AnalyzeLinkPath           config.URL                   `json:"analyzeLinkPath" pflag:",URL path where queries can be visualized on qubole website. Should be a valid url."`
	TokenKey                  string                       `json:"quboleTokenKey" pflag:",Name of the key where to find Qubole token in the secret manager."`
	ClusterConfigs            []ClusterConfig              `json:"clusterConfigs" pflag:"-,A list of cluster configs. Each of the configs corresponds to a service cluster"`
	DestinationClusterConfigs []DestinationClusterConfig   `json:"destinationClusterConfigs" pflag:"-,A list configs specifying the destination service cluster for (project, domain)"`
	ReadRateLimiter           remote.RateLimiterProperties `json:"readRateLimiter" pflag:",Defines rate limiter properties for read actions (e.g. retrieve status)."`
	WriteRateLimiter          remote.RateLimiterProperties `json:"writeRateLimiter" pflag:",Defines rate limiter properties for write actions."`
	Caching                   remote.CachingProperties     `json:"caching" pflag:",Defines caching characteristics."`
}

// Retrieves the current config value or default.
func GetQuboleConfig() *Config {
	return quboleConfigSection.GetConfig().(*Config)
}

func SetQuboleConfig(cfg *Config) error {
	return quboleConfigSection.SetConfig(cfg)
}
