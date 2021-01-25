/*
 * Copyright (c) 2018 Lyft. All rights reserved.
 */

package aws

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/lyft/flytestdlib/config"

	pluginsConfig "github.com/lyft/flyteplugins/go/tasks/config"
)

//go:generate pflags Config --default-var defaultConfig

const ConfigSectionKey = "aws"

var (
	defaultConfig = &Config{
		Region:  "us-east-1",
		Retries: 3,
		SdkConfig: aws.Config{
			Region: "us-east-1",
			Retryer: retry.NewStandard(func(options *retry.StandardOptions) {
				options.MaxAttempts = 3
			}),
		},
	}

	configSection = pluginsConfig.MustRegisterSubSection(ConfigSectionKey, defaultConfig)
)

// Config section for AWS Package
type Config struct {
	SdkConfig aws.Config `json:",inline"`

	// Deprecated, use SdkConfig instead
	Region string `json:"region" pflag:",Deprecated: use SdkConfig instead. AWS Region to connect to."`
	// Deprecated, use SdkConfig instead
	AccountID string `json:"accountId" pflag:",Deprecated: use SdkConfig instead. AWS Account Identifier."`
	// Deprecated, use SdkConfig instead
	Retries int `json:"retries" pflag:",Deprecated: use SdkConfig instead. Number of retries."`
}

type RateLimiterConfig struct {
	Rate  int64 `json:"rate" pflag:",Allowed rate of calls per second."`
	Burst int   `json:"burst" pflag:",Allowed burst rate of calls."`
}

// Gets loaded config for AWS
func GetConfig() *Config {
	cfg := configSection.GetConfig().(*Config)

	// For backward compatibility
	if len(cfg.SdkConfig.Region) == 0 {
		cfg.SdkConfig.Region = cfg.Region
		cfg.SdkConfig.Retryer = retry.NewStandard(func(options *retry.StandardOptions) {
			options.MaxAttempts = cfg.Retries
		})
	}

	return cfg
}

func MustRegisterSubSection(key config.SectionKey, cfg config.Config) config.Section {
	return configSection.MustRegisterSection(key, cfg)
}
