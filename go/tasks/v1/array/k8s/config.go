/*
 * Copyright (c) 2018 Lyft. All rights reserved.
 */

package k8s

import (
	"github.com/lyft/flytestdlib/config"
)

//go:generate pflags Config

var configSection = config.MustRegisterSection(configSectionKey, &Config{})

const configSectionKey = "k8s-array"

// Defines custom config for K8s Array plugin
type Config struct {
	DefaultScheduler     string `json:"scheduler" pflag:",Decides the scheduler to use when launching array-pods."`
	MaxErrorStringLength int    `json:"maxErrLength" pflag:",Determines the maximum length of the error string returned for the array."`
}

func GetConfig() *Config {
	return configSection.GetConfig().(*Config)
}
