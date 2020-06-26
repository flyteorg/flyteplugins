package config

//go:generate pflags Config --default-var=defaultConfig

import (
	pluginsConfig "github.com/lyft/flyteplugins/go/tasks/config"
)

const sagemakerConfigSectionKey = "sagemaker"

var (
	defaultConfig = Config{
		RoleArn: "default",
		Region:  "us-east-1",
		// https://docs.aws.amazon.com/sagemaker/latest/dg/sagemaker-algo-docker-registry-paths.html
		AlgorithmPrebuiltImages: map[string]map[string]map[string]string{
			"xgboost": {
				"us-east-1": {
					"0.72": "811284229777.dkr.ecr.us-east-1.amazonaws.com/xgboost:latest",
					"0.90": "683313688378.dkr.ecr.us-east-1.amazonaws.com/xgboost:latest",
				},
			},
		},
	}

	sagemakerConfigSection = pluginsConfig.MustRegisterSubSection(sagemakerConfigSectionKey, &defaultConfig)
)

// Sagemaker plugin configs
type Config struct {
	RoleArn                 string                                  `json:"roleArn" pflag:",The role the SageMaker plugin uses to communicate with the SageMaker service"`
	Region                  string                                  `json:"region" pflag:",The AWS region the SageMaker plugin communicates to"`
	AlgorithmPrebuiltImages map[string]map[string]map[string]string `json:"algorithmPrebuiltImages" pflag:"-,A map of maps containing the version to pre-built image for each supported algorithm"`
}

// Retrieves the current config value or default.
func GetSagemakerConfig() *Config {
	return sagemakerConfigSection.GetConfig().(*Config)
}

func SetSagemakerConfig(cfg *Config) error {
	return sagemakerConfigSection.SetConfig(cfg)
}
