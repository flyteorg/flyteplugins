package core

import (
	"fmt"

	"github.com/pkg/errors"

	"io/ioutil"

	restclient "k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

//go:generate pflags Config --default-var=defaultConfig

// TODO we may not want to expose this?
// A friendly controller-runtime client that gets passed to executors
type KubeClient interface {
	// GetClient returns a client configured with the Config
	GetClient() client.Client

	// GetCache returns a cache.Cache
	GetCache() cache.Cache
}

type ClusterConfig struct {
	Name     string `json:"name" pflag:","`
	Endpoint string `json:"endpoint" pflag:","`
	Auth     Auth   `json:"auth" pflag:","`
	Enabled  bool   `json:"enabled" pflag:","`
}

type Auth struct {
	Type      string `json:"type" pflag:","`
	TokenPath string `json:"tokenPath" pflag:","`
	CertPath  string `json:"certPath" pflag:","`
}

func (auth Auth) GetCA() ([]byte, error) {
	cert, err := ioutil.ReadFile(auth.CertPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read k8s CA cert from configured path")
	}
	return cert, nil
}

func (auth Auth) GetToken() (string, error) {
	token, err := ioutil.ReadFile(auth.TokenPath)
	if err != nil {
		return "", errors.Wrap(err, "failed to read k8s bearer token from configured path")
	}
	return string(token), nil
}

// Reads secret values from paths specified in the config to initialize a Kubernetes rest client Config.
func RemoteClusterConfig(host string, auth Auth) (*restclient.Config, error) {
	tokenString, err := auth.GetToken()
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to get auth token: %+v", err))
	}

	caCert, err := auth.GetCA()
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to get auth CA: %+v", err))
	}

	tlsClientConfig := restclient.TLSClientConfig{}
	tlsClientConfig.CAData = caCert
	return &restclient.Config{
		Host:            host,
		TLSClientConfig: tlsClientConfig,
		BearerToken:     tokenString,
	}, nil
}

func GetK8sClient(config ClusterConfig) (client.Client, error) {
	kubeConf, err := RemoteClusterConfig(config.Endpoint, config.Auth)
	if err != nil {
		return nil, err
	}
	return client.New(kubeConf, client.Options{})
}
