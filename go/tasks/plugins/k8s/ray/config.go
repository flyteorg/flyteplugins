package ray

import (
	"fmt"
	"io/ioutil"

	pluginsConfig "github.com/flyteorg/flyteplugins/go/tasks/config"
	"github.com/pkg/errors"
	restclient "k8s.io/client-go/rest"
)

//go:generate pflags Config --default-var=defaultConfig

type ClusterConfig struct {
	Name     string `json:"name" pflag:",Friendly name of the remote cluster"`
	Endpoint string `json:"endpoint" pflag:", Remote K8s cluster endpoint"`
	Auth     Auth   `json:"auth" pflag:"-, Auth setting for the cluster"`
	Enabled  bool   `json:"enabled" pflag:", Boolean flag to enable or disable"`
}

type Auth struct {
	TokenPath  string `json:"tokenPath" pflag:", Token path"`
	CaCertPath string `json:"caCertPath" pflag:", Certificate path"`
}

func (auth Auth) GetCA() ([]byte, error) {
	cert, err := ioutil.ReadFile(auth.CaCertPath)
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

// KubeClientConfig ...
func KubeClientConfig(host string, auth Auth) (*restclient.Config, error) {
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

var (
	defaultConfig = Config{
		ShutdownAfterJobFinishes: true,
		TTLSecondsAfterFinished:  3600,
		ServiceType:              "NodePort",
		IncludeDashboard:         true,
		DashboardHost:            "0.0.0.0",
		NodeIPAddress:            "$MY_POD_IP",
	}

	configSection = pluginsConfig.MustRegisterSubSection("ray", &defaultConfig)
)

// Config is config for 'ray' plugin
type Config struct {
	// ShutdownAfterJobFinishes will determine whether to delete the ray cluster once rayJob succeed or failed
	ShutdownAfterJobFinishes bool `json:"shutdownAfterJobFinishes,omitempty"`

	// TTLSecondsAfterFinished is the TTL to clean up RayCluster.
	// It's only working when ShutdownAfterJobFinishes set to true.
	TTLSecondsAfterFinished int32 `json:"ttlSecondsAfterFinished,omitempty"`

	// Kubernetes Service Type, valid values are 'ClusterIP', 'NodePort' and 'LoadBalancer'
	ServiceType string `json:"serviceType,omitempty"`

	// IncludeDashboard is used to start a Ray Dashboard if set to true
	IncludeDashboard bool `json:"includeDashboard,omitempty"`

	// DashboardHost the host to bind the dashboard server to, either localhost (127.0.0.1)
	// or 0.0.0.0 (available from all interfaces). By default, this is localhost.
	DashboardHost string `json:"dashboardHost,omitempty"`

	// NodeIPAddress the IP address of the head node. By default, this is pod ip address.
	NodeIPAddress string `json:"nodeIPAddress,omitempty"`

	// Remote Ray Cluster Config
	RemoteClusterConfig ClusterConfig `json:"remoteClusterConfig" pflag:"Configuration of remote K8s cluster for ray jobs"`
}

func GetConfig() *Config {
	return configSection.GetConfig().(*Config)
}

func SetConfig(cfg *Config) error {
	return configSection.SetConfig(cfg)
}
