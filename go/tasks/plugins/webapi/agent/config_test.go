package agent

import (
	"testing"
	"time"

	"github.com/flyteorg/flytestdlib/config"

	"github.com/stretchr/testify/assert"
)

func TestGetAndSetConfig(t *testing.T) {
	cfg := defaultConfig
	cfg.WebAPI.Caching.Workers = 1
	cfg.WebAPI.Caching.ResyncInterval.Duration = 5 * time.Second
	cfg.DefaultGrpcEndpoint.Insecure = false
	cfg.DefaultGrpcEndpoint.DefaultServiceConfig = "{\"loadBalancingConfig\": [{\"round_robin\":{}}]}"
	cfg.DefaultGrpcEndpoint.Timeouts = map[string]config.Duration{
		"CreateTask": {
			Duration: 1 * time.Millisecond,
		},
		"GetTask": {
			Duration: 2 * time.Millisecond,
		},
		"DeleteTask": {
			Duration: 3 * time.Millisecond,
		},
	}
	cfg.DefaultGrpcEndpoint.DefaultTimeout = config.Duration{Duration: 10 * time.Second}
	err := SetConfig(&cfg)
	assert.NoError(t, err)
	assert.Equal(t, &cfg, GetConfig())
}
