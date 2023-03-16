package ray

import (
	"testing"

	"gotest.tools/assert"
)

func TestLoadConfig(t *testing.T) {
	rayConfig := GetConfig()
	assert.Assert(t, rayConfig != nil)

	t.Run("remote cluster", func(t *testing.T) {
		config := GetConfig()
		remoteConfig := ClusterConfig{
			Enabled:  false,
			Endpoint: "",
			Auth: Auth{
				TokenPath:  "",
				CaCertPath: "",
			},
		}
		assert.DeepEqual(t, config.RemoteClusterConfig, remoteConfig)
	})
}
