package tasks_test

import (
	"context"
	"testing"

	sagemakerConfig "github.com/lyft/flyteplugins/go/tasks/plugins/k8s/sagemaker/config"

	"github.com/lyft/flytestdlib/config"
	"github.com/lyft/flytestdlib/config/viper"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	pluginsConfig "github.com/lyft/flyteplugins/go/tasks/config"
	"github.com/lyft/flyteplugins/go/tasks/logs"
	flyteK8sConfig "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/flytek8s/config"
	"github.com/lyft/flyteplugins/go/tasks/plugins/k8s/spark"
)

func TestLoadConfig(t *testing.T) {
	configAccessor := viper.NewAccessor(config.Options{
		StrictMode:  true,
		SearchPaths: []string{"testdata/config.yaml"},
	})

	err := configAccessor.UpdateConfig(context.TODO())
	assert.NoError(t, err)

	t.Run("root-config-test", func(t *testing.T) {
		assert.Equal(t, 1, len(pluginsConfig.GetConfig().EnabledPlugins))
	})

	t.Run("k8s-config-test", func(t *testing.T) {

		k8sConfig := flyteK8sConfig.GetK8sPluginConfig()
		assert.True(t, k8sConfig.InjectFinalizer)
		assert.Equal(t, map[string]string{
			"annotationKey1": "annotationValue1",
			"annotationKey2": "annotationValue2",
			"cluster-autoscaler.kubernetes.io/safe-to-evict": "false",
		}, k8sConfig.DefaultAnnotations)
		assert.Equal(t, map[string]string{
			"label1": "labelValue1",
			"label2": "labelValue2",
		}, k8sConfig.DefaultLabels)
		assert.Equal(t, map[string]string{
			"AWS_METADATA_SERVICE_NUM_ATTEMPTS": "20",
			"AWS_METADATA_SERVICE_TIMEOUT":      "5",
			"FLYTE_AWS_ACCESS_KEY_ID":           "minio",
			"FLYTE_AWS_ENDPOINT":                "http://minio.flyte:9000",
			"FLYTE_AWS_SECRET_ACCESS_KEY":       "miniostorage",
		}, k8sConfig.DefaultEnvVars)
		assert.NotNil(t, k8sConfig.ResourceTolerations)
		assert.Contains(t, k8sConfig.ResourceTolerations, v1.ResourceName("nvidia.com/gpu"))
		assert.Contains(t, k8sConfig.ResourceTolerations, v1.ResourceStorage)
		tolGPU := v1.Toleration{
			Key:      "flyte/gpu",
			Value:    "dedicated",
			Operator: v1.TolerationOpEqual,
			Effect:   v1.TaintEffectNoSchedule,
		}

		tolStorage := v1.Toleration{
			Key:      "storage",
			Value:    "special",
			Operator: v1.TolerationOpEqual,
			Effect:   v1.TaintEffectPreferNoSchedule,
		}

		assert.Equal(t, []v1.Toleration{tolGPU}, k8sConfig.ResourceTolerations[v1.ResourceName("nvidia.com/gpu")])
		assert.Equal(t, []v1.Toleration{tolStorage}, k8sConfig.ResourceTolerations[v1.ResourceStorage])
		assert.Equal(t, "1000m", k8sConfig.DefaultCPURequest)
		assert.Equal(t, "1024Mi", k8sConfig.DefaultMemoryRequest)
		assert.Equal(t, map[string]string{"x/interruptible": "true"}, k8sConfig.InterruptibleNodeSelector)
		assert.Equal(t, "x/flyte", k8sConfig.InterruptibleTolerations[0].Key)
		assert.Equal(t, "interruptible", k8sConfig.InterruptibleTolerations[0].Value)
	})

	t.Run("logs-config-test", func(t *testing.T) {
		assert.NotNil(t, logs.GetLogConfig())
		assert.True(t, logs.GetLogConfig().IsKubernetesEnabled)
	})

	t.Run("spark-config-test", func(t *testing.T) {
		assert.NotNil(t, spark.GetSparkConfig())
		assert.NotNil(t, spark.GetSparkConfig().DefaultSparkConfig)
		assert.Equal(t, 2, len(spark.GetSparkConfig().Features))
		assert.Equal(t, "feature1", spark.GetSparkConfig().Features[0].Name)
		assert.Equal(t, "feature2", spark.GetSparkConfig().Features[1].Name)
		assert.Equal(t, 2, len(spark.GetSparkConfig().Features[0].SparkConfig))
		assert.Equal(t, 2, len(spark.GetSparkConfig().Features[1].SparkConfig))

	})

	t.Run("sagemaker-config-test", func(t *testing.T) {
		assert.NotNil(t, sagemakerConfig.GetSagemakerConfig())
	})
}
