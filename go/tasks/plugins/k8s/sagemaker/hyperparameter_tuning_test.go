package sagemaker

import (
	"context"
	"fmt"
	"testing"

	flyteIdlCore "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	stdConfig "github.com/lyft/flytestdlib/config"
	"github.com/lyft/flytestdlib/config/viper"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/utils"
	"github.com/lyft/flyteplugins/go/tasks/plugins/k8s/sagemaker/config"

	hpojobv1 "github.com/aws/amazon-sagemaker-operator-for-k8s/api/v1/hyperparametertuningjob"
	sagemakerIdl "github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins/sagemaker"
	"github.com/stretchr/testify/assert"
)

func Test_awsSagemakerPlugin_BuildResourceForHyperparameterTuningJob(t *testing.T) {
	// Default config does not contain a roleAnnotationKey -> expecting to get the role from default config
	ctx := context.TODO()
	err := config.ResetSagemakerConfig()
	if err != nil {
		panic(err)
	}
	defaultCfg := config.GetSagemakerConfig()
	defer func() {
		_ = config.SetSagemakerConfig(defaultCfg)
	}()

	t.Run("hpo on built-in algorithm training", func(t *testing.T) {
		awsSageMakerHPOJobHandler := awsSagemakerPlugin{TaskType: hyperparameterTuningJobTaskType}

		tjObj := generateMockTrainingJobCustomObj(
			sagemakerIdl.InputMode_FILE, sagemakerIdl.AlgorithmName_XGBOOST, "0.90", []*sagemakerIdl.MetricDefinition{},
			sagemakerIdl.InputContentType_TEXT_CSV, 1, "ml.m4.xlarge", 25, sagemakerIdl.DistributedProtocol_UNSPECIFIED)
		htObj := generateMockHyperparameterTuningJobCustomObj(tjObj, 10, 5)
		taskTemplate := generateMockHyperparameterTuningJobTaskTemplate("the job", htObj)
		hpoJobResource, err := awsSageMakerHPOJobHandler.BuildResource(ctx, generateMockHyperparameterTuningJobTaskContext(taskTemplate, trainingJobTaskType))
		assert.NoError(t, err)
		assert.NotNil(t, hpoJobResource)

		hpoJob, ok := hpoJobResource.(*hpojobv1.HyperparameterTuningJob)
		assert.True(t, ok)
		assert.NotNil(t, hpoJob.Spec.TrainingJobDefinition)
		assert.Equal(t, 1, len(hpoJob.Spec.HyperParameterTuningJobConfig.ParameterRanges.IntegerParameterRanges))
		assert.Equal(t, 0, len(hpoJob.Spec.HyperParameterTuningJobConfig.ParameterRanges.ContinuousParameterRanges))
		assert.Equal(t, 0, len(hpoJob.Spec.HyperParameterTuningJobConfig.ParameterRanges.CategoricalParameterRanges))
		assert.Equal(t, "us-east-1", *hpoJob.Spec.Region)
		assert.Equal(t, "default_role", *hpoJob.Spec.TrainingJobDefinition.RoleArn)
		assert.NotNil(t, hpoJob.Spec.TrainingJobDefinition.InputDataConfig)
		// Image uri should come from config
		assert.Equal(t, defaultCfg.PrebuiltAlgorithms[0].RegionalConfig[0].VersionConfigs[0].Image, *hpoJob.Spec.TrainingJobDefinition.AlgorithmSpecification.TrainingImage)
	})

	t.Run("hpo on custom training", func(t *testing.T) {
		awsSageMakerHPOJobHandler := awsSagemakerPlugin{TaskType: hyperparameterTuningJobTaskType}

		tjObj := generateMockTrainingJobCustomObj(
			sagemakerIdl.InputMode_FILE, sagemakerIdl.AlgorithmName_CUSTOM, "0.90", []*sagemakerIdl.MetricDefinition{},
			sagemakerIdl.InputContentType_TEXT_CSV, 2, "ml.p3.2xlarge", 25, sagemakerIdl.DistributedProtocol_UNSPECIFIED)
		htObj := generateMockHyperparameterTuningJobCustomObj(tjObj, 10, 5)
		taskTemplate := generateMockHyperparameterTuningJobTaskTemplate("the job", htObj)
		taskContext := generateMockHyperparameterTuningJobTaskContext(taskTemplate, customTrainingJobTaskType)
		hpoJobResource, err := awsSageMakerHPOJobHandler.BuildResource(ctx, taskContext)
		assert.NoError(t, err)
		assert.NotNil(t, hpoJobResource)

		hpoJob, ok := hpoJobResource.(*hpojobv1.HyperparameterTuningJob)
		assert.True(t, ok)
		assert.NotNil(t, hpoJob.Spec.TrainingJobDefinition)
		assert.Equal(t, 1, len(hpoJob.Spec.HyperParameterTuningJobConfig.ParameterRanges.IntegerParameterRanges))
		assert.Equal(t, 1, len(hpoJob.Spec.HyperParameterTuningJobConfig.ParameterRanges.ContinuousParameterRanges))
		assert.Equal(t, 1, len(hpoJob.Spec.HyperParameterTuningJobConfig.ParameterRanges.CategoricalParameterRanges))
		assert.Equal(t, "us-east-1", *hpoJob.Spec.Region)
		assert.Equal(t, "default_role", *hpoJob.Spec.TrainingJobDefinition.RoleArn)
		assert.Nil(t, hpoJob.Spec.TrainingJobDefinition.InputDataConfig)
		// Image uri should come from taskContext
		assert.Equal(t, testImage, *hpoJob.Spec.TrainingJobDefinition.AlgorithmSpecification.TrainingImage)
	})
}

func Test_awsSagemakerPlugin_getEventInfoForHyperparameterTuningJob(t *testing.T) {
	// Default config does not contain a roleAnnotationKey -> expecting to get the role from default config
	ctx := context.TODO()
	defaultCfg := config.GetSagemakerConfig()
	defer func() {
		_ = config.SetSagemakerConfig(defaultCfg)
	}()

	t.Run("get event info should return correctly formatted log links for custom training job", func(t *testing.T) {
		// Injecting a config which contains a mismatched roleAnnotationKey -> expecting to get the role from the config
		configAccessor := viper.NewAccessor(stdConfig.Options{
			StrictMode: true,
			// Use a different
			SearchPaths: []string{"testdata/config2.yaml"},
		})

		err := configAccessor.UpdateConfig(context.TODO())
		assert.NoError(t, err)

		awsSageMakerHPOJobHandler := awsSagemakerPlugin{TaskType: hyperparameterTuningJobTaskType}

		tjObj := generateMockTrainingJobCustomObj(
			sagemakerIdl.InputMode_FILE, sagemakerIdl.AlgorithmName_XGBOOST, "0.90", []*sagemakerIdl.MetricDefinition{},
			sagemakerIdl.InputContentType_TEXT_CSV, 1, "ml.m4.xlarge", 25, sagemakerIdl.DistributedProtocol_UNSPECIFIED)
		htObj := generateMockHyperparameterTuningJobCustomObj(tjObj, 10, 5)
		taskTemplate := generateMockHyperparameterTuningJobTaskTemplate("the job", htObj)
		taskCtx := generateMockHyperparameterTuningJobTaskContext(taskTemplate, trainingJobTaskType)
		hpoJobResource, err := awsSageMakerHPOJobHandler.BuildResource(ctx, taskCtx)
		assert.NoError(t, err)
		assert.NotNil(t, hpoJobResource)

		hpoJob, ok := hpoJobResource.(*hpojobv1.HyperparameterTuningJob)
		assert.True(t, ok)

		taskInfo, err := awsSageMakerHPOJobHandler.getEventInfoForHyperparameterTuningJob(ctx, hpoJob)
		if err != nil {
			panic(err)
		}

		expectedTaskLogs := []*flyteIdlCore.TaskLog{
			{
				Uri: fmt.Sprintf("https://%s.console.aws.amazon.com/cloudwatch/home?region=%s#logStream:group=/aws/sagemaker/TrainingJobs;prefix=%s;streamFilter=typeLogStreamPrefix",
					"us-west-2", "us-west-2", taskCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()),
				Name:          CloudWatchLogLinkName,
				MessageFormat: flyteIdlCore.TaskLog_JSON,
			},
			{
				Uri: fmt.Sprintf("https://%s.console.aws.amazon.com/sagemaker/home?region=%s#/%s/%s",
					"us-west-2", "us-west-2", "hyper-tuning-jobs", taskCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()),
				Name:          HyperparameterTuningJobSageMakerLinkName,
				MessageFormat: flyteIdlCore.TaskLog_UNKNOWN,
			},
		}

		expectedCustomInfo, _ := utils.MarshalObjToStruct(map[string]string{})
		assert.Equal(t,
			func(tis []*flyteIdlCore.TaskLog) []flyteIdlCore.TaskLog {
				ret := make([]flyteIdlCore.TaskLog, 0, len(tis))
				for _, ti := range tis {
					ret = append(ret, *ti)
				}
				return ret
			}(expectedTaskLogs),
			func(tis []*flyteIdlCore.TaskLog) []flyteIdlCore.TaskLog {
				ret := make([]flyteIdlCore.TaskLog, 0, len(tis))
				for _, ti := range tis {
					ret = append(ret, *ti)
				}
				return ret
			}(taskInfo.Logs))
		assert.Equal(t, *expectedCustomInfo, *taskInfo.CustomInfo)
	})
}
