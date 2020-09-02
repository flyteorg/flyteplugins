package sagemaker

import (
	"context"
	"testing"

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
	awsSageMakerHPOJobHandler := awsSagemakerPlugin{TaskType: hyperparameterTuningJobTaskType}

	tjObj := generateMockTrainingJobCustomObj(
		sagemakerIdl.InputMode_FILE, sagemakerIdl.AlgorithmName_XGBOOST, "0.90", []*sagemakerIdl.MetricDefinition{},
		sagemakerIdl.InputContentType_TEXT_CSV, 1, "ml.m4.xlarge", 25)
	htObj := generateMockHyperparameterTuningJobCustomObj(tjObj, 10, 5)
	taskTemplate := generateMockHyperparameterTuningJobTaskTemplate("the job", htObj)
	hpoJobResource, err := awsSageMakerHPOJobHandler.BuildResource(ctx, generateMockHyperparameterTuningJobTaskContext(taskTemplate))
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

	err = config.SetSagemakerConfig(defaultCfg)
	if err != nil {
		panic(err)
	}
}
