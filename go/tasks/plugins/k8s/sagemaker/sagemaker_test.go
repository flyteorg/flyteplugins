package sagemaker

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/lyft/flytestdlib/contextutils"
	"github.com/lyft/flytestdlib/promutils/labeled"

	"github.com/lyft/flytestdlib/promutils"

	"github.com/pkg/errors"

	taskError "github.com/lyft/flyteplugins/go/tasks/errors"

	trainingjobController "github.com/aws/amazon-sagemaker-operator-for-k8s/controllers/trainingjob"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/k8s"

	commonv1 "github.com/aws/amazon-sagemaker-operator-for-k8s/api/v1/common"

	"github.com/lyft/flyteplugins/go/tasks/plugins/k8s/sagemaker/config"

	"github.com/golang/protobuf/proto"

	stdConfig "github.com/lyft/flytestdlib/config"
	"github.com/lyft/flytestdlib/config/viper"

	hpojobv1 "github.com/aws/amazon-sagemaker-operator-for-k8s/api/v1/hyperparametertuningjob"
	trainingjobv1 "github.com/aws/amazon-sagemaker-operator-for-k8s/api/v1/trainingjob"
	"github.com/aws/aws-sdk-go/service/sagemaker"
	"github.com/golang/protobuf/jsonpb"
	structpb "github.com/golang/protobuf/ptypes/struct"
	flyteIdlCore "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	sagemakerIdl "github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins/sagemaker"
	pluginsCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/flytek8s"
	pluginIOMocks "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io/mocks"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/utils"
	"github.com/lyft/flytestdlib/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const testImage = "image://"
const serviceAccount = "sagemaker_sa"

var (
	dummyEnvVars = []*flyteIdlCore.KeyValuePair{
		{Key: "Env_Var", Value: "Env_Val"},
	}

	testArgs = []string{
		"service_venv",
		"pyflyte-execute",
		"--test-opt1",
		"value1",
		"--test-opt2",
		"value2",
		"--test-flag",
	}

	testCmds = []string{
		"test-cmds1",
		"test-cmds2",
	}

	resourceRequirements = &corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:         resource.MustParse("1000m"),
			corev1.ResourceMemory:      resource.MustParse("1Gi"),
			flytek8s.ResourceNvidiaGPU: resource.MustParse("1"),
		},
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:         resource.MustParse("100m"),
			corev1.ResourceMemory:      resource.MustParse("512Mi"),
			flytek8s.ResourceNvidiaGPU: resource.MustParse("1"),
		},
	}
)

func generateMockTrainingJobTaskTemplate(id string, trainingJobCustomObj *sagemakerIdl.TrainingJob) *flyteIdlCore.TaskTemplate {

	tjObjJSON, err := utils.MarshalToString(trainingJobCustomObj)
	if err != nil {
		panic(err)
	}
	structObj := structpb.Struct{}

	err = jsonpb.UnmarshalString(tjObjJSON, &structObj)
	if err != nil {
		panic(err)
	}

	return &flyteIdlCore.TaskTemplate{
		Id:   &flyteIdlCore.Identifier{Name: id},
		Type: "container",
		Target: &flyteIdlCore.TaskTemplate_Container{
			Container: &flyteIdlCore.Container{
				Command: testCmds,
				Image:   testImage,
				Args:    testArgs,
				Env:     dummyEnvVars,
			},
		},
		Custom: &structObj,
	}
}

func generateMockHyperparameterTuningJobTaskTemplate(id string, hpoJobCustomObj *sagemakerIdl.HyperparameterTuningJob) *flyteIdlCore.TaskTemplate {

	htObjJSON, err := utils.MarshalToString(hpoJobCustomObj)
	if err != nil {
		panic(err)
	}
	structObj := structpb.Struct{}

	err = jsonpb.UnmarshalString(htObjJSON, &structObj)
	if err != nil {
		panic(err)
	}

	return &flyteIdlCore.TaskTemplate{
		Id:   &flyteIdlCore.Identifier{Name: id},
		Type: "container",
		Target: &flyteIdlCore.TaskTemplate_Container{
			Container: &flyteIdlCore.Container{
				Image: testImage,
				Args:  testArgs,
				Env:   dummyEnvVars,
			},
		},
		Custom: &structObj,
		Interface: &flyteIdlCore.TypedInterface{
			Inputs: &flyteIdlCore.VariableMap{
				Variables: map[string]*flyteIdlCore.Variable{
					"input": {
						Type: &flyteIdlCore.LiteralType{
							Type: &flyteIdlCore.LiteralType_CollectionType{
								CollectionType: &flyteIdlCore.LiteralType{Type: &flyteIdlCore.LiteralType_Simple{Simple: flyteIdlCore.SimpleType_INTEGER}},
							},
						},
					},
				},
			},
			Outputs: &flyteIdlCore.VariableMap{
				Variables: map[string]*flyteIdlCore.Variable{
					"output": {
						Type: &flyteIdlCore.LiteralType{
							Type: &flyteIdlCore.LiteralType_CollectionType{
								CollectionType: &flyteIdlCore.LiteralType{Type: &flyteIdlCore.LiteralType_Simple{Simple: flyteIdlCore.SimpleType_INTEGER}},
							},
						},
					},
				},
			},
		},
	}
}

// nolint
func generateMockCustomTrainingJobTaskContext(taskTemplate *flyteIdlCore.TaskTemplate, outputReaderPutError bool) pluginsCore.TaskExecutionContext {
	taskCtx := &mocks.TaskExecutionContext{}
	inputReader := &pluginIOMocks.InputReader{}
	inputReader.OnGetInputPrefixPath().Return(storage.DataReference("/input/prefix"))
	inputReader.OnGetInputPath().Return(storage.DataReference("/input"))

	trainBlobLoc := storage.DataReference("train-blob-loc")
	validationBlobLoc := storage.DataReference("validation-blob-loc")

	inputReader.OnGetMatch(mock.Anything).Return(
		&flyteIdlCore.LiteralMap{
			Literals: map[string]*flyteIdlCore.Literal{
				"train":      generateMockBlobLiteral(trainBlobLoc),
				"validation": generateMockBlobLiteral(validationBlobLoc),
				"hp_int":     utils.MustMakeLiteral(1),
				"hp_float":   utils.MustMakeLiteral(1.5),
				"hp_bool":    utils.MustMakeLiteral(false),
				"hp_string":  utils.MustMakeLiteral("a"),
			},
		}, nil)
	taskCtx.OnInputReader().Return(inputReader)

	outputReader := &pluginIOMocks.OutputWriter{}
	outputReader.OnGetOutputPath().Return(storage.DataReference("/data/outputs.pb"))
	outputReader.OnGetOutputPrefixPath().Return(storage.DataReference("/data/"))
	outputReader.OnGetRawOutputPrefix().Return(storage.DataReference("/raw/"))
	if outputReaderPutError {
		outputReader.OnPutMatch(mock.Anything, mock.Anything).Return(errors.Errorf("err"))
	} else {
		outputReader.OnPutMatch(mock.Anything, mock.Anything).Return(nil)
	}
	taskCtx.OnOutputWriter().Return(outputReader)

	taskReader := &mocks.TaskReader{}
	taskReader.OnReadMatch(mock.Anything).Return(taskTemplate, nil)
	taskCtx.OnTaskReader().Return(taskReader)

	tID := &mocks.TaskExecutionID{}
	tID.OnGetID().Return(flyteIdlCore.TaskExecutionIdentifier{
		NodeExecutionId: &flyteIdlCore.NodeExecutionIdentifier{
			ExecutionId: &flyteIdlCore.WorkflowExecutionIdentifier{
				Name:    "my_name",
				Project: "my_project",
				Domain:  "my_domain",
			},
		},
	})
	tID.OnGetGeneratedName().Return("some-acceptable-name")

	resources := &mocks.TaskOverrides{}
	resources.OnGetResources().Return(resourceRequirements)

	taskExecutionMetadata := &mocks.TaskExecutionMetadata{}
	taskExecutionMetadata.OnGetTaskExecutionID().Return(tID)
	taskExecutionMetadata.OnGetNamespace().Return("test-namespace")
	taskExecutionMetadata.OnGetAnnotations().Return(map[string]string{"iam.amazonaws.com/role": "metadata_role"})
	taskExecutionMetadata.OnGetLabels().Return(map[string]string{"label-1": "val1"})
	taskExecutionMetadata.OnGetOwnerReference().Return(v1.OwnerReference{
		Kind: "node",
		Name: "blah",
	})
	taskExecutionMetadata.OnIsInterruptible().Return(true)
	taskExecutionMetadata.OnGetOverrides().Return(resources)
	taskExecutionMetadata.OnGetK8sServiceAccount().Return(serviceAccount)
	taskCtx.OnTaskExecutionMetadata().Return(taskExecutionMetadata)

	dataStore, err := storage.NewDataStore(&storage.Config{Type: storage.TypeMemory}, promutils.NewTestScope())
	if err != nil {
		panic(err)
	}
	taskCtx.OnDataStore().Return(dataStore)

	taskCtx.OnMaxDatasetSizeBytes().Return(10000)

	return taskCtx
}

// nolint
func generateMockTrainingJobTaskContext(taskTemplate *flyteIdlCore.TaskTemplate, outputReaderPutError bool) pluginsCore.TaskExecutionContext {
	taskCtx := &mocks.TaskExecutionContext{}
	inputReader := &pluginIOMocks.InputReader{}
	inputReader.OnGetInputPrefixPath().Return(storage.DataReference("/input/prefix"))
	inputReader.OnGetInputPath().Return(storage.DataReference("/input"))

	trainBlobLoc := storage.DataReference("train-blob-loc")
	validationBlobLoc := storage.DataReference("validation-blob-loc")
	shp := map[string]string{"a": "1", "b": "2"}
	shpStructObj, _ := utils.MarshalObjToStruct(shp)

	inputReader.OnGetMatch(mock.Anything).Return(
		&flyteIdlCore.LiteralMap{
			Literals: map[string]*flyteIdlCore.Literal{
				"train":                  generateMockBlobLiteral(trainBlobLoc),
				"validation":             generateMockBlobLiteral(validationBlobLoc),
				"static_hyperparameters": utils.MakeGenericLiteral(shpStructObj),
			},
		}, nil)
	taskCtx.OnInputReader().Return(inputReader)

	outputReader := &pluginIOMocks.OutputWriter{}
	outputReader.OnGetOutputPath().Return(storage.DataReference("/data/outputs.pb"))
	outputReader.OnGetOutputPrefixPath().Return(storage.DataReference("/data/"))
	outputReader.OnGetRawOutputPrefix().Return(storage.DataReference("/raw/"))
	if outputReaderPutError {
		outputReader.OnPutMatch(mock.Anything).Return(errors.Errorf("err"))
	}
	taskCtx.OnOutputWriter().Return(outputReader)

	taskReader := &mocks.TaskReader{}
	taskReader.OnReadMatch(mock.Anything).Return(taskTemplate, nil)
	taskCtx.OnTaskReader().Return(taskReader)

	tID := &mocks.TaskExecutionID{}
	tID.OnGetID().Return(flyteIdlCore.TaskExecutionIdentifier{
		NodeExecutionId: &flyteIdlCore.NodeExecutionIdentifier{
			ExecutionId: &flyteIdlCore.WorkflowExecutionIdentifier{
				Name:    "my_name",
				Project: "my_project",
				Domain:  "my_domain",
			},
		},
	})
	tID.OnGetGeneratedName().Return("some-acceptable-name")

	resources := &mocks.TaskOverrides{}
	resources.OnGetResources().Return(resourceRequirements)

	taskExecutionMetadata := &mocks.TaskExecutionMetadata{}
	taskExecutionMetadata.OnGetTaskExecutionID().Return(tID)
	taskExecutionMetadata.OnGetNamespace().Return("test-namespace")
	taskExecutionMetadata.OnGetAnnotations().Return(map[string]string{"iam.amazonaws.com/role": "metadata_role"})
	taskExecutionMetadata.OnGetLabels().Return(map[string]string{"label-1": "val1"})
	taskExecutionMetadata.OnGetOwnerReference().Return(v1.OwnerReference{
		Kind: "node",
		Name: "blah",
	})
	taskExecutionMetadata.OnIsInterruptible().Return(true)
	taskExecutionMetadata.OnGetOverrides().Return(resources)
	taskExecutionMetadata.OnGetK8sServiceAccount().Return(serviceAccount)
	taskCtx.OnTaskExecutionMetadata().Return(taskExecutionMetadata)
	return taskCtx
}

func generateMockBlobLiteral(loc storage.DataReference) *flyteIdlCore.Literal {
	return &flyteIdlCore.Literal{
		Value: &flyteIdlCore.Literal_Scalar{
			Scalar: &flyteIdlCore.Scalar{
				Value: &flyteIdlCore.Scalar_Blob{
					Blob: &flyteIdlCore.Blob{
						Uri: loc.String(),
						Metadata: &flyteIdlCore.BlobMetadata{
							Type: &flyteIdlCore.BlobType{
								Dimensionality: flyteIdlCore.BlobType_SINGLE,
								Format:         "csv",
							},
						},
					},
				},
			},
		},
	}
}

func generateMockHyperparameterTuningJobTaskContext(taskTemplate *flyteIdlCore.TaskTemplate) pluginsCore.TaskExecutionContext {
	taskCtx := &mocks.TaskExecutionContext{}
	inputReader := &pluginIOMocks.InputReader{}
	inputReader.OnGetInputPrefixPath().Return(storage.DataReference("/input/prefix"))
	inputReader.OnGetInputPath().Return(storage.DataReference("/input"))

	trainBlobLoc := storage.DataReference("train-blob-loc")
	validationBlobLoc := storage.DataReference("validation-blob-loc")
	shp := map[string]string{"a": "1", "b": "2"}
	shpStructObj, _ := utils.MarshalObjToStruct(shp)
	hpoJobConfig := sagemakerIdl.HyperparameterTuningJobConfig{
		HyperparameterRanges: &sagemakerIdl.ParameterRanges{
			ParameterRangeMap: map[string]*sagemakerIdl.ParameterRangeOneOf{
				"a": {
					ParameterRangeType: &sagemakerIdl.ParameterRangeOneOf_IntegerParameterRange{
						IntegerParameterRange: &sagemakerIdl.IntegerParameterRange{
							MaxValue:    2,
							MinValue:    1,
							ScalingType: sagemakerIdl.HyperparameterScalingType_LINEAR,
						},
					},
				},
			},
		},
		TuningStrategy: sagemakerIdl.HyperparameterTuningStrategy_BAYESIAN,
		TuningObjective: &sagemakerIdl.HyperparameterTuningObjective{
			ObjectiveType: sagemakerIdl.HyperparameterTuningObjectiveType_MINIMIZE,
			MetricName:    "test:metric",
		},
		TrainingJobEarlyStoppingType: sagemakerIdl.TrainingJobEarlyStoppingType_AUTO,
	}
	hpoJobConfigByteArray, _ := proto.Marshal(&hpoJobConfig)

	inputReader.OnGetMatch(mock.Anything).Return(
		&flyteIdlCore.LiteralMap{
			Literals: map[string]*flyteIdlCore.Literal{
				"train":                            generateMockBlobLiteral(trainBlobLoc),
				"validation":                       generateMockBlobLiteral(validationBlobLoc),
				"static_hyperparameters":           utils.MakeGenericLiteral(shpStructObj),
				"hyperparameter_tuning_job_config": utils.MakeBinaryLiteral(hpoJobConfigByteArray),
			},
		}, nil)
	taskCtx.OnInputReader().Return(inputReader)

	outputReader := &pluginIOMocks.OutputWriter{}
	outputReader.OnGetOutputPath().Return(storage.DataReference("/data/outputs.pb"))
	outputReader.OnGetOutputPrefixPath().Return(storage.DataReference("/data/"))
	outputReader.OnGetRawOutputPrefix().Return(storage.DataReference("/raw/"))
	taskCtx.OnOutputWriter().Return(outputReader)

	taskReader := &mocks.TaskReader{}
	taskReader.OnReadMatch(mock.Anything).Return(taskTemplate, nil)
	taskCtx.OnTaskReader().Return(taskReader)
	taskExecutionMetadata := genMockTaskExecutionMetadata()
	taskCtx.OnTaskExecutionMetadata().Return(taskExecutionMetadata)
	return taskCtx
}

func genMockTaskExecutionMetadata() *mocks.TaskExecutionMetadata {
	tID := &mocks.TaskExecutionID{}
	tID.OnGetID().Return(flyteIdlCore.TaskExecutionIdentifier{
		NodeExecutionId: &flyteIdlCore.NodeExecutionIdentifier{
			ExecutionId: &flyteIdlCore.WorkflowExecutionIdentifier{
				Name:    "my_name",
				Project: "my_project",
				Domain:  "my_domain",
			},
		},
	})

	tID.OnGetGeneratedName().Return("some-acceptable-name")

	resources := &mocks.TaskOverrides{}
	resources.OnGetResources().Return(resourceRequirements)

	taskExecutionMetadata := &mocks.TaskExecutionMetadata{}
	taskExecutionMetadata.OnGetTaskExecutionID().Return(tID)
	taskExecutionMetadata.OnGetNamespace().Return("test-namespace")
	taskExecutionMetadata.OnGetAnnotations().Return(map[string]string{"iam.amazonaws.com/role": "metadata_role"})
	taskExecutionMetadata.OnGetLabels().Return(map[string]string{"label-1": "val1"})
	taskExecutionMetadata.OnGetOwnerReference().Return(v1.OwnerReference{
		Kind: "node",
		Name: "blah",
	})
	taskExecutionMetadata.OnIsInterruptible().Return(true)
	taskExecutionMetadata.OnGetOverrides().Return(resources)
	taskExecutionMetadata.OnGetK8sServiceAccount().Return(serviceAccount)
	return taskExecutionMetadata
}

// nolint
func generateMockTrainingJobCustomObj(
	inputMode sagemakerIdl.InputMode_Value, algName sagemakerIdl.AlgorithmName_Value, algVersion string,
	metricDefinitions []*sagemakerIdl.MetricDefinition, contentType sagemakerIdl.InputContentType_Value,
	instanceCount int64, instanceType string, volumeSizeInGB int64) *sagemakerIdl.TrainingJob {
	return &sagemakerIdl.TrainingJob{
		AlgorithmSpecification: &sagemakerIdl.AlgorithmSpecification{
			InputMode:         inputMode,
			AlgorithmName:     algName,
			AlgorithmVersion:  algVersion,
			MetricDefinitions: metricDefinitions,
			InputContentType:  contentType,
		},
		TrainingJobResourceConfig: &sagemakerIdl.TrainingJobResourceConfig{
			InstanceCount:  instanceCount,
			InstanceType:   instanceType,
			VolumeSizeInGb: volumeSizeInGB,
		},
	}
}

func generateMockHyperparameterTuningJobCustomObj(
	trainingJob *sagemakerIdl.TrainingJob, maxNumberOfTrainingJobs int64, maxParallelTrainingJobs int64) *sagemakerIdl.HyperparameterTuningJob {
	return &sagemakerIdl.HyperparameterTuningJob{
		TrainingJob:             trainingJob,
		MaxNumberOfTrainingJobs: maxNumberOfTrainingJobs,
		MaxParallelTrainingJobs: maxParallelTrainingJobs,
	}
}

func Test_awsSagemakerPlugin_BuildResourceForTrainingJob(t *testing.T) {
	// Default config does not contain a roleAnnotationKey -> expecting to get the role from default config
	ctx := context.TODO()
	defaultCfg := config.GetSagemakerConfig()
	defer func() {
		_ = config.SetSagemakerConfig(defaultCfg)
	}()

	t.Run("If roleAnnotationKey has a match, the role from the metadata should be fetched", func(t *testing.T) {
		// Injecting a config which contains a matching roleAnnotationKey -> expecting to get the role from metadata
		configAccessor := viper.NewAccessor(stdConfig.Options{
			StrictMode:  true,
			SearchPaths: []string{"testdata/config.yaml"},
		})

		err := configAccessor.UpdateConfig(context.TODO())
		assert.NoError(t, err)

		awsSageMakerTrainingJobHandler := awsSagemakerPlugin{TaskType: trainingJobTaskType}

		tjObj := generateMockTrainingJobCustomObj(
			sagemakerIdl.InputMode_FILE, sagemakerIdl.AlgorithmName_XGBOOST, "0.90", []*sagemakerIdl.MetricDefinition{},
			sagemakerIdl.InputContentType_TEXT_CSV, 1, "ml.m4.xlarge", 25)
		taskTemplate := generateMockTrainingJobTaskTemplate("the job x", tjObj)

		trainingJobResource, err := awsSageMakerTrainingJobHandler.BuildResource(ctx, generateMockTrainingJobTaskContext(taskTemplate, false))
		assert.NoError(t, err)
		assert.NotNil(t, trainingJobResource)

		trainingJob, ok := trainingJobResource.(*trainingjobv1.TrainingJob)
		assert.True(t, ok)
		assert.Equal(t, "metadata_role", *trainingJob.Spec.RoleArn)
	})

	t.Run("If roleAnnotationKey does not have a match, the role from the config should be fetched", func(t *testing.T) {
		// Injecting a config which contains a mismatched roleAnnotationKey -> expecting to get the role from the config
		configAccessor := viper.NewAccessor(stdConfig.Options{
			StrictMode: true,
			// Use a different
			SearchPaths: []string{"testdata/config2.yaml"},
		})

		err := configAccessor.UpdateConfig(context.TODO())
		assert.NoError(t, err)

		awsSageMakerTrainingJobHandler := awsSagemakerPlugin{TaskType: trainingJobTaskType}

		tjObj := generateMockTrainingJobCustomObj(
			sagemakerIdl.InputMode_FILE, sagemakerIdl.AlgorithmName_XGBOOST, "0.90", []*sagemakerIdl.MetricDefinition{},
			sagemakerIdl.InputContentType_TEXT_CSV, 1, "ml.m4.xlarge", 25)
		taskTemplate := generateMockTrainingJobTaskTemplate("the job y", tjObj)

		trainingJobResource, err := awsSageMakerTrainingJobHandler.BuildResource(ctx, generateMockTrainingJobTaskContext(taskTemplate, false))
		assert.NoError(t, err)
		assert.NotNil(t, trainingJobResource)

		trainingJob, ok := trainingJobResource.(*trainingjobv1.TrainingJob)
		assert.True(t, ok)
		assert.Equal(t, "config_role", *trainingJob.Spec.RoleArn)
	})

	t.Run("In a custom training job we should see the FLYTE_SAGEMAKER_CMD being injected", func(t *testing.T) {
		// Injecting a config which contains a mismatched roleAnnotationKey -> expecting to get the role from the config
		configAccessor := viper.NewAccessor(stdConfig.Options{
			StrictMode: true,
			// Use a different
			SearchPaths: []string{"testdata/config2.yaml"},
		})

		err := configAccessor.UpdateConfig(context.TODO())
		assert.NoError(t, err)

		awsSageMakerTrainingJobHandler := awsSagemakerPlugin{TaskType: trainingJobTaskType}

		tjObj := generateMockTrainingJobCustomObj(
			sagemakerIdl.InputMode_FILE, sagemakerIdl.AlgorithmName_XGBOOST, "0.90", []*sagemakerIdl.MetricDefinition{},
			sagemakerIdl.InputContentType_TEXT_CSV, 1, "ml.m4.xlarge", 25)
		taskTemplate := generateMockTrainingJobTaskTemplate("the job", tjObj)

		trainingJobResource, err := awsSageMakerTrainingJobHandler.BuildResource(ctx, generateMockTrainingJobTaskContext(taskTemplate, false))
		assert.NoError(t, err)
		assert.NotNil(t, trainingJobResource)

		trainingJob, ok := trainingJobResource.(*trainingjobv1.TrainingJob)
		assert.True(t, ok)
		assert.Equal(t, "config_role", *trainingJob.Spec.RoleArn)

		expectedHPs := []*commonv1.KeyValuePair{
			{Name: "a", Value: "1"},
			{Name: "b", Value: "2"},
		}

		assert.ElementsMatch(t,
			func(kvs []*commonv1.KeyValuePair) []commonv1.KeyValuePair {
				ret := make([]commonv1.KeyValuePair, 0, len(kvs))
				for _, kv := range kvs {
					ret = append(ret, *kv)
				}
				return ret
			}(expectedHPs),
			func(kvs []*commonv1.KeyValuePair) []commonv1.KeyValuePair {
				ret := make([]commonv1.KeyValuePair, 0, len(kvs))
				for _, kv := range kvs {
					ret = append(ret, *kv)
				}
				return ret
			}(trainingJob.Spec.HyperParameters))
	})
}

func Test_awsSagemakerPlugin_BuildResourceForCustomTrainingJob(t *testing.T) {
	// Default config does not contain a roleAnnotationKey -> expecting to get the role from default config
	ctx := context.TODO()
	defaultCfg := config.GetSagemakerConfig()
	defer func() {
		_ = config.SetSagemakerConfig(defaultCfg)
	}()
	t.Run("In a custom training job we should see the FLYTE_SAGEMAKER_CMD being injected", func(t *testing.T) {
		// Injecting a config which contains a mismatched roleAnnotationKey -> expecting to get the role from the config
		configAccessor := viper.NewAccessor(stdConfig.Options{
			StrictMode: true,
			// Use a different
			SearchPaths: []string{"testdata/config2.yaml"},
		})

		err := configAccessor.UpdateConfig(context.TODO())
		assert.NoError(t, err)

		awsSageMakerTrainingJobHandler := awsSagemakerPlugin{TaskType: customTrainingJobTaskType}

		tjObj := generateMockTrainingJobCustomObj(
			sagemakerIdl.InputMode_FILE, sagemakerIdl.AlgorithmName_CUSTOM, "0.90", []*sagemakerIdl.MetricDefinition{},
			sagemakerIdl.InputContentType_TEXT_CSV, 1, "ml.m4.xlarge", 25)
		taskTemplate := generateMockTrainingJobTaskTemplate("the job", tjObj)

		trainingJobResource, err := awsSageMakerTrainingJobHandler.BuildResource(ctx, generateMockCustomTrainingJobTaskContext(taskTemplate, false))
		assert.NoError(t, err)
		assert.NotNil(t, trainingJobResource)

		trainingJob, ok := trainingJobResource.(*trainingjobv1.TrainingJob)
		assert.True(t, ok)
		assert.Equal(t, "config_role", *trainingJob.Spec.RoleArn)
		//assert.Equal(t, 1, len(trainingJob.Spec.HyperParameters))
		fmt.Printf("%v", trainingJob.Spec.HyperParameters)
		expectedHPs := []*commonv1.KeyValuePair{
			{Name: FlyteSageMakerCmdKey, Value: "service_venv+pyflyte-execute"},
			{Name: fmt.Sprintf("%v%v%v", FlyteSageMakerCmdArgKeyPrefix, "test_opt1", FlyteSageMakerKeySuffix), Value: "value1"},
			{Name: fmt.Sprintf("%v%v%v", FlyteSageMakerCmdArgKeyPrefix, "test_opt2", FlyteSageMakerKeySuffix), Value: "value2"},
			{Name: fmt.Sprintf("%v%v%v", FlyteSageMakerCmdArgKeyPrefix, "test_flag", FlyteSageMakerKeySuffix), Value: ""},
			{Name: fmt.Sprintf("%v%v%v", FlyteSageMakerEnvVarKeyPrefix, "Env_Var", FlyteSageMakerKeySuffix), Value: "Env_Val"},
		}
		assert.Equal(t, len(expectedHPs), len(trainingJob.Spec.HyperParameters))
		for i := range expectedHPs {
			assert.Equal(t, expectedHPs[i].Name, trainingJob.Spec.HyperParameters[i].Name)
			assert.Equal(t, expectedHPs[i].Value, trainingJob.Spec.HyperParameters[i].Value)
		}

		//assert.Equal(t, 1, len(trainingJob.Spec.HyperParameters))
		//expectedCmd := "test-cmds1 test-cmds2 pyflyte-execute --test-opt1 value1 --test-opt2 value2 --test-flag --hp_int 1 --hp_float 1.5 --hp_bool false --hp_string a"
		//expectedCmd = strings.ReplaceAll(expectedCmd, " ", "+")
		//expectedHPs := []*commonv1.KeyValuePair{
		//	{Name: FlyteSageMakerCmdKey, Value: expectedCmd},
		//}
		//assert.Equal(t, expectedHPs[0].Name, trainingJob.Spec.HyperParameters[0].Name)

		//expectedSplit := strings.Split(expectedHPs[0].Value, "+")
		//sort.Strings(expectedSplit)
		//gotSplit := strings.Split(trainingJob.Spec.HyperParameters[0].Value, "+")
		//sort.Strings(gotSplit)
		//assert.Equal(t, expectedSplit, gotSplit)

		assert.Equal(t, testImage, *trainingJob.Spec.AlgorithmSpecification.TrainingImage)
	})
}

func Test_awsSagemakerPlugin_BuildResourceForHyperparameterTuningJob(t *testing.T) {
	// Default config does not contain a roleAnnotationKey -> expecting to get the role from default config
	ctx := context.TODO()
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

func Test_awsSagemakerPlugin_getEventInfoForJob(t *testing.T) {
	// Default config does not contain a roleAnnotationKey -> expecting to get the role from default config
	ctx := context.TODO()
	defaultCfg := config.GetSagemakerConfig()
	defer func() {
		_ = config.SetSagemakerConfig(defaultCfg)
	}()

	t.Run("get event info should return correctly formatted log links for training job", func(t *testing.T) {
		// Injecting a config which contains a mismatched roleAnnotationKey -> expecting to get the role from the config
		configAccessor := viper.NewAccessor(stdConfig.Options{
			StrictMode: true,
			// Use a different
			SearchPaths: []string{"testdata/config2.yaml"},
		})

		err := configAccessor.UpdateConfig(context.TODO())
		assert.NoError(t, err)

		awsSageMakerTrainingJobHandler := awsSagemakerPlugin{TaskType: trainingJobTaskType}

		tjObj := generateMockTrainingJobCustomObj(
			sagemakerIdl.InputMode_FILE, sagemakerIdl.AlgorithmName_XGBOOST, "0.90", []*sagemakerIdl.MetricDefinition{},
			sagemakerIdl.InputContentType_TEXT_CSV, 1, "ml.m4.xlarge", 25)
		taskTemplate := generateMockTrainingJobTaskTemplate("the job", tjObj)
		taskCtx := generateMockTrainingJobTaskContext(taskTemplate, false)
		trainingJobResource, err := awsSageMakerTrainingJobHandler.BuildResource(ctx, taskCtx)
		assert.NoError(t, err)
		assert.NotNil(t, trainingJobResource)

		trainingJob, ok := trainingJobResource.(*trainingjobv1.TrainingJob)
		assert.True(t, ok)

		taskInfo, err := awsSageMakerTrainingJobHandler.getEventInfoForJob(ctx, trainingJob)
		if err != nil {
			panic(err)
		}

		expectedTaskLogs := []*flyteIdlCore.TaskLog{
			{
				Uri: fmt.Sprintf("https://%s.console.aws.amazon.com/cloudwatch/home?region=%s#logStream:group=/aws/sagemaker/TrainingJobs;prefix=%s;streamFilter=typeLogStreamPrefix",
					"us-west-2", "us-west-2", taskCtx.TaskExecutionMetadata().GetTaskExecutionID().GetID().NodeExecutionId.GetExecutionId().GetName()),
				Name:          "CloudWatch Logs",
				MessageFormat: flyteIdlCore.TaskLog_JSON,
			},
			{
				Uri: fmt.Sprintf("https://%s.console.aws.amazon.com/sagemaker/home?region=%s#/%s/%s",
					"us-west-2", "us-west-2", "jobs", taskCtx.TaskExecutionMetadata().GetTaskExecutionID().GetID().NodeExecutionId.GetExecutionId().GetName()),
				Name:          "SageMaker Training Job",
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

func Test_awsSagemakerPlugin_BuildIdentityResource(t *testing.T) {
	ctx := context.TODO()
	type fields struct {
		TaskType pluginsCore.TaskType
	}
	type args struct {
		in0 context.Context
		in1 pluginsCore.TaskExecutionMetadata
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    k8s.Resource
		wantErr bool
	}{
		{name: "Training Job Identity Resource", fields: fields{TaskType: trainingJobTaskType},
			args: args{in0: ctx, in1: genMockTaskExecutionMetadata()}, want: &trainingjobv1.TrainingJob{}, wantErr: false},
		{name: "HPO Job Identity Resource", fields: fields{TaskType: hyperparameterTuningJobTaskType},
			args: args{in0: ctx, in1: genMockTaskExecutionMetadata()}, want: &hpojobv1.HyperparameterTuningJob{}, wantErr: false},
		{name: "Unsupported Job Identity Resource", fields: fields{TaskType: "bad type"},
			args: args{in0: ctx, in1: genMockTaskExecutionMetadata()}, want: nil, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := awsSagemakerPlugin{
				TaskType: tt.fields.TaskType,
			}
			got, err := m.BuildIdentityResource(tt.args.in0, tt.args.in1)
			if (err != nil) != tt.wantErr {
				t.Errorf("BuildIdentityResource() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BuildIdentityResource() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_createModelOutputPath(t *testing.T) {
	type args struct {
		job            k8s.Resource
		prefix         string
		bestExperiment string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{name: "training job: simple output path", args: args{job: &trainingjobv1.TrainingJob{}, prefix: "s3://my-bucket", bestExperiment: "job-ABC"},
			want: "s3://my-bucket/training_outputs/job-ABC/output/model.tar.gz"},
		{name: "hpo job: simple output path", args: args{job: &hpojobv1.HyperparameterTuningJob{}, prefix: "s3://my-bucket", bestExperiment: "job-ABC"},
			want: "s3://my-bucket/hyperparameter_tuning_outputs/job-ABC/output/model.tar.gz"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := createModelOutputPath(tt.args.job, tt.args.prefix, tt.args.bestExperiment); got != tt.want {
				t.Errorf("createModelOutputPath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_awsSagemakerPlugin_GetTaskPhaseForTrainingJob(t *testing.T) {
	ctx := context.TODO()
	// Injecting a config which contains a mismatched roleAnnotationKey -> expecting to get the role from the config
	configAccessor := viper.NewAccessor(stdConfig.Options{
		StrictMode: true,
		// Use a different
		SearchPaths: []string{"testdata/config2.yaml"},
	})

	err := configAccessor.UpdateConfig(context.TODO())
	assert.NoError(t, err)

	awsSageMakerTrainingJobHandler := awsSagemakerPlugin{TaskType: trainingJobTaskType}

	tjObj := generateMockTrainingJobCustomObj(
		sagemakerIdl.InputMode_FILE, sagemakerIdl.AlgorithmName_XGBOOST, "0.90", []*sagemakerIdl.MetricDefinition{},
		sagemakerIdl.InputContentType_TEXT_CSV, 1, "ml.m4.xlarge", 25)
	taskTemplate := generateMockTrainingJobTaskTemplate("the job", tjObj)
	taskCtx := generateMockTrainingJobTaskContext(taskTemplate, false)
	trainingJobResource, err := awsSageMakerTrainingJobHandler.BuildResource(ctx, taskCtx)
	assert.NoError(t, err)
	assert.NotNil(t, trainingJobResource)

	t.Run("ReconcilingTrainingJobStatus should lead to a retryable failure", func(t *testing.T) {

		trainingJob, ok := trainingJobResource.(*trainingjobv1.TrainingJob)
		assert.True(t, ok)
		trainingJob.Status.TrainingJobStatus = trainingjobController.ReconcilingTrainingJobStatus
		phaseInfo, err := awsSageMakerTrainingJobHandler.GetTaskPhaseForTrainingJob(ctx, taskCtx, trainingJob)
		assert.Nil(t, err)
		assert.Equal(t, phaseInfo.Phase(), pluginsCore.PhaseRetryableFailure)
		assert.Equal(t, phaseInfo.Err().GetKind(), flyteIdlCore.ExecutionError_USER)
		assert.Equal(t, phaseInfo.Err().GetCode(), trainingjobController.ReconcilingTrainingJobStatus)
		assert.Equal(t, phaseInfo.Err().GetMessage(), "")
	})
	t.Run("TrainingJobStatusFailed should be a permanent failure", func(t *testing.T) {
		trainingJob, ok := trainingJobResource.(*trainingjobv1.TrainingJob)
		assert.True(t, ok)
		trainingJob.Status.TrainingJobStatus = sagemaker.TrainingJobStatusFailed

		phaseInfo, err := awsSageMakerTrainingJobHandler.GetTaskPhaseForTrainingJob(ctx, taskCtx, trainingJob)
		assert.Nil(t, err)
		assert.Equal(t, phaseInfo.Phase(), pluginsCore.PhasePermanentFailure)
		assert.Equal(t, phaseInfo.Err().GetKind(), flyteIdlCore.ExecutionError_USER)
		assert.Equal(t, phaseInfo.Err().GetCode(), sagemaker.TrainingJobStatusFailed)
		assert.Equal(t, phaseInfo.Err().GetMessage(), "")
	})
	t.Run("TrainingJobStatusFailed should be a permanent failure", func(t *testing.T) {

		trainingJob, ok := trainingJobResource.(*trainingjobv1.TrainingJob)
		assert.True(t, ok)
		trainingJob.Status.TrainingJobStatus = sagemaker.TrainingJobStatusStopped

		phaseInfo, err := awsSageMakerTrainingJobHandler.GetTaskPhaseForTrainingJob(ctx, taskCtx, trainingJob)
		assert.Nil(t, err)
		assert.Equal(t, phaseInfo.Phase(), pluginsCore.PhaseRetryableFailure)
		assert.Equal(t, phaseInfo.Err().GetKind(), flyteIdlCore.ExecutionError_USER)
		assert.Equal(t, phaseInfo.Err().GetCode(), taskError.DownstreamSystemError)
		// We have a default message for TrainingJobStatusStopped
		assert.Equal(t, phaseInfo.Err().GetMessage(), "Training Job Stopped")
	})
	/*
		t.Run("TrainingJobStatusCompleted", func(t *testing.T) {
			taskCtx = generateMockTrainingJobTaskContext(taskTemplate, true)
			trainingJobResource, err := awsSageMakerTrainingJobHandler.BuildResource(ctx, taskCtx)
			assert.NoError(t, err)
			assert.NotNil(t, trainingJobResource)

			trainingJob, ok := trainingJobResource.(*trainingjobv1.TrainingJob)
			assert.True(t, ok)

			trainingJob.Status.TrainingJobStatus = sagemaker.TrainingJobStatusCompleted
			phaseInfo, err := awsSageMakerTrainingJobHandler.GetTaskPhaseForTrainingJob(ctx, taskCtx, trainingJob)
			assert.NotNil(t, err)
			assert.Equal(t, phaseInfo.Phase(), pluginsCore.PhaseUndefined)
		})
	*/
}

func Test_awsSagemakerPlugin_GetTaskPhaseForCustomTrainingJob(t *testing.T) {
	ctx := context.TODO()
	// Injecting a config which contains a mismatched roleAnnotationKey -> expecting to get the role from the config
	configAccessor := viper.NewAccessor(stdConfig.Options{
		StrictMode: true,
		// Use a different
		SearchPaths: []string{"testdata/config2.yaml"},
	})

	err := configAccessor.UpdateConfig(context.TODO())
	assert.NoError(t, err)

	awsSageMakerTrainingJobHandler := awsSagemakerPlugin{TaskType: customTrainingJobTaskType}

	tjObj := generateMockTrainingJobCustomObj(
		sagemakerIdl.InputMode_FILE, sagemakerIdl.AlgorithmName_XGBOOST, "0.90", []*sagemakerIdl.MetricDefinition{},
		sagemakerIdl.InputContentType_TEXT_CSV, 1, "ml.m4.xlarge", 25)
	taskTemplate := generateMockTrainingJobTaskTemplate("the job", tjObj)
	taskCtx := generateMockCustomTrainingJobTaskContext(taskTemplate, false)
	trainingJobResource, err := awsSageMakerTrainingJobHandler.BuildResource(ctx, taskCtx)
	assert.NoError(t, err)
	assert.NotNil(t, trainingJobResource)

	t.Run("TrainingJobStatusCompleted", func(t *testing.T) {
		taskCtx = generateMockCustomTrainingJobTaskContext(taskTemplate, false)
		trainingJobResource, err := awsSageMakerTrainingJobHandler.BuildResource(ctx, taskCtx)
		assert.NoError(t, err)
		assert.NotNil(t, trainingJobResource)

		trainingJob, ok := trainingJobResource.(*trainingjobv1.TrainingJob)
		assert.True(t, ok)

		trainingJob.Status.TrainingJobStatus = sagemaker.TrainingJobStatusCompleted
		phaseInfo, err := awsSageMakerTrainingJobHandler.GetTaskPhaseForCustomTrainingJob(ctx, taskCtx, trainingJob)
		assert.Nil(t, err)
		assert.Equal(t, phaseInfo.Phase(), pluginsCore.PhaseSuccess)
	})
	t.Run("OutputWriter.Put returns an error", func(t *testing.T) {
		taskCtx = generateMockCustomTrainingJobTaskContext(taskTemplate, true)
		trainingJobResource, err := awsSageMakerTrainingJobHandler.BuildResource(ctx, taskCtx)
		assert.NoError(t, err)
		assert.NotNil(t, trainingJobResource)

		trainingJob, ok := trainingJobResource.(*trainingjobv1.TrainingJob)
		assert.True(t, ok)

		trainingJob.Status.TrainingJobStatus = sagemaker.TrainingJobStatusCompleted
		phaseInfo, err := awsSageMakerTrainingJobHandler.GetTaskPhaseForCustomTrainingJob(ctx, taskCtx, trainingJob)
		assert.NotNil(t, err)
		assert.Equal(t, phaseInfo.Phase(), pluginsCore.PhaseUndefined)
	})
}

func init() {
	labeled.SetMetricKeys(contextutils.NamespaceKey)
}
