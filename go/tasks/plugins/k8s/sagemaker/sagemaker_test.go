package sagemaker

import (
	"context"
	"fmt"
	"testing"

	stdConfig "github.com/lyft/flytestdlib/config"
	"github.com/lyft/flytestdlib/config/viper"

	trainingjobv1 "github.com/aws/amazon-sagemaker-operator-for-k8s/api/v1/trainingjob"
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
		"test-args",
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
	fmt.Printf("%v", tjObjJSON)
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
				Image: testImage,
				Args:  testArgs,
				Env:   dummyEnvVars,
			},
		},
		Custom: &structObj,
	}
}

func generateMockTrainingJobTaskContext(taskTemplate *flyteIdlCore.TaskTemplate) pluginsCore.TaskExecutionContext {
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
				"train": {
					Value: &flyteIdlCore.Literal_Scalar{
						Scalar: &flyteIdlCore.Scalar{
							Value: &flyteIdlCore.Scalar_Blob{
								Blob: &flyteIdlCore.Blob{
									Uri: trainBlobLoc.String(),
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
				},
				"validation": {
					Value: &flyteIdlCore.Literal_Scalar{
						Scalar: &flyteIdlCore.Scalar{
							Value: &flyteIdlCore.Scalar_Blob{
								Blob: &flyteIdlCore.Blob{
									Uri: validationBlobLoc.String(),
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
				},
				"static_hyperparameters": utils.MakeGenericLiteral(shpStructObj),
			},
		}, nil)
	taskCtx.OnInputReader().Return(inputReader)

	outputReader := &pluginIOMocks.OutputWriter{}
	outputReader.OnGetOutputPath().Return(storage.DataReference("/data/outputs.pb"))
	outputReader.OnGetOutputPrefixPath().Return(storage.DataReference("/data/"))
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

func Test_awsSagemakerPlugin_BuildResourceForTrainingJob(t *testing.T) {
	// Default config does not contain a roleAnnotationKey -> expecting to get the role from default config
	ctx := context.TODO()

	awsSageMakerTrainingJobHandler := awsSagemakerPlugin{TaskType: trainingJobTaskType}

	tjObj := generateMockTrainingJobCustomObj(
		sagemakerIdl.InputMode_FILE, sagemakerIdl.AlgorithmName_XGBOOST, "0.90", []*sagemakerIdl.MetricDefinition{},
		sagemakerIdl.InputContentType_TEXT_CSV, 1, "ml.m4.xlarge", 25)
	taskTemplate := generateMockTrainingJobTaskTemplate("the job", tjObj)

	trainingJobResource, err := awsSageMakerTrainingJobHandler.BuildResource(ctx, generateMockTrainingJobTaskContext(taskTemplate))
	assert.NoError(t, err)
	assert.NotNil(t, trainingJobResource)

	trainingJob, ok := trainingJobResource.(*trainingjobv1.TrainingJob)
	assert.True(t, ok)
	assert.Equal(t, "default_role", *trainingJob.Spec.RoleArn)
	assert.Equal(t, "File", string(trainingJob.Spec.AlgorithmSpecification.TrainingInputMode))

	// Injecting a config which contains a matching roleAnnotationKey -> expecting to get the role from metadata
	configAccessor := viper.NewAccessor(stdConfig.Options{
		StrictMode:  true,
		SearchPaths: []string{"testdata/config.yaml"},
	})

	err = configAccessor.UpdateConfig(context.TODO())
	assert.NoError(t, err)

	awsSageMakerTrainingJobHandler = awsSagemakerPlugin{TaskType: trainingJobTaskType}

	tjObj = generateMockTrainingJobCustomObj(
		sagemakerIdl.InputMode_FILE, sagemakerIdl.AlgorithmName_XGBOOST, "0.90", []*sagemakerIdl.MetricDefinition{},
		sagemakerIdl.InputContentType_TEXT_CSV, 1, "ml.m4.xlarge", 25)
	taskTemplate = generateMockTrainingJobTaskTemplate("the job", tjObj)

	trainingJobResource, err = awsSageMakerTrainingJobHandler.BuildResource(ctx, generateMockTrainingJobTaskContext(taskTemplate))
	assert.NoError(t, err)
	assert.NotNil(t, trainingJobResource)

	trainingJob, ok = trainingJobResource.(*trainingjobv1.TrainingJob)
	assert.True(t, ok)
	assert.Equal(t, "metadata_role", *trainingJob.Spec.RoleArn)

	// Injecting a config which contains a mismatched roleAnnotationKey -> expecting to get the role from the config
	configAccessor = viper.NewAccessor(stdConfig.Options{
		StrictMode: true,
		// Use a different
		SearchPaths: []string{"testdata/config2.yaml"},
	})

	err = configAccessor.UpdateConfig(context.TODO())
	assert.NoError(t, err)

	awsSageMakerTrainingJobHandler = awsSagemakerPlugin{TaskType: trainingJobTaskType}

	tjObj = generateMockTrainingJobCustomObj(
		sagemakerIdl.InputMode_FILE, sagemakerIdl.AlgorithmName_XGBOOST, "0.90", []*sagemakerIdl.MetricDefinition{},
		sagemakerIdl.InputContentType_TEXT_CSV, 1, "ml.m4.xlarge", 25)
	taskTemplate = generateMockTrainingJobTaskTemplate("the job", tjObj)

	trainingJobResource, err = awsSageMakerTrainingJobHandler.BuildResource(ctx, generateMockTrainingJobTaskContext(taskTemplate))
	assert.NoError(t, err)
	assert.NotNil(t, trainingJobResource)

	trainingJob, ok = trainingJobResource.(*trainingjobv1.TrainingJob)
	assert.True(t, ok)
	assert.Equal(t, "config_role", *trainingJob.Spec.RoleArn)
}
