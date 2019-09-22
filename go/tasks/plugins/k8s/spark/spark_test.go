package spark

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/lyft/flytestdlib/storage"

	"github.com/lyft/flyteplugins/go/tasks/logs"

	pluginsCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/utils"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core/mocks"

	pluginIOMocks "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io/mocks"

	sj "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta1"
	"github.com/golang/protobuf/jsonpb"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const sparkApplicationFile = "local:///spark_app.py"
const testImage = "image://"
const sparkUIAddress = "spark-ui.flyte"

var (
	dummySparkConf = map[string]string{
		"spark.driver.memory":      "500M",
		"spark.driver.cores":       "1",
		"spark.executor.cores":     "1",
		"spark.executor.instances": "3",
		"spark.executor.memory":    "500M",
	}

	dummyEnvVars = []*core.KeyValuePair{
		{Key: "Env_Var", Value: "Env_Val"},
	}

	testArgs = []string{
		"execute-spark-task",
	}
)

func TestGetApplicationType(t *testing.T) {
	assert.Equal(t, getApplicationType(plugins.SparkApplication_PYTHON), sj.PythonApplicationType)
	assert.Equal(t, getApplicationType(plugins.SparkApplication_R), sj.RApplicationType)
	assert.Equal(t, getApplicationType(plugins.SparkApplication_JAVA), sj.JavaApplicationType)
	assert.Equal(t, getApplicationType(plugins.SparkApplication_SCALA), sj.ScalaApplicationType)
}

func TestGetEventInfo(t *testing.T) {
	assert.NoError(t, logs.SetLogConfig(&logs.LogConfig{
		IsCloudwatchEnabled: true,
		CloudwatchRegion:    "us-east-1",
		CloudwatchLogGroup:  "/kubernetes/flyte",
		IsKubernetesEnabled: true,
		KubernetesURL:       "k8s.com",
	}))
	info, err := getEventInfoForSpark(dummySparkApplication(sj.RunningState))
	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("https://%s", sparkUIAddress), info.CustomInfo.Fields[sparkDriverUI].GetStringValue())
	assert.Equal(t, "k8s.com/#!/log/spark-namespace/spark-pod/pod?namespace=spark-namespace", info.Logs[0].Uri)
	assert.Equal(t, "https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logStream:group=/kubernetes/flyte;prefix=var.log.containers.spark-pod;streamFilter=typeLogStreamPrefix", info.Logs[1].Uri)
	assert.Equal(t, "https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logStream:group=/kubernetes/flyte;prefix=system_log.var.log.containers.spark-app-name;streamFilter=typeLogStreamPrefix", info.Logs[2].Uri)
	assert.Equal(t, "https://spark-ui.flyte", info.Logs[3].Uri)

	assert.NoError(t, setSparkConfig(&Config{
		SparkHistoryServerURL: "spark-history.flyte",
	}))

	info, err = getEventInfoForSpark(dummySparkApplication(sj.FailedState))
	assert.NoError(t, err)
	assert.Equal(t, "spark-history.flyte/history/app-id", info.CustomInfo.Fields[sparkHistoryUI].GetStringValue())
	assert.Equal(t, "k8s.com/#!/log/spark-namespace/spark-pod/pod?namespace=spark-namespace", info.Logs[0].Uri)
	assert.Equal(t, "https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logStream:group=/kubernetes/flyte;prefix=var.log.containers.spark-pod;streamFilter=typeLogStreamPrefix", info.Logs[1].Uri)
	assert.Equal(t, "https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logStream:group=/kubernetes/flyte;prefix=system_log.var.log.containers.spark-app-name;streamFilter=typeLogStreamPrefix", info.Logs[2].Uri)
	assert.Equal(t, "spark-history.flyte/history/app-id", info.Logs[3].Uri)
}

func TestGetTaskPhase(t *testing.T) {
	sparkResourceHandler := sparkResourceHandler{}

	ctx := context.TODO()
	taskPhase, err := sparkResourceHandler.GetTaskPhase(ctx, nil, dummySparkApplication(sj.NewState))
	assert.NoError(t, err)
	assert.Equal(t, taskPhase.Phase(), pluginsCore.PhaseQueued)
	assert.NotNil(t, taskPhase.Info())
	assert.Nil(t, err)

	taskPhase, err = sparkResourceHandler.GetTaskPhase(ctx, nil, dummySparkApplication(sj.SubmittedState))
	assert.NoError(t, err)
	assert.Equal(t, taskPhase.Phase(), pluginsCore.PhaseQueued)
	assert.NotNil(t, taskPhase.Info())
	assert.Nil(t, err)

	taskPhase, err = sparkResourceHandler.GetTaskPhase(ctx, nil, dummySparkApplication(sj.RunningState))
	assert.NoError(t, err)
	assert.Equal(t, taskPhase.Phase(), pluginsCore.PhaseRunning)
	assert.NotNil(t, taskPhase.Info())
	assert.Nil(t, err)

	taskPhase, err = sparkResourceHandler.GetTaskPhase(ctx, nil, dummySparkApplication(sj.CompletedState))
	assert.NoError(t, err)
	assert.Equal(t, taskPhase.Phase(), pluginsCore.PhaseSuccess)
	assert.NotNil(t, taskPhase.Info())
	assert.Nil(t, err)

	taskPhase, err = sparkResourceHandler.GetTaskPhase(ctx, nil, dummySparkApplication(sj.InvalidatingState))
	assert.NoError(t, err)
	assert.Equal(t, taskPhase.Phase(), pluginsCore.PhaseRunning)
	assert.NotNil(t, taskPhase.Info())
	assert.Nil(t, err)

	taskPhase, err = sparkResourceHandler.GetTaskPhase(ctx, nil, dummySparkApplication(sj.FailingState))
	assert.NoError(t, err)
	assert.Equal(t, taskPhase.Phase(), pluginsCore.PhaseRunning)
	assert.NotNil(t, taskPhase.Info())
	assert.Nil(t, err)

	taskPhase, err = sparkResourceHandler.GetTaskPhase(ctx, nil, dummySparkApplication(sj.PendingRerunState))
	assert.NoError(t, err)
	assert.Equal(t, taskPhase.Phase(), pluginsCore.PhaseRunning)
	assert.NotNil(t, taskPhase.Info())
	assert.Nil(t, err)

	taskPhase, err = sparkResourceHandler.GetTaskPhase(ctx, nil, dummySparkApplication(sj.SucceedingState))
	assert.NoError(t, err)
	assert.Equal(t, taskPhase.Phase(), pluginsCore.PhaseRunning)
	assert.NotNil(t, taskPhase.Info())
	assert.Nil(t, err)

	taskPhase, err = sparkResourceHandler.GetTaskPhase(ctx, nil, dummySparkApplication(sj.FailedSubmissionState))
	assert.NoError(t, err)
	assert.Equal(t, taskPhase.Phase(), pluginsCore.PhaseRetryableFailure)
	assert.NotNil(t, taskPhase.Info())
	assert.Nil(t, err)

	taskPhase, err = sparkResourceHandler.GetTaskPhase(ctx, nil, dummySparkApplication(sj.FailedState))
	assert.NoError(t, err)
	assert.Equal(t, taskPhase.Phase(), pluginsCore.PhaseRetryableFailure)
	assert.NotNil(t, taskPhase.Info())
	assert.Nil(t, err)
}

func dummySparkApplication(state sj.ApplicationStateType) *sj.SparkApplication {

	return &sj.SparkApplication{
		ObjectMeta: v1.ObjectMeta{
			Name:      "spark-app-name",
			Namespace: "spark-namespace",
		},
		Status: sj.SparkApplicationStatus{
			SparkApplicationID: "app-id",
			AppState: sj.ApplicationState{
				State: state,
			},
			DriverInfo: sj.DriverInfo{
				PodName:             "spark-pod",
				WebUIIngressAddress: sparkUIAddress,
			},
			ExecutionAttempts: 1,
		},
	}
}

func dummySparkCustomObj() *plugins.SparkJob {
	sparkJob := plugins.SparkJob{}

	sparkJob.MainApplicationFile = sparkApplicationFile
	sparkJob.SparkConf = dummySparkConf
	sparkJob.ApplicationType = plugins.SparkApplication_PYTHON
	return &sparkJob
}

func dummySparkTaskTemplate(id string) *core.TaskTemplate {

	sparkJob := dummySparkCustomObj()
	sparkJobJSON, err := utils.MarshalToString(sparkJob)
	if err != nil {
		panic(err)
	}

	structObj := structpb.Struct{}

	err = jsonpb.UnmarshalString(sparkJobJSON, &structObj)
	if err != nil {
		panic(err)
	}

	return &core.TaskTemplate{
		Id:   &core.Identifier{Name: id},
		Type: "container",
		Target: &core.TaskTemplate_Container{
			Container: &core.Container{
				Image: testImage,
				Args:  testArgs,
				Env:   dummyEnvVars,
			},
		},
		Custom: &structObj,
	}
}

func dummySparkTaskContext(taskTemplate *core.TaskTemplate) pluginsCore.TaskExecutionContext {
	taskCtx := &mocks.TaskExecutionContext{}
	inputReader := &pluginIOMocks.InputReader{}
	inputReader.On("GetInputPath").Return(storage.DataReference("/input"))
	inputReader.On("Get", mock.Anything).Return(&core.LiteralMap{}, nil)
	taskCtx.On("InputReader").Return(inputReader)

	outputReader := &pluginIOMocks.OutputWriter{}
	outputReader.On("GetOutputPath").Return(storage.DataReference("/data/outputs.pb"))
	outputReader.On("GetOutputPrefixPath").Return(storage.DataReference("/data/"))
	taskCtx.On("OutputWriter").Return(outputReader)

	taskReader := &mocks.TaskReader{}
	taskReader.On("Read", mock.Anything).Return(taskTemplate, nil)
	taskCtx.On("TaskReader").Return(taskReader)

	tID := &mocks.TaskExecutionID{}
	tID.On("GetID").Return(core.TaskExecutionIdentifier{
		NodeExecutionId: &core.NodeExecutionIdentifier{
			ExecutionId: &core.WorkflowExecutionIdentifier{
				Name:    "my_name",
				Project: "my_project",
				Domain:  "my_domain",
			},
		},
	})
	tID.On("GetGeneratedName").Return("some-acceptable-name")

	taskExecutionMetadata := &mocks.TaskExecutionMetadata{}
	taskExecutionMetadata.On("GetTaskExecutionID").Return(tID)
	taskExecutionMetadata.On("GetNamespace").Return("test-namespace")
	taskExecutionMetadata.On("GetAnnotations").Return(map[string]string{"annotation-1": "val1"})
	taskExecutionMetadata.On("GetLabels").Return(map[string]string{"label-1": "val1"})
	taskExecutionMetadata.On("GetOwnerReference").Return(v1.OwnerReference{
		Kind: "node",
		Name: "blah",
	})

	taskCtx.On("TaskExecutionMetadata").Return(taskExecutionMetadata)
	return taskCtx
}

func TestBuildResourceSpark(t *testing.T) {
	sparkResourceHandler := sparkResourceHandler{}

	// Case1: Valid Spark Task-Template
	taskTemplate := dummySparkTaskTemplate("blah-1")

	resource, err := sparkResourceHandler.BuildResource(context.TODO(), dummySparkTaskContext(taskTemplate))
	assert.Nil(t, err)

	assert.NotNil(t, resource)
	sparkApp, ok := resource.(*sj.SparkApplication)
	assert.True(t, ok)
	assert.Equal(t, sparkApplicationFile, *sparkApp.Spec.MainApplicationFile)
	assert.Equal(t, sj.PythonApplicationType, sparkApp.Spec.Type)
	assert.Equal(t, testArgs, sparkApp.Spec.Arguments)
	assert.Equal(t, testImage, *sparkApp.Spec.Image)

	for confKey, confVal := range dummySparkConf {
		exists := false
		for k, v := range sparkApp.Spec.SparkConf {
			if k == confKey {
				assert.Equal(t, v, confVal)
				exists = true
			}
		}
		assert.True(t, exists)
	}

	assert.Equal(t, dummySparkConf["spark.driver.cores"], sparkApp.Spec.SparkConf["spark.kubernetes.driver.limit.cores"])
	assert.Equal(t, dummySparkConf["spark.executor.cores"], sparkApp.Spec.SparkConf["spark.kubernetes.executor.limit.cores"])

	// Case2: Invalid Spark Task-Template
	taskTemplate.Custom = nil
	resource, err = sparkResourceHandler.BuildResource(context.TODO(), dummySparkTaskContext(taskTemplate))
	assert.NotNil(t, err)
	assert.Nil(t, resource)
}
