package sagemaker

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/lyft/flytestdlib/contextutils"
	"github.com/lyft/flytestdlib/promutils/labeled"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/k8s"

	"github.com/lyft/flyteplugins/go/tasks/plugins/k8s/sagemaker/config"

	stdConfig "github.com/lyft/flytestdlib/config"
	"github.com/lyft/flytestdlib/config/viper"

	hpojobv1 "github.com/aws/amazon-sagemaker-operator-for-k8s/api/v1/hyperparametertuningjob"
	trainingjobv1 "github.com/aws/amazon-sagemaker-operator-for-k8s/api/v1/trainingjob"
	flyteIdlCore "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	sagemakerIdl "github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins/sagemaker"
	pluginsCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/utils"
	"github.com/stretchr/testify/assert"
)

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
					"us-west-2", "us-west-2", taskCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()),
				Name:          "CloudWatch Logs",
				MessageFormat: flyteIdlCore.TaskLog_JSON,
			},
			{
				Uri: fmt.Sprintf("https://%s.console.aws.amazon.com/sagemaker/home?region=%s#/%s/%s",
					"us-west-2", "us-west-2", "jobs", taskCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()),
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

func init() {
	labeled.SetMetricKeys(contextutils.NamespaceKey)
}
