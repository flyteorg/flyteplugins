package sagemaker

import (
	"context"
	"fmt"
	"time"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/ioutils"
	"github.com/lyft/flytestdlib/logger"
	"github.com/pkg/errors"
	"k8s.io/client-go/kubernetes/scheme"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	pluginsCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/flytek8s"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/k8s"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/utils"

	commonv1 "github.com/aws/amazon-sagemaker-operator-for-k8s/api/v1/common"
	hpojobv1 "github.com/aws/amazon-sagemaker-operator-for-k8s/api/v1/hyperparametertuningjob"
	"github.com/aws/aws-sdk-go/service/sagemaker"

	taskError "github.com/lyft/flyteplugins/go/tasks/errors"

	. "github.com/aws/amazon-sagemaker-operator-for-k8s/controllers/controllertest"
	sagemakerSpec "github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins/sagemaker"
	"github.com/lyft/flyteplugins/go/tasks/plugins/k8s/sagemaker/config"
)

// Sanity test that the plugin implements method of k8s.Plugin
var _ k8s.Plugin = awsSagemakerPlugin{}

type awsSagemakerPlugin struct {
}

func (m awsSagemakerPlugin) BuildIdentityResource(ctx context.Context, taskCtx pluginsCore.TaskExecutionMetadata) (k8s.Resource, error) {
	return &hpojobv1.HyperparameterTuningJob{}, nil
}

func (m awsSagemakerPlugin) BuildResource(ctx context.Context, taskCtx pluginsCore.TaskExecutionContext) (k8s.Resource, error) {
	// TODO build the actual spec of the k8s resource from the taskCtx Some helpful code is already added
	taskTemplate, err := taskCtx.TaskReader().Read(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to fetch task specification")
	} else if taskTemplate == nil {
		return nil, errors.Errorf("nil task specification")
	}

	taskInput, err := taskCtx.InputReader().Get(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to fetch task inputs")
	}

	// Get inputs from literals
	inputLiterals := taskInput.GetLiterals()
	trainPathLiteral, ok := inputLiterals["train"]
	if !ok {
		return nil, errors.Errorf("Input not specified: [train]")
	}
	validatePathLiteral, ok := inputLiterals["validation"]
	if !ok {
		return nil, errors.Errorf("Input not specified: [validation]")
	}
	staticHyperparamsLiteral, ok := inputLiterals["static_hyperparameters"]
	if !ok {
		return nil, errors.Errorf("Input not specified: [static_hyperparameters]")
	}

	trainingJobStoppingConditionLiteral, ok := inputLiterals["stopping_condition"]
	if !ok {
		return nil, errors.Errorf("Input not specified: [stopping_condition]")
	}

	hpoJobConfigLiteral, ok := inputLiterals["hpo_job_config"]
	if !ok {
		return nil, errors.Errorf("Input not specified: [hpo_job_config]")
	}

	outputPath := createOutputPath(taskCtx.OutputWriter().GetOutputPrefixPath().String())

	// Unmarshal the custom field of the task template back into the HPOJob struct generated in flyteidl
	sagemakerHPOJob := sagemakerSpec.HPOJob{}
	err = utils.UnmarshalStruct(taskTemplate.GetCustom(), &sagemakerHPOJob)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid task specification for taskType [%s]", sagemakerTaskType)
	}

	// Convert the hyperparameters to the spec value
	staticHyperparams, err := convertStaticHyperparamsLiteralToSpecType(staticHyperparamsLiteral)
	if err != nil {
		return nil, errors.Wrapf(err, "could not convert static hyperparameters to spec type")
	}

	trainingJobStoppingCondition, err := convertStoppingConditionToSpecType(trainingJobStoppingConditionLiteral)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to convert stopping condition literal to spec type")
	}

	// hpo_job_config is marshaled into a byte array in flytekit, so will have to unmarshal it back
	hpoJobConfig, err := convertHPOJobConfigToSpecType(hpoJobConfigLiteral)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to convert hpo job config literal to spec type")
	}

	// If the container is part of the task template you can access it here
	container := taskTemplate.GetContainer()

	// When adding env vars there are some default env vars that are available, you can pass them through
	envVars := flytek8s.DecorateEnvVars(ctx, flytek8s.ToK8sEnvVar(container.GetEnv()), taskCtx.TaskExecutionMetadata().GetTaskExecutionID())
	_ = envVars

	taskName := taskCtx.TaskExecutionMetadata().GetTaskExecutionID().GetID().NodeExecutionId.GetExecutionId().GetName()

	trainingImageStr, err := getTrainingImage(sagemakerHPOJob.GetTrainingJob())
	if err != nil {
		return nil, errors.Wrapf(err, "failed to find the training image")
	}

	hpoJobParameterRanges := buildParameterRanges(hpoJobConfig)

	cfg := config.GetSagemakerConfig()

	hpoJob := &hpojobv1.HyperparameterTuningJob{
		Spec: hpojobv1.HyperparameterTuningJobSpec{
			HyperParameterTuningJobName: &taskName,
			HyperParameterTuningJobConfig: &commonv1.HyperParameterTuningJobConfig{
				ResourceLimits: &commonv1.ResourceLimits{
					MaxNumberOfTrainingJobs: ToInt64Ptr(sagemakerHPOJob.GetMaxNumberOfTrainingJobs()),
					MaxParallelTrainingJobs: ToInt64Ptr(sagemakerHPOJob.GetMaxParallelTrainingJobs()),
				},
				Strategy: getAPIHyperParameterTuningJobStrategyType(hpoJobConfig.GetTuningStrategy()),
				HyperParameterTuningJobObjective: &commonv1.HyperParameterTuningJobObjective{
					Type:       getAPIHyperparameterTuningObjectiveType(hpoJobConfig.GetTuningObjective().GetObjectiveType()),
					MetricName: ToStringPtr(hpoJobConfig.GetTuningObjective().GetMetricName()),
				},
				ParameterRanges:              hpoJobParameterRanges,
				TrainingJobEarlyStoppingType: "Auto",
			},
			TrainingJobDefinition: &commonv1.HyperParameterTrainingJobDefinition{
				StaticHyperParameters: staticHyperparams,
				AlgorithmSpecification: &commonv1.HyperParameterAlgorithmSpecification{
					TrainingImage:     ToStringPtr(trainingImageStr),
					TrainingInputMode: getAPITrainingInputMode(sagemakerHPOJob.GetTrainingJob().GetAlgorithmSpecification().GetInputMode()),
				},
				InputDataConfig: []commonv1.Channel{
					{
						ChannelName: ToStringPtr("train"),
						DataSource: &commonv1.DataSource{
							S3DataSource: &commonv1.S3DataSource{
								S3DataType: "S3Prefix",
								S3Uri:      ToStringPtr(trainPathLiteral.GetScalar().GetBlob().GetUri()),
							},
						},
						ContentType: ToStringPtr("text/csv"), // TODO: can this be derived from the task spec?
						InputMode:   "File",
					},
					{
						ChannelName: ToStringPtr("validation"),
						DataSource: &commonv1.DataSource{
							S3DataSource: &commonv1.S3DataSource{
								S3DataType: "S3Prefix",
								S3Uri:      ToStringPtr(validatePathLiteral.GetScalar().GetBlob().GetUri()),
							},
						},
						ContentType: ToStringPtr("text/csv"), // TODO: can this be derived from the task spec?
						InputMode:   "File",
					},
				},
				OutputDataConfig: &commonv1.OutputDataConfig{
					S3OutputPath: ToStringPtr(outputPath),
				},
				ResourceConfig: &commonv1.ResourceConfig{
					InstanceType:   sagemakerHPOJob.GetTrainingJob().GetTrainingJobConfig().GetInstanceType(),
					InstanceCount:  ToInt64Ptr(sagemakerHPOJob.GetTrainingJob().GetTrainingJobConfig().GetInstanceCount()),
					VolumeSizeInGB: ToInt64Ptr(sagemakerHPOJob.GetTrainingJob().GetTrainingJobConfig().GetVolumeSizeInGb()),
					VolumeKmsKeyId: ToStringPtr(""), // TODO: add to proto and flytekit
				},
				RoleArn: ToStringPtr(cfg.RoleArn),
				StoppingCondition: &commonv1.StoppingCondition{
					MaxRuntimeInSeconds:  ToInt64Ptr(trainingJobStoppingCondition.GetMaxRuntimeInSeconds()),
					MaxWaitTimeInSeconds: ToInt64Ptr(trainingJobStoppingCondition.GetMaxWaitTimeInSeconds()),
				},
			},
			Region: ToStringPtr(cfg.Region),
		},
	}

	return hpoJob, nil
}

func getEventInfoForHPOJob(job *hpojobv1.HyperparameterTuningJob) (*pluginsCore.TaskInfo, error) {
	cwLogURL := fmt.Sprintf("https://%s.console.aws.amazon.com/cloudwatch/home?region=%s#logStream:group=/aws/sagemaker/TrainingJobs;prefix=%s;streamFilter=typeLogStreamPrefix",
		*job.Spec.Region, *job.Spec.Region, *job.Spec.HyperParameterTuningJobName)
	smLogURL := fmt.Sprintf("https://%s.console.aws.amazon.com/sagemaker/home?region=%s#/hyper-tuning-jobs/%s",
		*job.Spec.Region, *job.Spec.Region, *job.Spec.HyperParameterTuningJobName)

	taskLogs := []*core.TaskLog{
		{
			Uri:           cwLogURL,
			Name:          "CloudWatch Logs",
			MessageFormat: core.TaskLog_JSON,
		},
		{
			Uri:           smLogURL,
			Name:          "SageMaker Hyperparameter Tuning Job",
			MessageFormat: core.TaskLog_UNKNOWN,
		},
	}

	customInfoMap := make(map[string]string)

	customInfo, err := utils.MarshalObjToStruct(customInfoMap)
	if err != nil {
		return nil, err
	}

	return &pluginsCore.TaskInfo{
		Logs:       taskLogs,
		CustomInfo: customInfo,
	}, nil
}

func getOutputs(ctx context.Context, tr pluginsCore.TaskReader, outputPath string) (*core.LiteralMap, error) {
	tk, err := tr.Read(ctx)
	if err != nil {
		return nil, err
	}
	if tk.Interface.Outputs != nil && tk.Interface.Outputs.Variables == nil {
		logger.Warnf(ctx, "No outputs declared in the output interface. Ignoring the generated outputs.")
		return nil, nil
	}

	// We know that for XGBoost task there is only one output to be generated
	if len(tk.Interface.Outputs.Variables) > 1 {
		return nil, fmt.Errorf("expected to generate more than one outputs of type [%v]", tk.Interface.Outputs.Variables)
	}
	op := &core.LiteralMap{}
	// TODO: @kumare3 OMG This looks scary, provide helper methods for this
	for k := range tk.Interface.Outputs.Variables {
		// if v != core.LiteralType_Blob{}
		op.Literals = make(map[string]*core.Literal)
		op.Literals[k] = &core.Literal{
			Value: &core.Literal_Scalar{
				Scalar: &core.Scalar{
					Value: &core.Scalar_Blob{
						Blob: &core.Blob{
							Metadata: &core.BlobMetadata{
								Type: &core.BlobType{Dimensionality: core.BlobType_SINGLE},
							},
							Uri: outputPath,
						},
					},
				},
			},
		}
	}
	return op, nil
}

func createOutputPath(prefix string) string {
	return fmt.Sprintf("%s/hpo_outputs", prefix)
}

func createModelOutputPath(prefix, bestExperiment string) string {
	return fmt.Sprintf("%s/%s/output/model.tar.gz", createOutputPath(prefix), bestExperiment)
}

func (m awsSagemakerPlugin) GetTaskPhase(ctx context.Context, pluginContext k8s.PluginContext, resource k8s.Resource) (pluginsCore.PhaseInfo, error) {
	job := resource.(*hpojobv1.HyperparameterTuningJob)
	info, err := getEventInfoForHPOJob(job)
	if err != nil {
		return pluginsCore.PhaseInfoUndefined, err
	}

	occurredAt := time.Now()
	switch job.Status.HyperParameterTuningJobStatus {
	case sagemaker.HyperParameterTuningJobStatusFailed:
		execError := &core.ExecutionError{
			Message: job.Status.Additional,
		}
		return pluginsCore.PhaseInfoFailed(pluginsCore.PhasePermanentFailure, execError, info), nil
	case sagemaker.HyperParameterTuningJobStatusStopped:
		reason := fmt.Sprintf("HPO Job Stopped")
		return pluginsCore.PhaseInfoRetryableFailure(taskError.DownstreamSystemError, reason, info), nil
	case sagemaker.HyperParameterTuningJobStatusCompleted:
		// Now that it is success we will set the outputs as expected by the task
		out, err := getOutputs(ctx, pluginContext.TaskReader(), createModelOutputPath(pluginContext.OutputWriter().GetOutputPrefixPath().String(), *job.Status.BestTrainingJob.TrainingJobName))
		if err != nil {
			logger.Errorf(ctx, "Failed to create outputs, err: %s", err)
			return pluginsCore.PhaseInfoUndefined, errors.Wrapf(err, "failed to create outputs for the task")
		}
		if err := pluginContext.OutputWriter().Put(ctx, ioutils.NewInMemoryOutputReader(out, nil)); err != nil {
			return pluginsCore.PhaseInfoUndefined, err
		}
		logger.Debugf(ctx, "Successfully produced and returned outputs")
		return pluginsCore.PhaseInfoSuccess(info), nil
	case "":
		return pluginsCore.PhaseInfoQueued(occurredAt, pluginsCore.DefaultPhaseVersion, "job submitted"), nil
	}

	return pluginsCore.PhaseInfoRunning(pluginsCore.DefaultPhaseVersion, info), nil
}

func init() {
	if err := commonv1.AddToScheme(scheme.Scheme); err != nil {
		panic(err)
	}

	// Registering the plugin
	pluginmachinery.PluginRegistry().RegisterK8sPlugin(
		k8s.PluginEntry{
			ID:                  pluginID,
			RegisteredTaskTypes: []pluginsCore.TaskType{sagemakerTaskType},
			ResourceToWatch:     &hpojobv1.HyperparameterTuningJob{},
			Plugin:              awsSagemakerPlugin{},
			IsDefault:           false,
		})
}
