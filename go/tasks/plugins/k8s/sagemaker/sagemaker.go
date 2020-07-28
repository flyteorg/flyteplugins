package sagemaker

import (
	"context"
	"fmt"
	"time"

	hpojobController "github.com/aws/amazon-sagemaker-operator-for-k8s/controllers/hyperparametertuningjob"
	trainingjobController "github.com/aws/amazon-sagemaker-operator-for-k8s/controllers/trainingjob"
	"github.com/lyft/flytestdlib/logger"
	"github.com/pkg/errors"
	"k8s.io/client-go/kubernetes/scheme"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/ioutils"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"

	pluginsCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/k8s"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/utils"

	commonv1 "github.com/aws/amazon-sagemaker-operator-for-k8s/api/v1/common"
	hpojobv1 "github.com/aws/amazon-sagemaker-operator-for-k8s/api/v1/hyperparametertuningjob"
	trainingjobv1 "github.com/aws/amazon-sagemaker-operator-for-k8s/api/v1/trainingjob"
	"github.com/aws/aws-sdk-go/service/sagemaker"

	taskError "github.com/lyft/flyteplugins/go/tasks/errors"

	sagemakerSpec "github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins/sagemaker"

	"github.com/lyft/flyteplugins/go/tasks/plugins/k8s/sagemaker/config"
)

// Sanity test that the plugin implements method of k8s.Plugin
var _ k8s.Plugin = awsSagemakerPlugin{}

type awsSagemakerPlugin struct {
	TaskType pluginsCore.TaskType
}

func (m awsSagemakerPlugin) BuildIdentityResource(_ context.Context, _ pluginsCore.TaskExecutionMetadata) (k8s.Resource, error) {
	if m.TaskType == trainingJobTaskType {
		return &trainingjobv1.TrainingJob{}, nil
	}
	if m.TaskType == hpoJobTaskType {
		return &hpojobv1.HyperparameterTuningJob{}, nil
	}
	return nil, errors.Errorf("The sagemaker plugin is unable to build identity resource for an unknown task type [%v]", m.TaskType)
}

func (m awsSagemakerPlugin) BuildResourceForTrainingJob(
	ctx context.Context, taskCtx pluginsCore.TaskExecutionContext) (k8s.Resource, error) {

	logger.Infof(ctx, "Building a training job resource for task [%v]", taskCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName())
	taskTemplate, err := getTaskTemplate(ctx, taskCtx)
	if err != nil {
		return nil, err
	}

	// Unmarshal the custom field of the task template back into the HPOJob struct generated in flyteidl
	sagemakerTrainingJob := sagemakerSpec.TrainingJob{}
	err = utils.UnmarshalStruct(taskTemplate.GetCustom(), &sagemakerTrainingJob)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid TrainingJob task specification: not able to unmarshal the custom field to [%s]", m.TaskType)
	}

	taskInput, err := taskCtx.InputReader().Get(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to fetch task inputs")
	}

	// Get inputs from literals
	inputLiterals := taskInput.GetLiterals()

	trainPathLiteral, ok := inputLiterals["train"]
	if !ok {
		return nil, errors.Errorf("Required input not specified: [train]")
	}
	validatePathLiteral, ok := inputLiterals["validation"]
	if !ok {
		return nil, errors.Errorf("Required input not specified: [validation]")
	}
	staticHyperparamsLiteral, ok := inputLiterals["static_hyperparameters"]
	if !ok {
		return nil, errors.Errorf("Required input not specified: [static_hyperparameters]")
	}

	outputPath := createOutputPath(taskCtx.OutputWriter().GetOutputPrefixPath().String())

	// Convert the hyperparameters to the spec value
	staticHyperparams, err := convertStaticHyperparamsLiteralToSpecType(staticHyperparamsLiteral)
	if err != nil {
		return nil, errors.Wrapf(err, "could not convert static hyperparameters to spec type")
	}

	taskName := taskCtx.TaskExecutionMetadata().GetTaskExecutionID().GetID().NodeExecutionId.GetExecutionId().GetName()

	trainingImageStr, err := getTrainingImage(&sagemakerTrainingJob)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to find the training image")
	}

	logger.Infof(ctx, "The Sagemaker TrainingJob Task plugin received static hyperparameters [%v]", staticHyperparams)

	cfg := config.GetSagemakerConfig()

	trainingJob := &trainingjobv1.TrainingJob{
		Spec: trainingjobv1.TrainingJobSpec{
			AlgorithmSpecification: &commonv1.AlgorithmSpecification{
				AlgorithmName:     nil, // TODO: to add
				TrainingImage:     ToStringPtr(trainingImageStr),
				TrainingInputMode: getAPITrainingInputMode(sagemakerTrainingJob.GetAlgorithmSpecification().GetInputMode()),
				MetricDefinitions: nil, // TODO: to add
			},
			EnableManagedSpotTraining: nil,
			HyperParameters:           staticHyperparams,
			InputDataConfig: []commonv1.Channel{
				{
					ChannelName: ToStringPtr("train"),
					DataSource: &commonv1.DataSource{
						S3DataSource: &commonv1.S3DataSource{
							S3DataType: "S3Prefix",
							S3Uri:      ToStringPtr(trainPathLiteral.GetScalar().GetBlob().GetUri()),
						},
					},
					ContentType: ToStringPtr("text/csv"), // TODO: can this be derived from the BlobMetadata?
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
					ContentType: ToStringPtr("text/csv"), // TODO: can this be derived from the BlobMetadata?
					InputMode:   "File",
				},
			},
			OutputDataConfig: &commonv1.OutputDataConfig{
				S3OutputPath: ToStringPtr(outputPath),
			},
			CheckpointConfig: nil,
			ResourceConfig: &commonv1.ResourceConfig{
				InstanceType:   sagemakerTrainingJob.GetTrainingJobResourceConfig().GetInstanceType(),
				InstanceCount:  ToInt64Ptr(sagemakerTrainingJob.GetTrainingJobResourceConfig().GetInstanceCount()),
				VolumeSizeInGB: ToInt64Ptr(sagemakerTrainingJob.GetTrainingJobResourceConfig().GetVolumeSizeInGb()),
				VolumeKmsKeyId: ToStringPtr(""), // TODO: Not yet supported. Need to add to proto and flytekit in the future
			},
			RoleArn: ToStringPtr(cfg.RoleArn),
			Region:  ToStringPtr(cfg.Region),
			StoppingCondition: &commonv1.StoppingCondition{
				MaxRuntimeInSeconds:  nil, // TODO: decide how to coordinate this and Flyte's timeout
				MaxWaitTimeInSeconds: nil, // TODO: decide how to coordinate this and Flyte's timeout and queueing budget
			},
			TensorBoardOutputConfig: nil,
			Tags:                    nil,
			TrainingJobName:         &taskName,
		},
	}
	logger.Infof(ctx, "Successfully built a training job resource for task [%v]", taskCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName())
	return trainingJob, nil
}

func (m awsSagemakerPlugin) BuildResourceForHPOJob(
	ctx context.Context, taskCtx pluginsCore.TaskExecutionContext) (k8s.Resource, error) {

	logger.Infof(ctx, "Building a hpo job resource for task [%v]", taskCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName())

	taskTemplate, err := getTaskTemplate(ctx, taskCtx)
	if err != nil {
		return nil, err
	}

	// Unmarshal the custom field of the task template back into the HPOJob struct generated in flyteidl
	sagemakerHPOJob := sagemakerSpec.HyperparameterTuningJob{}
	err = utils.UnmarshalStruct(taskTemplate.GetCustom(), &sagemakerHPOJob)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid HPOJob task specification: not able to unmarshal the custom field to [%s]", hpoJobTaskType)
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
		return nil, errors.Errorf("Required input not specified: [validation]")
	}
	staticHyperparamsLiteral, ok := inputLiterals["static_hyperparameters"]
	if !ok {
		return nil, errors.Errorf("Required input not specified: [static_hyperparameters]")
	}

	hpoJobConfigLiteral, ok := inputLiterals["hpo_job_config"]
	if !ok {
		return nil, errors.Errorf("Required input not specified: [hpo_job_config]")
	}

	outputPath := createOutputPath(taskCtx.OutputWriter().GetOutputPrefixPath().String())

	// Convert the hyperparameters to the spec value
	staticHyperparams, err := convertStaticHyperparamsLiteralToSpecType(staticHyperparamsLiteral)
	if err != nil {
		return nil, errors.Wrapf(err, "could not convert static hyperparameters to spec type")
	}

	// hpo_job_config is marshaled into a byte array in flytekit, so will have to unmarshal it back
	hpoJobConfig, err := convertHPOJobConfigToSpecType(hpoJobConfigLiteral)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to convert hpo job config literal to spec type")
	}

	taskName := taskCtx.TaskExecutionMetadata().GetTaskExecutionID().GetID().NodeExecutionId.GetExecutionId().GetName()

	trainingImageStr, err := getTrainingImage(sagemakerHPOJob.GetTrainingJob())
	if err != nil {
		return nil, errors.Wrapf(err, "failed to find the training image")
	}

	hpoJobParameterRanges := buildParameterRanges(hpoJobConfig)

	logger.Infof(ctx, "The Sagemaker HPOJob Task plugin received the following inputs: \n"+
		"static hyperparameters: [%v]\n"+
		"hpo job config: [%v]\n"+
		"parameter ranges: [%v]", staticHyperparams, hpoJobConfig, hpoJobParameterRanges)

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
				TrainingJobEarlyStoppingType: getAPITrainingJobEarlyStoppingType(hpoJobConfig.TrainingJobEarlyStoppingType),
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
						ContentType: ToStringPtr("text/csv"), // TODO: can this be derived from the BlobMetadata
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
						ContentType: ToStringPtr("text/csv"), // TODO: can this be derived from the BlobMetadata
						InputMode:   "File",
					},
				},
				OutputDataConfig: &commonv1.OutputDataConfig{
					S3OutputPath: ToStringPtr(outputPath),
				},
				ResourceConfig: &commonv1.ResourceConfig{
					InstanceType:   sagemakerHPOJob.GetTrainingJob().GetTrainingJobResourceConfig().GetInstanceType(),
					InstanceCount:  ToInt64Ptr(sagemakerHPOJob.GetTrainingJob().GetTrainingJobResourceConfig().GetInstanceCount()),
					VolumeSizeInGB: ToInt64Ptr(sagemakerHPOJob.GetTrainingJob().GetTrainingJobResourceConfig().GetVolumeSizeInGb()),
					VolumeKmsKeyId: ToStringPtr(""), // TODO: Not yet supported. Need to add to proto and flytekit in the future
				},
				RoleArn: ToStringPtr(cfg.RoleArn),
				StoppingCondition: &commonv1.StoppingCondition{
					MaxRuntimeInSeconds:  nil,
					MaxWaitTimeInSeconds: nil,
				},
			},
			Region: ToStringPtr(cfg.Region),
		},
	}

	logger.Infof(ctx, "Successfully built a hpo job resource for task [%v]", taskCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName())
	return hpoJob, nil
}

func getTaskTemplate(ctx context.Context, taskCtx pluginsCore.TaskExecutionContext) (*core.TaskTemplate, error) {
	taskTemplate, err := taskCtx.TaskReader().Read(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to fetch task specification")
	} else if taskTemplate == nil {
		return nil, errors.Errorf("nil task specification")
	}
	return taskTemplate, nil
}

func (m awsSagemakerPlugin) BuildResource(ctx context.Context, taskCtx pluginsCore.TaskExecutionContext) (k8s.Resource, error) {

	// Unmarshal the custom field of the task template back into the HPOJob struct generated in flyteidl
	if m.TaskType == trainingJobTaskType {
		return m.BuildResourceForTrainingJob(ctx, taskCtx)
	}
	if m.TaskType == hpoJobTaskType {
		return m.BuildResourceForHPOJob(ctx, taskCtx)
	}
	return nil, errors.Errorf("The SageMaker plugin is unable to build resource for unknown task type [%s]", m.TaskType)
}

func (m awsSagemakerPlugin) getEventInfoForJob(ctx context.Context, job k8s.Resource) (*pluginsCore.TaskInfo, error) {

	var jobRegion, jobName, jobTypeInURL, sagemakerLinkName string
	if m.TaskType == trainingJobTaskType {
		trainingJob := job.(*trainingjobv1.TrainingJob)
		jobRegion = *trainingJob.Spec.Region
		jobName = *trainingJob.Spec.TrainingJobName
		jobTypeInURL = "jobs"
		sagemakerLinkName = "SageMaker Training Job"
	} else if m.TaskType == hpoJobTaskType {
		trainingJob := job.(*hpojobv1.HyperparameterTuningJob)
		jobRegion = *trainingJob.Spec.Region
		jobName = *trainingJob.Spec.HyperParameterTuningJobName
		jobTypeInURL = "hyper-tuning-jobs"
		sagemakerLinkName = "SageMaker Hyperparameter Tuning Job"
	} else {
		return nil, errors.Errorf("The plugin is unable to get event info for unknown task type {%v}", m.TaskType)
	}

	logger.Infof(ctx, "Getting event information for task type: [%v], job region: [%v], job name: [%v], "+
		"job type in url: [%v], sagemaker link name: [%v]", m.TaskType, jobRegion, jobName, jobTypeInURL, sagemakerLinkName)

	cwLogURL := fmt.Sprintf("https://%s.console.aws.amazon.com/cloudwatch/home?region=%s#logStream:group=/aws/sagemaker/TrainingJobs;prefix=%s;streamFilter=typeLogStreamPrefix",
		jobRegion, jobRegion, jobName)
	smLogURL := fmt.Sprintf("https://%s.console.aws.amazon.com/sagemaker/home?region=%s#/%s/%s",
		jobRegion, jobRegion, jobTypeInURL, jobName)

	taskLogs := []*core.TaskLog{
		{
			Uri:           cwLogURL,
			Name:          "CloudWatch Logs",
			MessageFormat: core.TaskLog_JSON,
		},
		{
			Uri:           smLogURL,
			Name:          sagemakerLinkName,
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
	op := createOutputLiteralMap(tk, outputPath)
	return op, nil
}

func createOutputPath(prefix string) string {
	return fmt.Sprintf("%s/hpo_outputs", prefix)
}

func createModelOutputPath(prefix, bestExperiment string) string {
	return fmt.Sprintf("%s/%s/output/model.tar.gz", createOutputPath(prefix), bestExperiment)
}

func (m awsSagemakerPlugin) GetTaskPhaseForTrainingJob(
	ctx context.Context, pluginContext k8s.PluginContext, trainingJob *trainingjobv1.TrainingJob) (pluginsCore.PhaseInfo, error) {

	logger.Infof(ctx, "Getting task phase for sagemaker training job [%v]", trainingJob.Status.SageMakerTrainingJobName)
	info, err := m.getEventInfoForJob(ctx, trainingJob)
	if err != nil {
		return pluginsCore.PhaseInfoUndefined, err
	}

	occurredAt := time.Now()

	switch trainingJob.Status.TrainingJobStatus {
	case trainingjobController.ReconcilingTrainingJobStatus:
		logger.Errorf(ctx, "Job stuck in reconciling status, assuming retryable failure [%s]", trainingJob.Status.Additional)
		// TODO talk to AWS about why there cannot be an explicit condition that signals AWS API call errors
		execError := &core.ExecutionError{
			Message: trainingJob.Status.Additional,
			Kind:    core.ExecutionError_USER,
			Code:    trainingjobController.ReconcilingTrainingJobStatus,
		}
		return pluginsCore.PhaseInfoFailed(pluginsCore.PhaseRetryableFailure, execError, info), nil
	case sagemaker.TrainingJobStatusFailed:
		execError := &core.ExecutionError{
			Message: trainingJob.Status.Additional,
			Kind:    core.ExecutionError_USER,
			Code:    sagemaker.TrainingJobStatusFailed,
		}
		return pluginsCore.PhaseInfoFailed(pluginsCore.PhasePermanentFailure, execError, info), nil
	case sagemaker.TrainingJobStatusStopped:
		reason := fmt.Sprintf("Training Job Stopped")
		return pluginsCore.PhaseInfoRetryableFailure(taskError.DownstreamSystemError, reason, info), nil
	case sagemaker.TrainingJobStatusCompleted:
		// Now that it is success we will set the outputs as expected by the task
		out, err := getOutputs(ctx, pluginContext.TaskReader(), createModelOutputPath(pluginContext.OutputWriter().GetOutputPrefixPath().String(), trainingJob.Status.SageMakerTrainingJobName))
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

func (m awsSagemakerPlugin) GetTaskPhaseForHPOJob(
	ctx context.Context, pluginContext k8s.PluginContext, hpoJob *hpojobv1.HyperparameterTuningJob) (pluginsCore.PhaseInfo, error) {

	logger.Infof(ctx, "Getting task phase for hpo job [%v]", hpoJob.Status.SageMakerHyperParameterTuningJobName)
	info, err := m.getEventInfoForJob(ctx, hpoJob)
	if err != nil {
		return pluginsCore.PhaseInfoUndefined, err
	}

	occurredAt := time.Now()

	switch hpoJob.Status.HyperParameterTuningJobStatus {
	case hpojobController.ReconcilingTuningJobStatus:
		logger.Errorf(ctx, "Job stuck in reconciling status, assuming retryable failure [%s]", hpoJob.Status.Additional)
		// TODO talk to AWS about why there cannot be an explicit condition that signals AWS API call errors
		execError := &core.ExecutionError{
			Message: hpoJob.Status.Additional,
			Kind:    core.ExecutionError_USER,
			Code:    hpojobController.ReconcilingTuningJobStatus,
		}
		return pluginsCore.PhaseInfoFailed(pluginsCore.PhaseRetryableFailure, execError, info), nil
	case sagemaker.HyperParameterTuningJobStatusFailed:
		execError := &core.ExecutionError{
			Message: hpoJob.Status.Additional,
			Kind:    core.ExecutionError_USER,
			Code:    sagemaker.HyperParameterTuningJobStatusFailed,
		}
		return pluginsCore.PhaseInfoFailed(pluginsCore.PhasePermanentFailure, execError, info), nil
	case sagemaker.HyperParameterTuningJobStatusStopped:
		reason := fmt.Sprintf("HPO Job Stopped")
		return pluginsCore.PhaseInfoRetryableFailure(taskError.DownstreamSystemError, reason, info), nil
	case sagemaker.HyperParameterTuningJobStatusCompleted:
		// Now that it is success we will set the outputs as expected by the task
		out, err := getOutputs(ctx, pluginContext.TaskReader(), createModelOutputPath(pluginContext.OutputWriter().GetOutputPrefixPath().String(), *hpoJob.Status.BestTrainingJob.TrainingJobName))
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

func (m awsSagemakerPlugin) GetTaskPhase(ctx context.Context, pluginContext k8s.PluginContext, resource k8s.Resource) (pluginsCore.PhaseInfo, error) {
	if m.TaskType == trainingJobTaskType {
		job := resource.(*trainingjobv1.TrainingJob)
		return m.GetTaskPhaseForTrainingJob(ctx, pluginContext, job)
	} else if m.TaskType == hpoJobTaskType {
		job := resource.(*hpojobv1.HyperparameterTuningJob)
		return m.GetTaskPhaseForHPOJob(ctx, pluginContext, job)
	}
	return pluginsCore.PhaseInfoUndefined, errors.Errorf("cannot get task phase for unknown task type [%s]", m.TaskType)
}

func init() {
	if err := commonv1.AddToScheme(scheme.Scheme); err != nil {
		panic(err)
	}

	// Registering the plugin for HPOJob
	pluginmachinery.PluginRegistry().RegisterK8sPlugin(
		k8s.PluginEntry{
			ID:                  hpoJobTaskPluginID,
			RegisteredTaskTypes: []pluginsCore.TaskType{hpoJobTaskType},
			ResourceToWatch:     &hpojobv1.HyperparameterTuningJob{},
			Plugin:              awsSagemakerPlugin{TaskType: hpoJobTaskType},
			IsDefault:           false,
		})

	// Registering the plugin for standalone TrainingJob
	pluginmachinery.PluginRegistry().RegisterK8sPlugin(
		k8s.PluginEntry{
			ID:                  trainingJobTaskPluginID,
			RegisteredTaskTypes: []pluginsCore.TaskType{trainingJobTaskType},
			ResourceToWatch:     &trainingjobv1.TrainingJob{},
			Plugin:              awsSagemakerPlugin{TaskType: trainingJobTaskType},
			IsDefault:           false,
		})
}
