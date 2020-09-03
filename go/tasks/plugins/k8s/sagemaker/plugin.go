package sagemaker

import (
	"context"
	"fmt"

	pluginErrors "github.com/lyft/flyteplugins/go/tasks/errors"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/utils"

	flyteIdlCore "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/logger"

	"k8s.io/client-go/kubernetes/scheme"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery"

	commonv1 "github.com/aws/amazon-sagemaker-operator-for-k8s/api/v1/common"
	hpojobv1 "github.com/aws/amazon-sagemaker-operator-for-k8s/api/v1/hyperparametertuningjob"
	trainingjobv1 "github.com/aws/amazon-sagemaker-operator-for-k8s/api/v1/trainingjob"
	pluginsCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/k8s"
)

// Sanity test that the plugin implements method of k8s.Plugin
var _ k8s.Plugin = awsSagemakerPlugin{}

type awsSagemakerPlugin struct {
	TaskType pluginsCore.TaskType
}

func (m awsSagemakerPlugin) BuildIdentityResource(_ context.Context, _ pluginsCore.TaskExecutionMetadata) (k8s.Resource, error) {
	if m.TaskType == trainingJobTaskType || m.TaskType == customTrainingJobTaskType {
		return &trainingjobv1.TrainingJob{}, nil
	}
	if m.TaskType == hyperparameterTuningJobTaskType {
		return &hpojobv1.HyperparameterTuningJob{}, nil
	}
	return nil, pluginErrors.Errorf(pluginErrors.BadTaskSpecification, "The sagemaker plugin is unable to build identity resource for an unknown task type [%v]", m.TaskType)
}

func (m awsSagemakerPlugin) BuildResource(ctx context.Context, taskCtx pluginsCore.TaskExecutionContext) (k8s.Resource, error) {

	// Unmarshal the custom field of the task template back into the HyperparameterTuningJob struct generated in flyteidl
	if m.TaskType == trainingJobTaskType {
		return m.buildResourceForTrainingJob(ctx, taskCtx)
	}
	if m.TaskType == customTrainingJobTaskType {
		return m.buildResourceForCustomTrainingJob(ctx, taskCtx)
	}
	if m.TaskType == hyperparameterTuningJobTaskType {
		return m.buildResourceForHyperparameterTuningJob(ctx, taskCtx)
	}
	return nil, pluginErrors.Errorf(pluginErrors.BadTaskSpecification, "The SageMaker plugin is unable to build resource for unknown task type [%s]", m.TaskType)
}

func (m awsSagemakerPlugin) getEventInfoForJob(ctx context.Context, job k8s.Resource) (*pluginsCore.TaskInfo, error) {

	var jobRegion, jobName, jobTypeInURL, sagemakerLinkName string
	if m.TaskType == trainingJobTaskType {
		trainingJob := job.(*trainingjobv1.TrainingJob)
		jobRegion = *trainingJob.Spec.Region
		jobName = *trainingJob.Spec.TrainingJobName
		jobTypeInURL = "jobs"
		sagemakerLinkName = "SageMaker Training Job"
	} else if m.TaskType == customTrainingJobTaskType {
		trainingJob := job.(*trainingjobv1.TrainingJob)
		jobRegion = *trainingJob.Spec.Region
		jobName = *trainingJob.Spec.TrainingJobName
		jobTypeInURL = "jobs"
		sagemakerLinkName = "SageMaker Custom Training Job"
	} else if m.TaskType == hyperparameterTuningJobTaskType {
		trainingJob := job.(*hpojobv1.HyperparameterTuningJob)
		jobRegion = *trainingJob.Spec.Region
		jobName = *trainingJob.Spec.HyperParameterTuningJobName
		jobTypeInURL = "hyper-tuning-jobs"
		sagemakerLinkName = "SageMaker Hyperparameter Tuning Job"
	} else {
		return nil, pluginErrors.Errorf(pluginErrors.BadTaskSpecification, "The plugin is unable to get event info for unknown task type {%v}", m.TaskType)
	}

	logger.Infof(ctx, "Getting event information for task type: [%v], job region: [%v], job name: [%v], "+
		"job type in url: [%v], sagemaker link name: [%v]", m.TaskType, jobRegion, jobName, jobTypeInURL, sagemakerLinkName)

	cwLogURL := fmt.Sprintf("https://%s.console.aws.amazon.com/cloudwatch/home?region=%s#logStream:group=/aws/sagemaker/TrainingJobs;prefix=%s;streamFilter=typeLogStreamPrefix",
		jobRegion, jobRegion, jobName)
	smLogURL := fmt.Sprintf("https://%s.console.aws.amazon.com/sagemaker/home?region=%s#/%s/%s",
		jobRegion, jobRegion, jobTypeInURL, jobName)

	taskLogs := []*flyteIdlCore.TaskLog{
		{
			Uri:           cwLogURL,
			Name:          "CloudWatch Logs",
			MessageFormat: flyteIdlCore.TaskLog_JSON,
		},
		{
			Uri:           smLogURL,
			Name:          sagemakerLinkName,
			MessageFormat: flyteIdlCore.TaskLog_UNKNOWN,
		},
	}

	customInfoMap := make(map[string]string)

	customInfo, err := utils.MarshalObjToStruct(customInfoMap)
	if err != nil {
		return nil, pluginErrors.Wrapf(pluginErrors.RuntimeFailure, err, "Unable to create a custom info object")
	}

	return &pluginsCore.TaskInfo{
		Logs:       taskLogs,
		CustomInfo: customInfo,
	}, nil
}

func (m awsSagemakerPlugin) GetTaskPhase(ctx context.Context, pluginContext k8s.PluginContext, resource k8s.Resource) (pluginsCore.PhaseInfo, error) {
	if m.TaskType == trainingJobTaskType {
		job := resource.(*trainingjobv1.TrainingJob)
		return m.getTaskPhaseForTrainingJob(ctx, pluginContext, job)
	} else if m.TaskType == customTrainingJobTaskType {
		job := resource.(*trainingjobv1.TrainingJob)
		return m.getTaskPhaseForCustomTrainingJob(ctx, pluginContext, job)
	} else if m.TaskType == hyperparameterTuningJobTaskType {
		job := resource.(*hpojobv1.HyperparameterTuningJob)
		return m.getTaskPhaseForHyperparameterTuningJob(ctx, pluginContext, job)
	}
	return pluginsCore.PhaseInfoUndefined, pluginErrors.Errorf(pluginErrors.BadTaskSpecification, "cannot get task phase for unknown task type [%s]", m.TaskType)
}

func init() {
	if err := commonv1.AddToScheme(scheme.Scheme); err != nil {
		panic(err)
	}

	// Registering the plugin for HyperparameterTuningJob
	pluginmachinery.PluginRegistry().RegisterK8sPlugin(
		k8s.PluginEntry{
			ID:                  hyperparameterTuningJobTaskPluginID,
			RegisteredTaskTypes: []pluginsCore.TaskType{hyperparameterTuningJobTaskType},
			ResourceToWatch:     &hpojobv1.HyperparameterTuningJob{},
			Plugin:              awsSagemakerPlugin{TaskType: hyperparameterTuningJobTaskType},
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

	// Registering the plugin for custom TrainingJob
	pluginmachinery.PluginRegistry().RegisterK8sPlugin(
		k8s.PluginEntry{
			ID:                  customTrainingJobTaskPluginID,
			RegisteredTaskTypes: []pluginsCore.TaskType{customTrainingJobTaskType},
			ResourceToWatch:     &trainingjobv1.TrainingJob{},
			Plugin:              awsSagemakerPlugin{TaskType: customTrainingJobTaskType},
			IsDefault:           false,
		})
}
