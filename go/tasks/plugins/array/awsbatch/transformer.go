package awsbatch

import (
	"context"
	"sort"

	config2 "github.com/lyft/flyteplugins/go/tasks/plugins/array/awsbatch/config"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/flytek8s"

	"github.com/aws/aws-sdk-go/service/batch"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/utils"

	"github.com/lyft/flyteplugins/go/tasks/errors"
	pluginCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	v1 "k8s.io/api/core/v1"
)

const (
	ArrayJobIndex       = "BATCH_JOB_ARRAY_INDEX_VAR_NAME"
	LogStreamFormatter  = "https://console.aws.amazon.com/cloudwatch/home?region=%v#logEventViewer:group=/aws/batch/job;stream=%v"
	JobFormatter        = "https://console.aws.amazon.com/batch/home?region=%v#/jobs/%v/child/%v:%v"
	arrayJobIDFormatter = "%v:%v"
)

// Note that Name is not set on the result object.
// It's up to the caller to set the Name before creating the object in K8s.
func FlyteTaskToBatchInput(ctx context.Context, tCtx pluginCore.TaskExecutionContext, jobDefinition string, cfg *config2.Config) (
	batchInput *batch.SubmitJobInput, err error) {

	// Check that the taskTemplate is valid
	taskTemplate, err := tCtx.TaskReader().Read(ctx)
	if err != nil {
		return nil, err
	} else if taskTemplate == nil {
		return nil, errors.Errorf(errors.BadTaskSpecification, "Required value not set, taskTemplate is nil")
	}

	if taskTemplate.GetContainer() == nil {
		return nil, errors.Errorf(errors.BadTaskSpecification,
			"Required value not set, taskTemplate Container")
	}

	jobConfig := newJobConfig().MergeFromConfigMap(tCtx.TaskExecutionMetadata().GetOverrides().GetConfig())
	if len(jobConfig.DynamicTaskQueue) == 0 {
		return nil, errors.Errorf(errors.BadTaskSpecification, "config[%v] is missing", DynamicTaskQueueKey)
	}

	cmd, err := utils.ReplaceTemplateCommandArgs(ctx, taskTemplate.GetContainer().GetCommand(), tCtx.InputReader(), tCtx.OutputWriter())
	if err != nil {
		return nil, err
	}

	args, err := utils.ReplaceTemplateCommandArgs(ctx, taskTemplate.GetContainer().GetArgs(), tCtx.InputReader(), tCtx.OutputWriter())
	taskTemplate.GetContainer().GetEnv()

	envVars := getEnvVarsForTask(ctx, tCtx, taskTemplate.GetContainer().GetEnv(), cfg.DefaultEnvVars)
	resources := newContainerResourcesFromContainerTask(ctx, taskTemplate.GetContainer())
	return &batch.SubmitJobInput{
		JobName:            refStr(tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()),
		JobDefinition:      refStr(jobDefinition),
		JobQueue:           refStr(jobConfig.DynamicTaskQueue),
		RetryStrategy:      toRetryStrategy(ctx, toBackoffLimit(taskTemplate.Metadata), cfg.MinRetries, cfg.MaxRetries),
		ContainerOverrides: toContainerOverrides(ctx, append(cmd, args...), resources, envVars),
	}, nil
}

func UpdateBatchInputForArray(_ context.Context, batchInput *batch.SubmitJobInput, arraySize int64) *batch.SubmitJobInput {
	var arrayProps *batch.ArrayProperties
	envVars := batchInput.ContainerOverrides.Environment
	if arraySize > 1 {
		envVars = append(envVars, &batch.KeyValuePair{Name: refStr(ArrayJobIndex), Value: refStr("AWS_BATCH_JOB_ARRAY_INDEX")})
		arrayProps = &batch.ArrayProperties{
			Size: refInt(arraySize),
		}
	} else {
		// AWS Batch doesn't allow arrays of size 1. Since the task template defines the job as an array, we substitute
		// these env vars to make it look like one.
		envVars = append(envVars, &batch.KeyValuePair{Name: refStr(ArrayJobIndex), Value: refStr("FAKE_JOB_ARRAY_INDEX")},
			&batch.KeyValuePair{Name: refStr("FAKE_JOB_ARRAY_INDEX"), Value: refStr("0")})
	}

	batchInput.ArrayProperties = arrayProps
	batchInput.ContainerOverrides.Environment = envVars

	return batchInput
}

func getEnvVarsForTask(ctx context.Context, tCtx pluginCore.TaskExecutionContext, containerEnvVars []*core.KeyValuePair,
	defaultEnvVars map[string]string) []v1.EnvVar {
	envVars := flytek8s.DecorateEnvVars(ctx, flytek8s.ToK8sEnvVar(containerEnvVars), tCtx.TaskExecutionMetadata().GetTaskExecutionID())

	for key, value := range defaultEnvVars {
		envVars = append(envVars, v1.EnvVar{
			Name:  key,
			Value: value,
		})
	}

	return envVars
}

func toEnvironmentVariables(_ context.Context, envVars []v1.EnvVar) []*batch.KeyValuePair {
	res := make([]*batch.KeyValuePair, 0, len(envVars))
	for _, pair := range envVars {
		res = append(res, &batch.KeyValuePair{
			Name:  refStr(pair.Name),
			Value: refStr(pair.Value),
		})
	}

	sort.Slice(res, func(i, j int) bool {
		return *res[i].Name < *res[j].Name
	})

	return res
}

func toContainerOverrides(ctx context.Context, command []string, overrides resourceOverrides,
	envVars []v1.EnvVar) *batch.ContainerOverrides {

	return &batch.ContainerOverrides{
		Memory:      refInt(overrides.MemoryMB),
		Vcpus:       refInt(overrides.Cpus),
		Environment: toEnvironmentVariables(ctx, envVars),
		Command:     refStrSlice(command),
	}
}

func refStr(s string) *string {
	res := (s + " ")[:len(s)]
	return &res
}

func refStrSlice(s []string) []*string {
	res := make([]*string, 0, len(s))
	for _, str := range s {
		res = append(res, refStr(str))
	}

	return res
}

func refInt(i int64) *int64 {
	return &i
}

func toRetryStrategy(_ context.Context, backoffLimit *int32, minRetryAttempts, maxRetryAttempts int32) *batch.RetryStrategy {
	if backoffLimit == nil || *backoffLimit < 0 {
		return nil
	}

	retries := *backoffLimit
	if retries > maxRetryAttempts {
		retries = maxRetryAttempts
	}

	if retries < minRetryAttempts {
		retries = minRetryAttempts
	}

	return &batch.RetryStrategy{
		Attempts: refInt(int64(retries)),
	}
}

func toK8sEnvVars(envVars []*core.KeyValuePair) []v1.EnvVar {
	res := make([]v1.EnvVar, 0, len(envVars))
	for _, v := range envVars {
		res = append(res, v1.EnvVar{Name: v.Key, Value: v.Value})
	}

	return res
}

func toBackoffLimit(metadata *core.TaskMetadata) *int32 {
	if metadata == nil || metadata.Retries == nil {
		return nil
	}

	i := int32(metadata.Retries.Retries)
	return &i
}

func jobPhaseToPluginsPhase(jobStatus string) pluginCore.Phase {
	switch jobStatus {
	case batch.JobStatusSubmitted:
		fallthrough
	case batch.JobStatusPending:
		fallthrough
	case batch.JobStatusRunnable:
		fallthrough
	case batch.JobStatusStarting:
		return pluginCore.PhaseQueued
	case batch.JobStatusRunning:
		return pluginCore.PhaseRunning
	case batch.JobStatusSucceeded:
		return pluginCore.PhaseSuccess
	case batch.JobStatusFailed:
		// Retryable failure vs Permanent can be overriden if the task writes an errors.pb in the output prefix.
		return pluginCore.PhaseRetryableFailure
	}

	return pluginCore.PhaseUndefined
}
