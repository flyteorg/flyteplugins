/*
 * Copyright (c) 2018 Lyft. All rights reserved.
 */

package awsbatch

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/lyft/flytedynamicjoboperator/pkg/aws"

	"github.com/lyft/flyteplugins/go/tasks/v1/utils"

	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"
	"github.com/lyft/flyteplugins/go/tasks/v1/types"
	"github.com/lyft/flytestdlib/logger"

	"github.com/aws/aws-sdk-go/service/batch"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	v1 "k8s.io/api/core/v1"

	"github.com/lyft/flytedynamicjoboperator/errors"
	ctrlConfig "github.com/lyft/flytedynamicjoboperator/pkg/controller/config"
	"github.com/lyft/flytedynamicjoboperator/pkg/internal"
	"github.com/lyft/flytedynamicjoboperator/pkg/resource/env"
)

const (
	MinRetryAttempts   = 1
	MaxRetryAttempts   = 10
	LogStreamFormatter = "https://console.aws.amazon.com/cloudwatch/home?region=%v#logEventViewer:group=/aws/batch/job;stream=%v"
)

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
		Memory:      refInt(MemoryMB),
		Vcpus:       refInt(Cpus),
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

func toRetryStrategy(_ context.Context, backoffLimit *int32) *batch.RetryStrategy {
	if backoffLimit == nil || *backoffLimit < 0 {
		return nil
	}

	retries := internal.JailValueUint32(uint32(*backoffLimit), MinRetryAttempts, MaxRetryAttempts)
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

func toArrayJob(structObj *structpb.Struct) (*plugins.ArrayJob, error) {
	arrayJob := &plugins.ArrayJob{}
	err := internal.UnmarshalStruct(structObj, arrayJob)
	return arrayJob, err
}

// Builds batch.SubmitJobInput from an ArrayJob
func ArrayJobToBatchInput(ctx context.Context, taskCtx types.TaskContext, jobDefinition string,
	task *core.TaskTemplate, numCachedJobs int64) (*plugins.ArrayJob, *batch.SubmitJobInput, error) {

	if task.GetContainer() == nil {
		return nil, nil, errors.NewRequiredValueNotSet("container")
	}

	var arrayJob *plugins.ArrayJob
	var err error

	if task.GetCustom() != nil {
		arrayJob, err = toArrayJob(task.GetCustom())
		if err != nil {
			return nil, nil, errors.NewInvalidFormat("custom")
		}
	}

	cfg := MergeFromConfigMap(taskCtx.GetOverrides().GetConfig())
	resources := newContainerResourcesFromContainerTask(ctx, task.GetContainer())

	if len(DynamicTaskQueue) == 0 {
		return nil, nil, errors.NewRequiredValueNotSet(fmt.Sprintf("configs.config[%v]", ChildTaskQueueKey))
	}

	var envVars []v1.EnvVar
	if task.GetContainer().GetEnv() != nil {
		envVars = toK8sEnvVars(task.GetContainer().GetEnv())
	} else {
		envVars = make([]v1.EnvVar, 0)
	}

	envVars = env.FinalizeEnvironmentVariables(ctx, taskCtx, envVars)

	// There is no statsagent configured as a service in AWS Batch. override this env var so that the SDK (and other SDK)
	// can know where the statsd will be:
	envVars = append(envVars, v1.EnvVar{Name: env.StatsdHost, Value: "localhost"})

	// TODO: divide completions by slots...
	size := int64(1)
	if arrayJob != nil && arrayJob.Size > 0 {
		arrayJob.Size -= numCachedJobs
		if arrayJob.MinSuccesses <= numCachedJobs {
			arrayJob.MinSuccesses = 0
		} else {
			arrayJob.MinSuccesses -= numCachedJobs
		}
		size = arrayJob.Size
	}

	var arrayProps *batch.ArrayProperties
	if size > 1 {
		envVars = append(envVars, v1.EnvVar{Name: env.ArrayJobIndex, Value: "AWS_BATCH_JOB_ARRAY_INDEX"})
		arrayProps = &batch.ArrayProperties{
			Size: refInt(size),
		}
	} else {
		// This is done so that the SDK shouldn't special-handle whether it's running as an array job or not.
		envVars = append(envVars, v1.EnvVar{Name: env.ArrayJobIndex, Value: "FAKE_JOB_ARRAY_INDEX"},
			v1.EnvVar{Name: "FAKE_JOB_ARRAY_INDEX", Value: "0"})
	}

	command := task.GetContainer().Command
	if len(task.GetContainer().Args) > 0 {
		command = append(command, task.GetContainer().Args...)
	}

	args := utils.CommandLineTemplateArgs{
		Input:        taskCtx.GetDataDir().String(),
		OutputPrefix: taskCtx.GetDataDir().String(),
	}

	command, err = utils.ReplaceTemplateCommandArgs(ctx, command, args)
	if err != nil {
		return nil, nil, errors.WrapError(err, errors.NewInvalidFormat("Command"))
	}

	return arrayJob, &batch.SubmitJobInput{
		JobName:            refStr(taskCtx.GetTaskExecutionID().GetGeneratedName()),
		JobDefinition:      refStr(jobDefinition),
		JobQueue:           refStr(DynamicTaskQueue),
		RetryStrategy:      toRetryStrategy(ctx, toBackoffLimit(task.Metadata)),
		ContainerOverrides: toContainerOverrides(ctx, command, resources, envVars),
		ArrayProperties:    arrayProps,
	}, nil
}

func int64OrDefault(i *int64) int64 {
	if i == nil {
		return 0
	}

	return *i
}

func jobDetailToJob(ctx context.Context, j *batch.JobDetail) *Job {
	res := &Job{}
	//if j != nil {
	//	// If a job isn't returned from the Batch GetJobDetailsBatch call, then we skip updating it, but only if
	//	// it's been more than day. This logic is here to keep track of jobs that get reaped by Batch, but we
	//	// don't want to trigger it if the job was recently submitted, in case of consistency issues we think we've
	//	// run into. Sometimes, when we submit a job, and check right after, the job we just submitted doesn't come back
	//	// TODO: Implement this logic as a retry counter instead eventually. Relying on wall clock time is a hack.
	//	if j.StartedAt != nil && *j.StartedAt > (time.Duration(24)*time.Hour).Nanoseconds() {
	//		logger.Errorf(ctx, "Empty Batch JobDetail received, failing job %s", ID)
	//		Phase = batch.JobStatusFailed
	//		Message = "JobID missing in AWS BATCH"
	//	} else {
	//		logger.Infof(ctx, "Job details not found for JobID within a day [%s]. Skipping update...", j)
	//	}
	//	return crdJob
	//}

	if j == nil {
		return nil
	}

	if j.Status == nil {
		logger.Warnf(ctx, "No status received for job [%v]", *j.JobId)
		res.Status.Message = "JobID in AWS BATCH has no Status"
		return res
	}

	res.Status.Phase = *j.Status
	if j.StatusReason != nil {
		res.Status.Message = *j.StatusReason
	}

	if j.ArrayProperties != nil {
		arrayJob := ArrayJobSummary{}
		for jobStatus, count := range j.ArrayProperties.StatusSummary {
			arrayJob[jobStatus] = int64OrDefault(count)
		}

		res.
	}

	if len(j.Attempts) > 0 {
		lastAttempt := j.Attempts[len(j.Attempts)-1]
		if lastAttempt.StartedAt != nil {
			StartedAt = time.Unix(*lastAttempt.StartedAt, 0)
		}

		if lastAttempt.StoppedAt != nil {
			StoppedAt = time.Unix(*lastAttempt.StoppedAt, 0)
		}

		if lastAttempt.Container != nil {
			if lastAttempt.Container.LogStreamName != nil {
				LogStream = formatLogStream(*lastAttempt.Container.LogStreamName)
			}
		}
	}
	return crdJob

}

func formatLogStream(logStreamName string) string {
	return fmt.Sprintf(LogStreamFormatter, aws.GetConfig().Region, logStreamName)
}

func getRole(_ context.Context, annotations map[string]string) string {
	if key := ctrlConfig.GetConfig().RoleAnnotationKey; len(key) > 0 {
		return annotations[key]
	}

	return ""
}

func getContainerImage(_ context.Context, task *core.TaskTemplate) string {
	if task.GetContainer() != nil && len(task.GetContainer().Image) > 0 {
		return task.GetContainer().Image
	}

	return ""
}
