/*
 * Copyright (c) 2018 Lyft. All rights reserved.
 */

// This package deals with the communication with AWS-Batch and adopting its APIs to the flyte-plugin model.
package awsbatch

import (
	"context"
	"fmt"

	"github.com/lyft/flyteplugins/go/tasks/aws"
	"github.com/lyft/flytestdlib/utils"

	"github.com/lyft/flytedynamicjoboperator/pkg/aws/batch/definition"
	"github.com/lyft/flytestdlib/logger"

	a "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/batch"
)

// AWS Batch Client interface.
type Client interface {
	// Submits a new job to AWS Batch and retrieves job info. Note that submitted jobs will not have status populated.
	SubmitJob(ctx context.Context, input *batch.SubmitJobInput) (Job, error)

	// Attempts to terminate a job. If the job hasn't started yet, it'll just get deleted.
	TerminateJob(ctx context.Context, jobID JobID, reason string) error

	// Retrieves jobs' details from AWS Batch.
	GetJobDetailsBatch(ctx context.Context, ids []JobID) ([]*batch.JobDetail, error)

	// Registers a new Job Definition with AWS Batch provided a name, image and role.
	RegisterJobDefinition(ctx context.Context, name, image, role string) (arn string, err error)
}

// BatchServiceClient is an interface on top of the native AWS Batch client to allow for mocking and alternative implementations.
type BatchServiceClient interface {
	SubmitJobWithContext(ctx a.Context, input *batch.SubmitJobInput, opts ...request.Option) (*batch.SubmitJobOutput, error)
	TerminateJobWithContext(ctx a.Context, input *batch.TerminateJobInput, opts ...request.Option) (*batch.TerminateJobOutput, error)
	DescribeJobsWithContext(ctx a.Context, input *batch.DescribeJobsInput, opts ...request.Option) (*batch.DescribeJobsOutput, error)
	RegisterJobDefinitionWithContext(ctx a.Context, input *batch.RegisterJobDefinitionInput, opts ...request.Option) (*batch.RegisterJobDefinitionOutput, error)
}

type client struct {
	Batch              BatchServiceClient
	store              Store
	getRateLimiter     utils.RateLimiter
	defaultRateLimiter utils.RateLimiter
}

// Registers a new job definition. There is no deduping on AWS side (even for the same name).
func (b *client) RegisterJobDefinition(ctx context.Context, name, image, role string) (arn definition.JobDefinitionArn, err error) {
	logger.Infof(ctx, "Registering job definition with name [%v], image [%v], role [%v]", name, image, role)

	res, err := b.Batch.RegisterJobDefinitionWithContext(ctx, &batch.RegisterJobDefinitionInput{
		Type:              refStr(batch.JobDefinitionTypeContainer),
		JobDefinitionName: refStr(name),
		ContainerProperties: &batch.ContainerProperties{
			Image:      refStr(image),
			JobRoleArn: refStr(role),

			// These will be overwritten on execution
			Vcpus:  refInt(1),
			Memory: refInt(100),
		},
	})

	if err != nil {
		return "", err
	}

	return *res.JobDefinitionArn, nil
}

// Submits a new job to a desired queue
func (b *client) SubmitJob(ctx context.Context, input *batch.SubmitJobInput) (Job, error) {
	if input == nil {
		return Job{}, nil
	}

	if existingJob, found, err := b.store.Get(ctx, *input.JobName); found && err == nil {
		logger.Debugf(ctx, "Not submitting a new job because an existing one with the same name already exists. Existing Job [%v]", existingJob)
		return existingJob, nil
	}

	if err := b.defaultRateLimiter.Wait(ctx); err != nil {
		return Job{}, err
	}

	output, err := b.Batch.SubmitJobWithContext(ctx, input)
	if err != nil {
		return Job{}, err
	}

	if output.JobId == nil {
		logger.Errorf(ctx, "Job submitted has no ID and no error is returned. This is an AWS-issue. Input [%v]", input.JobName)
		return Job{}, fmt.Errorf("job submitted has no ID and no error is returned. This is an AWS-issue. Input [%v]", input.JobName)
	}

	return Job{
		Id:     *output.JobId,
		Name:   *output.JobName,
		Status: JobStatus{},
	}, nil
}

// Terminates an in progress job
func (b *client) TerminateJob(ctx context.Context, jobID JobID, reason string) error {
	if err := b.defaultRateLimiter.Wait(ctx); err != nil {
		return err
	}

	input := batch.TerminateJobInput{
		JobId:  refStr(jobID),
		Reason: refStr(reason),
	}

	if _, err := b.Batch.TerminateJobWithContext(ctx, &input); err != nil {
		return err
	}

	return nil
}

func (b *client) GetJobDetailsBatch(ctx context.Context, jobIds []JobID) ([]*batch.JobDetail, error) {
	if err := b.getRateLimiter.Wait(ctx); err != nil {
		return nil, err
	}

	ids := make([]*string, 0, len(jobIds))
	for _, id := range jobIds {
		ids = append(ids, refStr(id))
	}

	input := batch.DescribeJobsInput{
		Jobs: ids,
	}

	output, err := b.Batch.DescribeJobsWithContext(ctx, &input)
	if err != nil {
		return nil, err
	}

	return output.Jobs, nil
}

// Initializes a new Batch Client that can be used to interact with AWS Batch.
func NewBatchClient(aws aws.Client,
	store Store,
	getRateLimiter utils.RateLimiter,
	defaultRateLimiter utils.RateLimiter) Client {

	batchClient := batch.New(aws.GetSession(), aws.GetConfig())
	return NewCustomBatchClient(batchClient, store, getRateLimiter, defaultRateLimiter)
}

func NewCustomBatchClient(batchClient BatchServiceClient, store Store,
	getRateLimiter utils.RateLimiter,
	defaultRateLimiter utils.RateLimiter) Client {
	return &client{
		store:              store,
		Batch:              batchClient,
		getRateLimiter:     getRateLimiter,
		defaultRateLimiter: defaultRateLimiter,
	}
}
