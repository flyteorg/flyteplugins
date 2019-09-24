/*
 * Copyright (c) 2018 Lyft. All rights reserved.
 */

package awsbatch

import (
	"context"

	"github.com/lyft/flytestdlib/promutils"

	"testing"

	"github.com/aws/aws-sdk-go/service/batch"
	"github.com/lyft/flytedynamicjoboperator/internal/mocks"
	"github.com/lyft/flytedynamicjoboperator/pkg/resource/flowcontrol"
	"github.com/stretchr/testify/assert"
)

func newClientWithMockBatch() *client {
	rateLimiter := flowcontrol.NewRateLimiter("Get", 1000, 1000)
	return NewCustomBatchClient(mocks.NewMockAwsBatchClient(), NewInMemoryStore(1000, promutils.NewTestScope()), rateLimiter, rateLimiter).(*client)
}

func TestClient_SubmitJob(t *testing.T) {
	rateLimiter := flowcontrol.NewRateLimiter("Get", 1000, 1000)
	store := NewInMemoryStore(1000, promutils.NewTestScope())
	c := NewCustomBatchClient(mocks.NewMockAwsBatchClient(), store, rateLimiter, rateLimiter).(*client)
	o, err := SubmitJob(context.TODO(), &batch.SubmitJobInput{
		JobName: refStr("test-job"),
	})

	assert.NoError(t, err)
	assert.NotNil(t, o)
	assert.NotEmpty(t, String())

	added, err := store.Add(context.TODO(), o)
	assert.NoError(t, err)
	assert.True(t, added)

	// Resubmit the same job
	o, err = SubmitJob(context.TODO(), &batch.SubmitJobInput{
		JobName: refStr("test-job"),
	})

	assert.NoError(t, err)
	assert.NotNil(t, o)
	assert.NotEmpty(t, String())
}

func TestClient_TerminateJob(t *testing.T) {
	c := newClientWithMockBatch()
	err := TerminateJob(context.TODO(), "1", "")
	assert.NoError(t, err)
}

func TestClient_GetJobDetailsBatch(t *testing.T) {
	c := newClientWithMockBatch()
	o, err := GetJobDetailsBatch(context.TODO(), []string{})
	assert.NoError(t, err)
	assert.NotNil(t, o)

	o, err = GetJobDetailsBatch(context.TODO(), []string{"fake_job_id"})
	assert.NoError(t, err)
	assert.NotNil(t, o)
}

func TestClient_RegisterJobDefinition(t *testing.T) {
	c := newClientWithMockBatch()
	j, err := RegisterJobDefinition(context.TODO(), "name-abc", "img", "admin-role")
	assert.NoError(t, err)
	assert.NotNil(t, j)
}
