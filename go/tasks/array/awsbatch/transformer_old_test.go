/*
 * Copyright (c) 2018 Lyft. All rights reserved.
 */

package awsbatch

import (
	"context"
	"testing"

	"github.com/lyft/flytedynamicjoboperator/pkg/resource/env"

	mocks2 "github.com/lyft/flyteplugins/go/tasks/v1/types/mocks"
	v12 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/lyft/flytestdlib/storage"

	"github.com/aws/aws-sdk-go/service/batch"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/lyft/flytedynamicjoboperator/pkg/internal"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"
	"github.com/stretchr/testify/assert"
)

func createSampleContainerTask() *core.Container {
	return &core.Container{
		Command: []string{"cmd"},
		Args:    []string{"{{$input}}"},
		Image:   "img1",
		Config: []*core.KeyValuePair{
			{
				Key:   ChildTaskQueueKey,
				Value: "child_queue",
			},
		},
	}
}

func TestArrayJobToBatchInput(t *testing.T) {
	expectedBatchInput := &batch.SubmitJobInput{
		ArrayProperties: &batch.ArrayProperties{
			Size: refInt(10),
		},
		JobDefinition: refStr(""),
		JobName:       refStr("Job_Name"),
		JobQueue:      refStr("child_queue"),
		ContainerOverrides: &batch.ContainerOverrides{
			Command: []*string{internal.RefString("cmd"), internal.RefString("/path/output/")},
			Environment: []*batch.KeyValuePair{
				{Name: refStr("BATCH_JOB_ARRAY_INDEX_VAR_NAME"), Value: refStr("AWS_BATCH_JOB_ARRAY_INDEX")},
				{Name: refStr(env.StatsdHost), Value: refStr("localhost")},
			},
			Memory: refInt(700),
			Vcpus:  refInt(2),
		},
	}

	input := &plugins.ArrayJob{
		Size:        10,
		Parallelism: 5,
	}

	taskCtx := &mocks2.TaskContext{}
	taskCtx.On("GetNamespace").Return("ns")
	taskCtx.On("GetAnnotations").Return(map[string]string{"aKey": "aVal"})
	taskCtx.On("GetLabels").Return(map[string]string{"lKey": "lVal"})
	taskCtx.On("GetOwnerReference").Return(v1.OwnerReference{Name: "x"})
	taskCtx.On("GetOutputsFile").Return(storage.DataReference("outputs"))
	taskCtx.On("GetInputsFile").Return(storage.DataReference("inputs"))
	taskCtx.On("GetErrorFile").Return(storage.DataReference("error"))
	taskCtx.On("GetDataDir").Return(storage.DataReference("/path/output/"))
	taskCtx.On("GetOwnerID").Return("owner_id")

	to := &mocks2.TaskOverrides{}
	to.On("GetConfig").Return(&v12.ConfigMap{})
	taskCtx.On("GetOverrides").Return(to)

	id := &mocks2.TaskExecutionID{}
	id.On("GetGeneratedName").Return("Job_Name")
	id.On("GetID").Return(core.TaskExecutionIdentifier{})
	taskCtx.On("GetTaskExecutionID").Return(id)

	st := &structpb.Struct{}
	err := internal.MarshalToStruct(input, st)
	assert.NoError(t, err)

	taskTemplate := &core.TaskTemplate{
		Id:     &core.Identifier{Name: "Job_Name"},
		Custom: st,
		Target: &core.TaskTemplate_Container{
			Container: createSampleContainerTask(),
		},
	}

	_, batchInput, err := ArrayJobToBatchInput(context.Background(), taskCtx, "", taskTemplate, 0)
	assert.NoError(t, err)
	assert.NotNil(t, batchInput)
	assert.Equal(t, *expectedBatchInput, *batchInput)
}
