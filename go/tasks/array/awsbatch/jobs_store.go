/*
 * Copyright (c) 2018 Lyft. All rights reserved.
 */

package awsbatch

import (
	"context"
	"fmt"
	"time"

	"github.com/lyft/flytestdlib/cache"
	"github.com/lyft/flytestdlib/utils"

	"github.com/aws/aws-sdk-go/service/batch"

	"github.com/lyft/flytestdlib/promutils"
	"k8s.io/apimachinery/pkg/types"
)

type JobName = string
type JobID = string
type JobPhaseType = string
type ArrayJobSummary map[JobPhaseType]int64

type Job struct {
	Name      JobName              `json:"name,omitempty"`
	Id        JobID                `json:"id,omitempty"`
	Owner     types.NamespacedName `json:"owner,omitempty"`
	LogStream string               `json:"logStream,omitempty"`
	StartedAt time.Time            `json:"startedAt,omitempty"`
	StoppedAt time.Time            `json:"stoppedAt,omitempty"`

	Status  JobStatus `json:"status,omitempty"`
	SubJobs []*Job     `json:"array,omitempty"`
}

type JobStatus struct {
	Phase   JobPhaseType `json:"phase,omitempty"`
	Message string       `json:"msg,omitempty"`
}

func (j JobStatus) isTerminal() bool {
	phaseString := j.Phase
	if phaseString == batch.JobStatusFailed || phaseString == batch.JobStatusSucceeded {
		return true
	}
	return false
}

func (j *Job) ID() cache.ItemID {
	return j.Id
}

func (j Job) String() string {
	return fmt.Sprintf("(ID: %v, Name: %v)", j.Id, j.Name)
}

func GetJobID(id JobID, index int) JobID {
	return fmt.Sprintf(arrayJobIDFormatter, id, index)
}

func batchJobsForSync(_ context.Context, batchChunkSize int) cache.CreateBatchesFunc {
	return func(ctx context.Context, items []cache.Item) (batches [][]cache.Item, err error) {
		batches = make([][]cache.Item, 0, 100)
		currentBatch := make([]cache.Item, 0, 10)
		currentBatchSize := 0
		for _, item := range items {
			j := item.(*Job)
			if currentBatchSize+len(j.SubJobs)+1 >= batchChunkSize {
				batches = append(batches, currentBatch)
				currentBatchSize = 0
				currentBatch = make([]cache.Item, 0, 10)
			}

			currentBatchSize += len(j.SubJobs) + 1
			currentBatch = append(currentBatch, item)
		}

		if len(currentBatch) != 0 {
			batches = append(batches, currentBatch)
		}

		return batches, nil
	}
}

func syncBatches(_ context.Context, client Client) cache.SyncFunc {
	return func(ctx context.Context, batch []cache.Item) ([]cache.ItemSyncResponse, error) {
		jobIDsMap := make(map[JobID]*Job, len(batch))
		jobIds := make([]JobID, 0, len(batch))
		for _, item := range batch {
			j := item.(*Job)
			jobIds = append(jobIds, j.Id)
			jobIDsMap[j.Id] = j

			for idx, subJob := range j.SubJobs {
				if !subJob.Status.isTerminal() {
					jobName := GetJobID(j.Id, idx)
					jobIds = append(jobIds, jobName)
					jobIDsMap[jobName] = subJob
				}
			}
		}

		response, err := client.GetJobDetailsBatch(ctx, jobIds)
		if err != nil {
			return nil, err
		}

		for _, jobDetail := range response {
			jobDetailToJob(ctx, jobDetail, *jobIDsMap[*jobDetail.JobId])
		}
	}
}

// Constructs a new in-memory store.
func NewInMemoryStore(cacheSize int, resyncPeriod time.Duration, batchChunkSize, scope promutils.Scope) (cache.AutoRefresh, error) {
	ctx := context.TODO()
	cache, err := cache.NewAutoRefreshBatchedCache(batchJobsForSync(ctx, batchChunkSize), syncBatches,
		utils.NewRateLimiter("aws_batch", 100, 100),
		resyncPeriod, cacheSize, scope)
	return cache, err
}
