/*
 * Copyright (c) 2018 Lyft. All rights reserved.
 */

package awsbatch

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/lyft/flyteplugins/go/tasks/plugins/array/awsbatch/config"

	"k8s.io/client-go/util/workqueue"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flytestdlib/logger"

	"k8s.io/apimachinery/pkg/types"

	"github.com/lyft/flyteplugins/go/tasks/errors"

	"github.com/aws/aws-sdk-go/service/batch"
	"github.com/lyft/flytestdlib/cache"

	"github.com/lyft/flytestdlib/promutils"
)

type JobName = string
type JobID = string
type JobPhaseType = core.Phase
type ArrayJobSummary map[JobPhaseType]int64

const (
	ErrAlreadyExists errors.ErrorCode = "ALREADY_EXIST"
)

type Event struct {
	OldJob *Job
	NewJob *Job
}

type EventHandler struct {
	Updated func(ctx context.Context, event Event)
}

type Job struct {
	ID             JobID                `json:"id,omitempty"`
	OwnerReference types.NamespacedName `json:"owner.omitempty"`
	Attempts       []Attempt            `json:"attempts,omitempty"`
	Status         JobStatus            `json:"status,omitempty"`
	SubJobs        []*Job               `json:"array,omitempty"`
}

type Attempt struct {
	LogStream string    `json:"logStream,omitempty"`
	StartedAt time.Time `json:"startedAt,omitempty"`
	StoppedAt time.Time `json:"stoppedAt,omitempty"`
}

type JobStatus struct {
	Phase   JobPhaseType `json:"phase,omitempty"`
	Message string       `json:"msg,omitempty"`
}

func (j Job) String() string {
	return fmt.Sprintf("(ID: %v)", j.ID)
}

func GetJobID(id JobID, index int) JobID {
	return fmt.Sprintf(arrayJobIDFormatter, id, index)
}

func batchJobsForSync(_ context.Context, batchChunkSize int) cache.CreateBatchesFunc {
	return func(ctx context.Context, items []cache.ItemWrapper) (batches []cache.Batch, err error) {
		batches = make([]cache.Batch, 0, 100)
		currentBatch := make(cache.Batch, 0, batchChunkSize)
		currentBatchSize := 0
		for _, item := range items {
			j := item.GetItem().(*Job)
			if j.Status.Phase.IsTerminal() {
				// If the job has already been terminated, do not include it in any batch.
				continue
			}

			if currentBatchSize+len(j.SubJobs)+1 >= batchChunkSize {
				batches = append(batches, currentBatch)
				currentBatchSize = 0
				currentBatch = make(cache.Batch, 0, batchChunkSize)
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

func syncBatches(_ context.Context, client Client, handler EventHandler) cache.SyncFunc {
	return func(ctx context.Context, batch cache.Batch) ([]cache.ItemSyncResponse, error) {
		jobIDsMap := make(map[JobID]*Job, len(batch))
		jobIds := make([]JobID, 0, len(batch))
		jobNames := make(map[JobID]string, len(batch))

		// Build a flat list of JobIds to query batch for their status. Also build a reverse lookup to find these jobs
		// and update them in the cache.
		for _, item := range batch {
			j := item.GetItem().(*Job)
			if j.Status.Phase.IsTerminal() {
				continue
			}

			jobIds = append(jobIds, j.ID)
			jobIDsMap[j.ID] = j
			jobNames[j.ID] = item.GetID()

			for idx, subJob := range j.SubJobs {
				if !subJob.Status.Phase.IsTerminal() {
					fullJobID := GetJobID(j.ID, idx)
					jobIds = append(jobIds, fullJobID)
					jobIDsMap[fullJobID] = subJob
				}
			}
		}

		if len(jobIds) == 0 {
			return []cache.ItemSyncResponse{}, nil
		}

		response, err := client.GetJobDetailsBatch(ctx, jobIds)
		if err != nil {
			return nil, err
		}

		res := make([]cache.ItemSyncResponse, 0, len(response))
		for _, jobDetail := range response {
			job, found := jobIDsMap[*jobDetail.JobId]
			if !found {
				logger.Warn(ctx, "Received an update for unrequested job id [%v]", jobDetail.JobId)
				continue
			}

			changed := false
			msg := make([]string, 0, 2)
			if jobDetail.Status == nil {
				logger.Warnf(ctx, "No status received for job [%v]", *jobDetail.JobId)
				msg = append(msg, "JobID in AWS BATCH has no Status")
			} else {
				newPhase := jobPhaseToPluginsPhase(*jobDetail.Status)
				if job.Status.Phase != newPhase {
					changed = true
				}

				job.Status.Phase = newPhase
			}

			if jobDetail.StatusReason != nil {
				msg = append(msg, *jobDetail.StatusReason)
			}

			job.Attempts = make([]Attempt, 0, len(jobDetail.Attempts))
			lastStatusReason := ""
			for _, attempt := range jobDetail.Attempts {
				a := Attempt{}
				if attempt.StartedAt != nil {
					a.StartedAt = time.Unix(*attempt.StartedAt, 0)
				}

				if attempt.StoppedAt != nil {
					a.StoppedAt = time.Unix(*attempt.StoppedAt, 0)
				}

				if container := attempt.Container; container != nil {
					if container.LogStreamName != nil {
						a.LogStream = *container.LogStreamName
					}

					if container.Reason != nil {
						lastStatusReason = *container.Reason
					}

					if container.ExitCode != nil {
						lastStatusReason += fmt.Sprintf(" exit(%v)", *container.ExitCode)
					}
				}

				job.Attempts = append(job.Attempts, a)
			}

			msg = append(msg, lastStatusReason)

			job.Status.Message = strings.Join(msg, " - ")

			if changed {
				handler.Updated(ctx, Event{
					NewJob: job,
				})
			}

			action := cache.Unchanged
			if changed {
				action = cache.Update
			}

			res = append(res, cache.ItemSyncResponse{
				ID:     jobNames[job.ID],
				Item:   job,
				Action: action,
			})
		}

		return res, nil
	}
}

type JobStore struct {
	Client
	cache.AutoRefresh

	started bool
}

// Submits a new job to AWS Batch and retrieves job info. Note that submitted jobs will not have status populated.
//
func (s JobStore) SubmitJob(ctx context.Context, input *batch.SubmitJobInput) (jobID string, err error) {
	name := *input.JobName
	if item, err := s.AutoRefresh.Get(name); err == nil {
		logger.Infof(ctx, "Job already found in cache with the same name [%v]. Will not submit a new job.",
			name)
		return item.(*Job).ID, nil
	}

	return s.Client.SubmitJob(ctx, input)
}

func (s *JobStore) Start(ctx context.Context) error {
	err := s.AutoRefresh.Start(ctx)
	if err != nil {
		return err
	}

	s.started = true
	return nil
}

func (s JobStore) GetOrCreate(jobName string, job *Job) (*Job, error) {
	j, err := s.AutoRefresh.GetOrCreate(jobName, job)
	if err != nil {
		return nil, err
	}

	return j.(*Job), err
}

func (s JobStore) Get(jobName string) *Job {
	j, err := s.AutoRefresh.Get(jobName)
	if err != nil {
		return nil
	}

	return j.(*Job)
}

func (s JobStore) IsStarted() bool {
	return s.started
}

// Constructs a new in-memory store.
func NewJobStore(ctx context.Context, batchClient Client, resyncPeriod time.Duration, cfg config.JobStoreConfig,
	handler EventHandler, scope promutils.Scope) (JobStore, error) {

	store := JobStore{
		Client: batchClient,
	}

	autoCache, err := cache.NewAutoRefreshBatchedCache(batchJobsForSync(ctx, cfg.BatchChunkSize), syncBatches(ctx, store, handler),
		workqueue.DefaultControllerRateLimiter(), resyncPeriod, cfg.Parallelizm, cfg.CacheSize, scope)

	store.AutoRefresh = autoCache
	return store, err
}
