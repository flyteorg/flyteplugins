/*
 * Copyright (c) 2018 Lyft. All rights reserved.
 */

package awsbatch

import (
	"context"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"sync"
	"time"

	"github.com/lyft/flyteplugins/go/tasks/v1/types"

	"github.com/aws/aws-sdk-go/service/batch"
	"github.com/lyft/flytedynamicjoboperator/errors"
	"github.com/lyft/flytedynamicjoboperator/pkg/internal"
	"github.com/lyft/flytestdlib/logger"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	batchChunkSize = 100
)

var once sync.Once

// Run watcher to periodically poll AWS Batch for updates about the jobs in store.
func (e Executor) runWatcher(ctx context.Context, enqueueOwner core.EnqueueOwner, resyncPeriod time.Duration) {
	once.Do(func() {
		go internal.WithRecover(ctx, func() {
			err := wait.PollUntil(resyncPeriod, func() (done bool, err error) {
				res, err := e.doSync(ctx)
				if err != nil {
					logger.Warnf(ctx, "Failed to list batch jobs. Will skip this attempt. Error: %v", err)
					return false, nil
				}

				for _, j := range res {
					err = AddOrUpdate(ctx, j)
					if err != nil {
						logger.Warnf(ctx, "Failed to add/Update job [%v]. Skipping its update this time.", j)
					}

					err = enqueueOwner(Owner)
					if err != nil {
						logger.Warnf(ctx, "Failed to enqueue owner for job [%v]. Skipping its update this time.", j)
					}
				}

				return false, nil
			}, ctx.Done())

			if err != nil {
				logger.Panicf(ctx, "AWS Batch Watcher is terminating. Error: %v", err)
				return
			}
		})()
	})
}

func (e Executor) doSync(ctx context.Context) ([]Job, error) {
	jobs, err := e.jobsStore.Checkpoint(ctx)
	if err != nil {
		return nil, err
	}

	if len(jobs) > 0 {
		logger.Infof(ctx, "Listing aws batch jobs status for %v job(s).", len(jobs))
	} else {
		logger.Debugf(ctx, "Listing aws batch jobs status for %v job(s).", len(jobs))
	}

	jobIds := sets.NewString()
	crdMap := make(map[JobID]Job)
	for _, j := range jobs {
		if len(ID) == 0 {
			logger.Warnf(ctx, "AWS Job ID is empty for job [%v] and owner [%v]. Skipping sync.", Name, Owner)
		} else {
			jobIds.Insert(ID)
			crdMap[ID] = j
		}
	}

	newJobs := make([]Job, 0, len(jobIds)+1)
	for chunk := range internal.ToChunks(ctx, jobIds.List(), batchChunkSize) {
		details, err := GetJobDetailsBatch(ctx, chunk)
		if err != nil {
			logger.Errorf(ctx, "Failed to get job details from batch. Error: %v", err)
			return nil, errors.WrapError(err, errors.NewUnknownError("Failed to get job details from batch"))
		}
		detailsMap := make(map[JobID]*batch.JobDetail, len(details))
		for _, d := range details {
			if d.JobId == nil {
				logger.Errorf(ctx, "Missing job id in AWS Batch job name: %s.", d.JobName)
				continue
			}
			detailsMap[*d.JobId] = d
		}
		for _, j := range chunk {
			job := crdMap[j]
			d, ok := detailsMap[j]
			if !ok {
				logger.Errorf(ctx, "Job details not found for JobID [%s]", j)
				d = nil
			}
			newJobs = append(newJobs, jobDetailToJob(ctx, d, job))
		}
	}

	return newJobs, nil
}
