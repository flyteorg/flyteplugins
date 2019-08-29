/*
 * Copyright (c) 2018 Lyft. All rights reserved.
 */

package arraystatus

import (
	"context"
	"encoding/json"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"

	"github.com/lyft/flytedynamicjoboperator/pkg/internal/bitarray"
	"github.com/lyft/flyteplugins/go/tasks/v1/types"
	"github.com/lyft/flytestdlib/logger"
)

type JobID = string

type ArrayStatus struct {
	// Summary of the array job. It's a map of phases and how many jobs are in that phase.
	Summary ArraySummary `json:"summary"`

	// Status of every job in the array.
	Detailed bitarray.CompactArray `json:"details"`
}

type ArraySummary map[types.TaskPhase]int64

func deleteOrSet(summary ArraySummary, key types.TaskPhase, value int64) {
	if value == 0 {
		delete(summary, key)
	} else {
		summary[key] = value
	}
}

func (in ArraySummary) IncByCount(phase types.TaskPhase, count int64) {
	if existing, found := in[phase]; !found {
		in[phase] = count
	} else {
		in[phase] = existing + count
	}
}

func (in ArraySummary) Inc(phase types.TaskPhase) {
	in.IncByCount(phase, 1)
}

func (in ArraySummary) Dec(phase types.TaskPhase) {
	// TODO: Error if already 0?
	in.IncByCount(phase, -1)
}

func (in ArraySummary) MergeFrom(other ArraySummary) (updated bool) {
	// TODO: Refactor using sets
	if other == nil {
		for key := range in {
			delete(in, key)
			updated = true
		}

		return
	}

	for key, otherValue := range other {
		if value, found := in[key]; found {
			if value != otherValue {
				deleteOrSet(in, key, otherValue)
				updated = true
			}
		} else if otherValue != 0 {
			in[key] = otherValue
			updated = true
		}
	}

	for key := range in {
		if _, found := other[key]; !found {
			delete(in, key)
		}
	}

	return
}

func CopyArraySummaryFrom(original ArraySummary) ArraySummary {
	copy := make(map[types.TaskPhase]int64)
	for key, value := range original {
		copy[key] = value
	}
	return copy
}

type CustomState struct {
	JobID JobID `json:"job-id,omitempty"`
	// Optional status of the array job.
	ArrayStatus *ArrayStatus `json:"arr,omitempty"`

	ArrayProperties *ArrayJob `json:"arrJob,omitempty"`

	ArrayCachedStatus *ArrayCachedStatus `json:"taskMetadata,omitempty"`
}

// This is a status object that is returned after we make Catalog calls to see if subtasks are Cached
type ArrayCachedStatus struct {
	TaskIdentifier      *core.Identifier `json:"taskIdentifier,omitempty"`
	DiscoverableVersion string           `json:"discoverableVersion"`
	CachedArrayJobs     *bitarray.BitSet `json:"cachedArrayJobs"`
	NumCached           uint             `json:"numCached"`
}

type ArrayJob struct {
	plugins.ArrayJob
}

func (s CustomState) AsMap(ctx context.Context) (types.CustomState, error) {
	raw, err := json.Marshal(s)
	if err != nil {
		logger.Warnf(ctx, "Failed to marshal custom status as Map. Error: %v", err)
		return nil, err
	}

	m := map[string]interface{}{}
	err = json.Unmarshal(raw, &m)
	if err != nil {
		logger.Warnf(ctx, "Failed to unmarshal custom status into map. Error: %v", err)
		return nil, err
	}

	return m, nil
}

func (s *CustomState) MergeFrom(ctx context.Context, customStatus types.CustomState) error {
	raw, err := json.Marshal(customStatus)
	if err != nil {
		logger.Warnf(ctx, "Failed to marshal plugins custom status. Error: %v", err)
		return err
	}

	// json.Unmarshal doesn't wipe out existing keys from maps so nil the summary here to ensure we don't unmarshal into
	// a dirty map.
	if s.ArrayStatus != nil {
		s.ArrayStatus.Summary = nil
	}

	err = json.Unmarshal(raw, s)
	if err != nil {
		logger.Warnf(ctx, "Failed to unmarshal plugins custom status into AWS Custom status. Error: %v", err)
		return err
	}

	return nil
}
