/*
 * Copyright (c) 2018 Lyft. All rights reserved.
 */

package awsbatch

import (
	v1 "k8s.io/api/core/v1"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
)

const (
	// Keep these in-sync with flyteAdmin @
	// https://github.com/lyft/flyteadmin/commit/15dc00010d379c9240657326e0e95c60993ba30b#diff-fc047e54b9dd82ca7c89ac9b32cb07b3R37
	PrimaryTaskQueueKey = "primary_queue"
	DynamicTaskQueueKey = "dynamic_queue"
)

type JobConfig struct {
	PrimaryTaskQueue string `json:"master_queue"`
	DynamicTaskQueue string `json:"child_queue"`
}

func (j *JobConfig) setKeyIfKnown(key, value string) bool {
	switch key {
	case PrimaryTaskQueueKey:
		j.PrimaryTaskQueue = value
		return true
	case DynamicTaskQueueKey:
		j.DynamicTaskQueue = value
		return true
	default:
		return false
	}
}

func (j *JobConfig) MergeFromKeyValuePairs(pairs []*core.KeyValuePair) *JobConfig {
	for _, entry := range pairs {
		j.setKeyIfKnown(entry.Key, entry.Value)
	}

	return j
}

func (j *JobConfig) MergeFromConfigMap(configMap *v1.ConfigMap) *JobConfig {
	if configMap == nil {
		return j
	}

	for key, value := range configMap.Data {
		j.setKeyIfKnown(key, value)
	}

	return j
}

func newJobConfig() (cfg *JobConfig) {
	return &JobConfig{}
}
