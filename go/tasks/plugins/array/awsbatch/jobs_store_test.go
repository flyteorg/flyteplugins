/*
 * Copyright (c) 2018 Lyft. All rights reserved.
 */

package awsbatch

import (
	"context"
	"fmt"
	"testing"

	"github.com/lyft/flyteplugins/go/tasks/plugins/array/awsbatch/config"

	"github.com/lyft/flytestdlib/promutils"

	"github.com/stretchr/testify/assert"
)

func createJobWithID(id JobID) *Job {
	return &Job{
		ID: id,
	}
}

func newJobsStore(t testing.TB, batchClient Client) *JobStore {
	store, err := NewJobStore(context.TODO(), batchClient, 1000, config.JobStoreConfig{
		CacheSize:      1,
		Parallelizm:    1,
		BatchChunkSize: 1,
	}, EventHandler{}, promutils.NewTestScope())
	assert.NoError(t, err)
	return &store
}
func TestGetJobsStore(t *testing.T) {
	s := newJobsStore(t, nil)
	assert.NotNil(t, s)
	assert.NotNil(t, s)
}

func TestJobStore_GetOrCreate(t *testing.T) {
	s := newJobsStore(t, nil)
	assert.NotNil(t, s)
	ok, err := s.GetOrCreate("RandomId", createJobWithID("RandomId"))
	assert.NoError(t, err)
	assert.NotNil(t, ok)
}

func TestStore_Get(t *testing.T) {
	s := newJobsStore(t, nil)
	assert.NotNil(t, s)
	j, err := s.GetOrCreate("Id1", createJobWithID("Id1"))
	assert.NoError(t, err)
	j, err = s.GetOrCreate("Id2", createJobWithID("Id2"))
	assert.NoError(t, err)

	j = s.Get("Id2")
	assert.NotNil(t, j)
	assert.Equal(t, "Id2", j.ID)

	j = s.Get("Id3")
	assert.Nil(t, j)
}

// Current values:
// BenchmarkStore_AddOrUpdate-8   	  500000	      2677 ns/op
func BenchmarkStore_GetOrUpdate(b *testing.B) {
	s := newJobsStore(b, nil)
	assert.NotNil(b, s)
	for i := 0; i < b.N; i++ {
		_, err := s.GetOrCreate("Id1", createJobWithID("Id1"))
		assert.NoError(b, err)
	}
}

// Current values:
// BenchmarkStore_Get-8           	  200000	     11400 ns/op
func BenchmarkStore_Get(b *testing.B) {
	n := b.N
	s := newJobsStore(b, nil)
	assert.NotNil(b, s)
	createName := func(i int) string {
		return fmt.Sprintf("Id%v", i)
	}

	for i := 0; i < n; i++ {
		_, err := s.GetOrCreate(createName(i), createJobWithID(createName(i)))
		assert.NoError(b, err)
	}

	b.ResetTimer()

	for i := 0; i < n; i++ {
		j := s.Get(createName(i))
		assert.NotNil(b, j)
	}
}
