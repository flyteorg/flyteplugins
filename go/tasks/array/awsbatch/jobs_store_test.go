/*
 * Copyright (c) 2018 Lyft. All rights reserved.
 */

package awsbatch

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/lyft/flytestdlib/promutils"

	"github.com/stretchr/testify/assert"
)

func createJobWithID(id JobID) Job {
	return Job{
		Name: id,
		ID:   id,
	}
}

func TestGetJobsStore(t *testing.T) {
	s := NewInMemoryStore(100, promutils.NewTestScope()).(*store)
	assert.NotNil(t, s)
	assert.NotNil(t, s)
}

func TestStore_Add(t *testing.T) {
	s := NewInMemoryStore(100, promutils.NewTestScope())
	assert.NotNil(t, s)
	ok, err := Add(context.TODO(), createJobWithID("RandomId"))
	assert.NoError(t, err)
	assert.True(t, ok)

	// Inserting the same id should fail.
	ok, err = Add(context.TODO(), createJobWithID("RandomId"))
	assert.NoError(t, err)
	assert.False(t, ok)
}

func TestStore_Checkpoint(t *testing.T) {
	s := NewInMemoryStore(100, promutils.NewTestScope())
	assert.NotNil(t, s)
	_, err := Add(context.TODO(), createJobWithID("Id1"))
	assert.NoError(t, err)
	_, err = Add(context.TODO(), createJobWithID("Id2"))
	assert.NoError(t, err)

	jobs, err := Checkpoint(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 2, len(jobs))
	sort.SliceStable(jobs, func(i, j int) bool {
		return ID < ID
	})

	assert.Equal(t, "Id1", ID)
	assert.Equal(t, "Id2", ID)
}

func TestStore_Get(t *testing.T) {
	s := NewInMemoryStore(100, promutils.NewTestScope())
	assert.NotNil(t, s)
	_, err := Add(context.TODO(), createJobWithID("Id1"))
	assert.NoError(t, err)
	_, err = Add(context.TODO(), createJobWithID("Id2"))
	assert.NoError(t, err)

	j, found, err := Get(context.TODO(), "Id2")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "Id2", ID)

	j, found, err = Get(context.TODO(), "Id3")
	assert.NoError(t, err)
	assert.False(t, found)
}

// Current values:
// BenchmarkStore_AddOrUpdate-8   	  500000	      2677 ns/op
func BenchmarkStore_AddOrUpdate(b *testing.B) {
	s := NewInMemoryStore(b.N, promutils.NewTestScope())
	assert.NotNil(b, s)
	for i := 0; i < b.N; i++ {
		err := AddOrUpdate(context.TODO(), createJobWithID("Id1"))
		assert.NoError(b, err)
	}
}

// Current values:
// BenchmarkStore_Get-8           	  200000	     11400 ns/op
func BenchmarkStore_Get(b *testing.B) {
	n := b.N
	s := NewInMemoryStore(n*2, promutils.NewTestScope())
	assert.NotNil(b, s)
	createName := func(i int) string {
		return fmt.Sprintf("Id%v", i)
	}

	addedCnt := 0
	for i := 0; i < n; i++ {
		added, err := Add(context.TODO(), createJobWithID(createName(i)))
		assert.NoError(b, err)
		assert.True(b, added)
		if added {
			addedCnt++
		}
	}

	b.ResetTimer()

	for i := 0; i < n; i++ {
		_, found, err := Get(context.TODO(), createName(i))
		assert.NoError(b, err)
		assert.True(b, found, createName(i))
	}
}
