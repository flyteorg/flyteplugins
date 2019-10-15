package workqueue

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/stretchr/testify/assert"

	"github.com/lyft/flytestdlib/promutils"
)

type singleStatusProcessor struct {
	targetStatus WorkStatus
}

func (s singleStatusProcessor) Process(ctx context.Context, workItem WorkItem) (WorkStatus, error) {
	return s.targetStatus, nil
}

func newSingleStatusProcessor(status WorkStatus) singleStatusProcessor {
	return singleStatusProcessor{targetStatus: status}
}

type alwaysFailingProcessor struct{}

func (alwaysFailingProcessor) Process(ctx context.Context, workItem WorkItem) (WorkStatus, error) {
	return WorkStatusNotDone, fmt.Errorf("this processor always errors")
}

func TestWorkStatus_IsTerminal(t *testing.T) {
	tests := []struct {
		w    WorkStatus
		want bool
	}{
		{WorkStatusSucceeded, true},
		{WorkStatusNotDone, false},
		{WorkStatusFailed, true},
	}
	for _, tt := range tests {
		t.Run(string(tt.w), func(t *testing.T) {
			if got := tt.w.IsTerminal(); got != tt.want {
				t.Errorf("WorkStatus.IsTerminal() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_workItemCache_Get(t *testing.T) {
	l, err := lru.New(10)
	assert.NoError(t, err)

	c := workItemCache{Cache: l}
	item := &workItemWrapper{
		id:      "ABC",
		payload: "hello",
	}
	c.Add(item)

	tests := []struct {
		name      string
		c         workItemCache
		args      WorkItemID
		wantItem  *workItemWrapper
		wantFound bool
	}{
		{"Found", c, "ABC", item, true},
		{"NotFound", c, "EFG", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i, gotFound := tt.c.Get(tt.args)
			if gotFound != tt.wantFound {
				t.Errorf("workItemCache.Get() gotFound = %v, want %v", gotFound, tt.wantFound)
			}

			if tt.wantItem != nil {
				assert.Equal(t, tt.wantItem.ID(), i.ID())
				assert.Equal(t, tt.wantItem.Item(), i.Item())
				assert.Equal(t, tt.wantItem.Error(), i.Error())
				assert.Equal(t, tt.wantItem.Status(), i.Status())
			}
		})
	}
}

func Test_workItemCache_Add(t *testing.T) {
	l, err := lru.New(1)
	assert.NoError(t, err)

	c := workItemCache{Cache: l}

	tests := []struct {
		name        string
		c           workItemCache
		args        *workItemWrapper
		wantEvicted bool
	}{
		{"NotEvicted", c, &workItemWrapper{id: "abc"}, false},
		{"NotEvicted2", c, &workItemWrapper{id: "abc"}, false},
		{"Evicted", c, &workItemWrapper{id: "efg"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotEvicted := tt.c.Add(tt.args); gotEvicted != tt.wantEvicted {
				t.Errorf("workItemCache.Add() = %v, want %v", gotEvicted, tt.wantEvicted)
			}
		})
	}
}

func Test_queue_Queue(t *testing.T) {
	t.Run("Err when not started", func(t *testing.T) {
		q, err := NewIndexedWorkQueue(newSingleStatusProcessor(WorkStatusSucceeded), Config{Workers: 1, MaxRetries: 0, IndexCacheMaxItems: 1}, promutils.NewTestScope())
		assert.NoError(t, err)
		assert.Error(t, q.Queue("abc", "abc"))
	})

	t.Run("Started first", func(t *testing.T) {
		q, err := NewIndexedWorkQueue(newSingleStatusProcessor(WorkStatusSucceeded), Config{Workers: 1, MaxRetries: 0, IndexCacheMaxItems: 1}, promutils.NewTestScope())
		assert.NoError(t, err)

		ctx, cancelNow := context.WithCancel(context.Background())
		assert.NoError(t, q.Start(ctx))
		assert.NoError(t, q.Queue("abc", "abc"))
		cancelNow()
	})
}

func Test_queue_Get(t *testing.T) {
	q, err := NewIndexedWorkQueue(newSingleStatusProcessor(WorkStatusSucceeded), Config{Workers: 1, MaxRetries: 0, IndexCacheMaxItems: 1}, promutils.NewTestScope())
	assert.NoError(t, err)

	ctx, cancelNow := context.WithCancel(context.Background())
	defer cancelNow()
	assert.NoError(t, q.Start(ctx))

	assert.NoError(t, q.Queue("abc", "hello"))

	tests := []struct {
		name      string
		q         IndexedWorkQueue
		id        WorkItemID
		wantInfo  WorkItemInfo
		wantFound bool
		wantErr   bool
	}{
		{"Found", q, "abc", &workItemWrapper{
			status:  WorkStatusSucceeded,
			id:      "abc",
			payload: "hello",
		}, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotInfo, gotFound, err := tt.q.Get(tt.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("queue.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotInfo, tt.wantInfo) {
				t.Errorf("queue.Get() gotInfo = %v, want %v", gotInfo, tt.wantInfo)
			}
			if gotFound != tt.wantFound {
				t.Errorf("queue.Get() gotFound = %v, want %v", gotFound, tt.wantFound)
			}
		})
	}
}

func Test_queue_Start(t *testing.T) {
	q, err := NewIndexedWorkQueue(newSingleStatusProcessor(WorkStatusSucceeded), Config{Workers: 1, MaxRetries: 0, IndexCacheMaxItems: 1}, promutils.NewTestScope())
	assert.NoError(t, err)

	ctx, cancelNow := context.WithCancel(context.Background())
	defer cancelNow()
	assert.NoError(t, q.Start(ctx))
	assert.Error(t, q.Start(ctx))
}

func Test_Failures(t *testing.T) {
	q, err := NewIndexedWorkQueue(alwaysFailingProcessor{}, Config{Workers: 1, MaxRetries: 0, IndexCacheMaxItems: 1}, promutils.NewTestScope())
	assert.NoError(t, err)

	ctx, cancelNow := context.WithCancel(context.Background())
	defer cancelNow()
	assert.NoError(t, q.Start(ctx))

	assert.NoError(t, q.Queue("abc", "hello"))
	time.Sleep(100 * time.Millisecond)
	info, found, err := q.Get("abc")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, WorkStatusFailed.String(), info.Status().String())
}
