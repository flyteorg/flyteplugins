package workqueue

import (
	"context"
	"fmt"
	"sync"

	"github.com/lyft/flyteplugins/go/tasks/errors"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/promutils"

	lru "github.com/hashicorp/golang-lru"

	"k8s.io/client-go/util/workqueue"
)

//go:generate mockery -all -case=underscore
//go:generate enumer --type=WorkStatus

type WorkItemID = string
type WorkStatus uint8

const (
	WorkStatusNotDone WorkStatus = iota
	WorkStatusSucceeded
	WorkStatusFailed
)

const (
	ErrNotYetStarted errors.ErrorCode = "NOT_STARTED"
)

func (w WorkStatus) IsTerminal() bool {
	return w == WorkStatusFailed || w == WorkStatusSucceeded
}

// WorkItem is a generic item that can be stored in the work queue.
type WorkItem interface{}

// Represents the result of the work item processing.
type WorkItemInfo interface {
	Item() WorkItem
	ID() WorkItemID
	Status() WorkStatus
	Error() error
}

// Represents the indexed queue semantics. An indexed work queue is a work queue that additionally keeps track of the
// final processing results of work items.
type IndexedWorkQueue interface {
	// Queues the item to be processed. If the item is already in the cache or has been processed before (and is still
	// in-memory), it'll not be added again.
	Queue(id WorkItemID, once WorkItem) error

	// Retrieves an item by id.
	Get(id WorkItemID) (info WorkItemInfo, found bool, err error)

	// Start must be called before queuing items into the queue.
	Start(ctx context.Context) error
}

// Represents the processor logic to operate on work items.
type Processor interface {
	Process(ctx context.Context, workItem WorkItem) (WorkStatus, error)
}

type workItemWrapper struct {
	id         WorkItemID
	payload    WorkItem
	status     WorkStatus
	retryCount uint
	err        error
}

func (w workItemWrapper) Item() WorkItem {
	return w.payload
}

func (w workItemWrapper) ID() WorkItemID {
	return w.id
}

func (w workItemWrapper) Status() WorkStatus {
	return w.status
}

func (w workItemWrapper) Error() error {
	return w.err
}

func (w workItemWrapper) Clone() workItemWrapper {
	return w
}

type queue struct {
	wlock      sync.Mutex
	rlock      sync.RWMutex
	workers    int
	maxRetries int
	started    bool
	queue      workqueue.Interface
	index      workItemCache
	processor  Processor
}

type workItemCache struct {
	*lru.Cache
}

func (c workItemCache) Get(id WorkItemID) (item *workItemWrapper, found bool) {
	o, found := c.Cache.Get(id)
	if !found {
		return nil, found
	}

	return o.(*workItemWrapper), true
}

func (c workItemCache) Add(item *workItemWrapper) (evicted bool) {
	return c.Cache.Add(item.id, item)
}

func (q *queue) Queue(id WorkItemID, once WorkItem) error {
	q.wlock.Lock()
	defer q.wlock.Unlock()

	if !q.started {
		return errors.Errorf(ErrNotYetStarted, "Queue must be started before enqueuing any item.")
	}

	if _, found := q.index.Get(id); found {
		return nil
	}

	wrapper := &workItemWrapper{
		id:      id,
		payload: once,
	}

	q.index.Add(wrapper)
	q.queue.Add(wrapper)
	return nil
}

func (q queue) Get(id WorkItemID) (info WorkItemInfo, found bool, err error) {
	q.rlock.Lock()
	defer q.rlock.Unlock()

	wrapper, found := q.index.Get(id)
	if !found {
		return nil, found, nil
	}

	v := wrapper.Clone()
	return &v, true, nil
}

func (q *queue) Start(ctx context.Context) error {
	q.wlock.Lock()
	defer q.wlock.Unlock()

	if q.started {
		return fmt.Errorf("queue already started")
	}

	for i := 0; i < q.workers; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					logger.Debug(ctx, "Context cancelled. Shutting down.")
					return
				default:
					item, shutdown := q.queue.Get()
					if shutdown {
						logger.Debug(ctx, "Work queue is shutting down.")
						return
					}

					wrapperV := item.(*workItemWrapper).Clone()
					wrapper := &wrapperV
					ws, err := q.processor.Process(ctx, wrapper.payload)
					if err != nil {
						wrapper.retryCount++
						wrapper.err = err
						if wrapper.retryCount >= uint(q.maxRetries) {
							logger.Debugf(ctx, "WorkItem [%v] exhausted all retries. Last Error: %v.",
								wrapper.ID(), err)
							wrapper.status = WorkStatusFailed
							ws = WorkStatusFailed
							q.index.Add(wrapper)
							continue
						}
					}

					wrapper.status = ws
					q.index.Add(wrapper)
					if !ws.IsTerminal() {
						q.queue.Add(wrapper)
					}
				}
			}
		}()
	}

	q.started = true
	return nil
}

// Instantiates a new Indexed Work queue.
func NewIndexedWorkQueue(processor Processor, cfg Config, metricsScope promutils.Scope) (IndexedWorkQueue, error) {
	cache, err := lru.New(cfg.IndexCacheMaxItems)
	if err != nil {
		return nil, err
	}

	return &queue{
		wlock:      sync.Mutex{},
		rlock:      sync.RWMutex{},
		workers:    cfg.Workers,
		maxRetries: cfg.MaxRetries,
		// TODO: assign name to get metrics
		queue:     workqueue.NewNamed(metricsScope.CurrentScope()),
		index:     workItemCache{Cache: cache},
		processor: processor,
	}, nil
}
