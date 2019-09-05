package workqueue

import (
	"context"
	"fmt"
	"sync"

	lru "github.com/hashicorp/golang-lru"

	"k8s.io/client-go/util/workqueue"
)

type WorkItemID = string
type WorkStatus uint8

const (
	WorkStatusNotDone WorkStatus = iota
	WorkStatusDone
)

type IndexedWorkQueue interface {
	Queue(once WorkItem) error
	Get(id WorkItemID) (item WorkItem, found bool, err error)
}

type Processor interface {
	Process(ctx context.Context, workItem WorkItem) (WorkStatus, error)
}

type WorkItem interface {
	GetId() WorkItemID
	GetWorkStatus() WorkStatus
}

type workItemWrapper struct {
	payload    WorkItem
	retryCount uint
	err        error
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
	return c.Cache.Add(item.payload.GetId(), item)
}

func (q *queue) Queue(once WorkItem) error {
	q.wlock.Lock()
	defer q.wlock.Unlock()

	if _, found := q.index.Get(once.GetId()); found {
		return nil
	}

	wrapper := &workItemWrapper{
		payload: once,
	}

	q.queue.Add(wrapper)
	q.index.Add(wrapper)
	return nil
}

func (q queue) Get(id WorkItemID) (item WorkItem, found bool, err error) {
	q.rlock.Lock()
	defer q.rlock.Unlock()

	wrapper, found := q.index.Get(id)
	if !found {
		return nil, found, nil
	}

	if wrapper.err != nil {
		return nil, true, wrapper.err
	}

	return wrapper.payload, true, nil
}

func (q queue) Start(ctx context.Context) error {
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
					// TODO: log
					return
				default:
					item, shutdown := q.queue.Get()
					if shutdown {
						// TODO: log
						return
					}

					wrapper := item.(*workItemWrapper)
					ws, err := q.processor.Process(ctx, wrapper.payload)
					if err != nil {
						wrapper.retryCount++
						wrapper.err = err
						if wrapper.retryCount >= uint(q.maxRetries) {
							// TODO: log
						} else {
							q.queue.Add(wrapper)
						}

						continue
					}

					if ws != WorkStatusDone {
						q.queue.Add(wrapper)
					}
				}
			}
		}()
	}

	q.started = true
	return nil
}

func NewIndexedWorkQueue(processor Processor, cfg Config) (IndexedWorkQueue, error) {
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
		queue: workqueue.New(),
		// TODO: Default size?
		index:     workItemCache{Cache: cache},
		processor: processor,
	}, nil
}
