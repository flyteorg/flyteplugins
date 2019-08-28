package k8sarray

import (
	"context"
	"fmt"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/storage"
)

type DoOnceWorkerInterface interface {
	Queue(once DoOnce) error
	Get(id string) (DoOnce, error)
}

type Processor interface {
	Process(ctx context.Context, workItem DoOnce) (WorkStatus, error)
}

func NewDoOnceWorker(processor Processor) (DoOnceWorkerInterface, error) {
	return DoOnceWorker{}, nil
}

type DoOnceWorker struct {
	queue []WorkItem
	index map[string]WorkItem
	processor Processor
}

func (DoOnceWorker) Queue(once DoOnce) error {
	panic("implement me")
}

func (DoOnceWorker) Get(id string) (DoOnce, error) {
	panic("implement me")
}

type WorkItem struct {
	payload DoOnce
	retryCount int
	err error
}

type DoOnce interface {
	GetId() string
	GetPayload()
	GetWorkStatus() WorkStatus
}

type WorkStatus int

const (
	WorkStatusNotDone WorkStatus = iota
	WorkStatusDone
)













type DiscoveryWorkItem struct {
	// WorkItem outputs:
	cached bool

	// WorkItem Inputs:
	outputPath storage.DataReference
	// Inputs to query data catalog
	inputPath storage.DataReference
	taskTemplate *core.TaskTemplate
}

func (*DiscoveryWorkItem) GetId() string {
	panic("implement me")
}

func (*DiscoveryWorkItem) GetPayload() {
	panic("implement me")
}

func (*DiscoveryWorkItem) GetWorkStatus() WorkStatus {
	panic("implement me")
}


type CatalogProcessor struct {
	// CatalogClient
	protobufWriter storage.ProtobufStore
}

func (CatalogProcessor) Process(ctx context.Context, workItem DoOnce) (WorkStatus, error) {
	_, casted := workItem.(*DiscoveryWorkItem)
	if !casted {
		return WorkStatusNotDone, fmt.Errorf("blah")
	}

	// Download wi.inputsPath
	// Call catalog with wi.InputsPath ... etc as inputs
	// if cached, write outputs using protobufWriter
	// mutate wi.cached
	return WorkStatusDone, nil
}