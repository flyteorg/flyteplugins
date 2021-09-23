package catalog

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/datacatalog"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/io"
)

//go:generate mockery -all -case=underscore

// Metadata to be associated with the catalog object
type Metadata struct {
	WorkflowExecutionIdentifier *core.WorkflowExecutionIdentifier
	NodeExecutionIdentifier     *core.NodeExecutionIdentifier
	TaskExecutionIdentifier     *core.TaskExecutionIdentifier
}

// An identifier for a catalog object.
type Key struct {
	Identifier     core.Identifier
	CacheVersion   string
	TypedInterface core.TypedInterface
	InputReader    io.InputReader
}

func (k Key) String() string {
	return fmt.Sprintf("%v:%v", k.Identifier, k.CacheVersion)
}

// Indicates that status of the query to Catalog. This can be returned for both Get and Put calls
type Status struct {
	cacheStatus core.CatalogCacheStatus
	metadata    *core.CatalogMetadata
}

func (s Status) GetCacheStatus() core.CatalogCacheStatus {
	return s.cacheStatus
}

func (s Status) GetMetadata() *core.CatalogMetadata {
	return s.metadata
}

func NewStatus(cacheStatus core.CatalogCacheStatus, md *core.CatalogMetadata) Status {
	return Status{cacheStatus: cacheStatus, metadata: md}
}

// Indicates the Entry in Catalog that was populated
type Entry struct {
	outputs io.OutputReader
	status  Status
}

func (e Entry) GetOutputs() io.OutputReader {
	return e.outputs
}

func (e Entry) GetStatus() Status {
	return e.status
}

func NewFailedCatalogEntry(status Status) Entry {
	return Entry{status: status}
}

func NewCatalogEntry(outputs io.OutputReader, status Status) Entry {
	return Entry{outputs: outputs, status: status}
}

// Indicates the Reservation in Catalog
type ReservationEntry struct {
	expiresAt         time.Time
	heartbeatInterval time.Duration
	ownerID           string
	status            core.CatalogReservationStatus
}

func (r ReservationEntry) GetExpiresAt() time.Time {
	return r.expiresAt
}

func (r ReservationEntry) GetHeartbeatInterval() time.Duration {
	return r.heartbeatInterval
}

func (r ReservationEntry) GetOwnerID() string {
	return r.ownerID
}

func (r ReservationEntry) GetStatus() core.CatalogReservationStatus {
	return r.status
}

func NewReservationEntryStatus(status core.CatalogReservationStatus) ReservationEntry {
	duration := 0 * time.Second
	return ReservationEntry{
		expiresAt:         time.Time{},
		heartbeatInterval: duration,
		ownerID:           "",
		status:            status,
	}
}

func NewReservationEntry(expiresAt time.Time, heartbeatInterval time.Duration, ownerID string, status core.CatalogReservationStatus) ReservationEntry {
	return ReservationEntry{
		expiresAt:         expiresAt,
		heartbeatInterval: heartbeatInterval,
		ownerID:           ownerID,
		status:            status,
	}
}

// Default Catalog client that allows memoization and indexing of intermediate data in Flyte
type Client interface {
	Get(ctx context.Context, key Key) (Entry, error)
	GetOrExtendReservation(ctx context.Context, key Key, ownerID string, heartbeatInterval time.Duration) (*datacatalog.Reservation, error)
	Put(ctx context.Context, key Key, reader io.OutputReader, metadata Metadata) (Status, error)
	ReleaseReservation(ctx context.Context, key Key, ownerID string) error
}

func IsNotFound(err error) bool {
	taskStatus, ok := grpcStatus.FromError(err)
	return ok && taskStatus.Code() == codes.NotFound
}
