package core

import "context"

type AllocationStatus string

const (
	// This is the enum returned when there's an error
	AllocationUndefined AllocationStatus = "ResourceGranted"

	// Go for it
	AllocationStatusGranted AllocationStatus = "ResourceGranted"

	// This means that no resources are available globally.  This is the only rejection message we use right now.
	AllocationStatusExhausted AllocationStatus = "ResourceExhausted"

	// We're not currently using this - but this would indicate that things globally are okay, but that your
	// own namespace is too busy
	AllocationStatusNamespaceQuotaExceeded AllocationStatus = "NamespaceQuotaExceeded"
)

// Resource Manager manages a single resource type, and each allocation is of size one
type ResourceManager interface {
	AllocateResource(ctx context.Context, namespace string, allocationToken string) (AllocationStatus, error)
	ReleaseResource(ctx context.Context, namespace string, allocationToken string) error
}

