package remote

import (
	"time"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/remote"
)

// Phase represents current phase of the execution
type Phase int

const (
	PhaseNotStarted Phase = iota
	PhaseAllocationTokenAcquired
	PhaseResourcesCreated
	PhaseSucceeded
	PhaseFailed
)

func (p Phase) IsTerminal() bool {
	return p == PhaseSucceeded || p == PhaseFailed
}

type State struct {
	Phase Phase `json:"phase"`

	// ResourceKeys contain the resource keys of the launched resources. If this is empty, it means
	// the job used the default (preferred) name from tCtx.GetGeneratedName()
	ResourceMeta remote.ResourceMeta `json:"resourceMeta,omitempty"`

	// This number keeps track of the number of failures within the sync function. Without this, what happens in
	// the sync function is entirely opaque. Note that this field is completely orthogonal to Flyte system/node/task
	// level retries, just errors from hitting API, inside the sync loop
	SyncFailureCount int `json:"syncFailureCount,omitempty"`

	LatestPhaseInfo core.PhaseInfo `json:"latestPhaseInfo,omitEmpty"`

	// In creating the resource, this is the number of failures
	CreationFailureCount int `json:"creationFailureCount,omitempty"`

	// The time the execution first requests for an allocation token
	AllocationTokenRequestStartTime time.Time `json:"allocationTokenRequestStartTime,omitempty"`
}
