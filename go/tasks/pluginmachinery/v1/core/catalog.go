package core

import (
	"context"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/io"
)

type CatalogMetadata struct {
	WorkflowExecutionIdentifier *core.WorkflowExecutionIdentifier
	NodeExecutionIdentifier *core.NodeExecutionIdentifier
	TaskExecutionIdentifier *core.TaskExecutionIdentifier
}

type CatalogKey struct {
	Identifier core.Identifier
	DiscoveryVersion string
	TypedInterface core.TypedInterface
	InputReader io.InputReader
}

// An interface to interest with the catalog service
type CatalogClient interface {
	// Returns if an entry exists for the given task and input. It returns the data as a LiteralMap
	Get(ctx context.Context, key CatalogKey) (artifactData io.OutputReader, artifactMetadata CatalogMetadata, err error)

	// Adds a new entry to catalog for the given task execution context and the generated output
	Put(ctx context.Context, key CatalogKey, artifactData io.OutputReader, artifactMetadata CatalogMetadata) error
}
