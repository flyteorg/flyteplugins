package core

import (
	"context"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/lyft/flyteplugins/go/tasks/v1/pluginmachinery/io"
)

// An interface to interest with the catalog service
type CatalogClient interface {
	// Returns if an entry exists for the given task and input. It returns the data as a LiteralMap
	Get(ctx context.Context, tr TaskReader, input io.InputReader) (*core.LiteralMap, error)
	// Adds a new entry to catalog for the given task execution context and the generated output
	Put(ctx context.Context, tCtx TaskExecutionContext, op io.OutputReader) error
}
