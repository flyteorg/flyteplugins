package core

import (
	"context"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/lyft/flyteplugins/go/tasks/v1/pluginmachinery/io"
)

type CatalogClient interface {
	Get(ctx context.Context, tr TaskReader, input io.InputReader) (*core.LiteralMap, error)
	Put(ctx context.Context, tCtx TaskExecutionContext, op io.OutputReader) error
}
