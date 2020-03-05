package command

import (
	"context"
)

type CommandClient interface {
	ExecuteCommand(ctx context.Context, commandStr string, extraArgs interface{}) (interface{}, error)
	KillCommand(ctx context.Context, commandID string) error
	GetCommandStatus(ctx context.Context, commandID string) (interface{}, error)
}
