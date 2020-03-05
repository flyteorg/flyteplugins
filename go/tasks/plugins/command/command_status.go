package command

import (
	"context"
	"github.com/lyft/flytestdlib/logger"
	"strings"
)

// This type is meant only to encapsulate the response coming from a command as a type, it is
// not meant to be stored locally.
type CommandStatus string

const (
	CommandStatusUnknown   CommandStatus = "UNKNOWN"
	CommandStatusWaiting   CommandStatus = "WAITING"
	CommandStatusRunning   CommandStatus = "RUNNING"
	CommandStatusDone      CommandStatus = "DONE"
	CommandStatusError     CommandStatus = "ERROR"
	CommandStatusCancelled CommandStatus = "CANCELLED"
)

var CommandStatuses = map[CommandStatus]struct{}{
	CommandStatusUnknown:   {},
	CommandStatusWaiting:   {},
	CommandStatusRunning:   {},
	CommandStatusDone:      {},
	CommandStatusError:     {},
	CommandStatusCancelled: {},
}

func NewCommandStatus(ctx context.Context, status string) CommandStatus {
	upperCased := strings.ToUpper(status)
	if _, ok := CommandStatuses[CommandStatus(upperCased)]; ok {
		return CommandStatus(upperCased)
	}
	logger.Warnf(ctx, "Invalid Command Status found: %v", status)
	return CommandStatusUnknown
}
