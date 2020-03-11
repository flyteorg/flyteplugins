package client

import (
	"context"
	"github.com/lyft/flyteplugins/go/tasks/plugins/command"
	"github.com/lyft/flytestdlib/logger"
	"strings"
)

// This type is meant only to encapsulate the response coming from Presto as a type, it is
// not meant to be stored locally.

const (
	PrestoStatusUnknown   command.CommandStatus = "UNKNOWN"
	PrestoStatusQueued    command.CommandStatus = "QUEUED"
	PrestoStatusRunning   command.CommandStatus = "RUNNING"
	PrestoStatusFinished  command.CommandStatus = "FINISHED"
	PrestoStatusFailed    command.CommandStatus = "FAILED"
	PrestoStatusCancelled command.CommandStatus = "CANCELLED"
)

var PrestoStatuses = map[command.CommandStatus]struct{}{
	PrestoStatusUnknown:   {},
	PrestoStatusQueued:    {},
	PrestoStatusRunning:   {},
	PrestoStatusFinished:  {},
	PrestoStatusFailed:    {},
	PrestoStatusCancelled: {},
}

func NewPrestoStatus(ctx context.Context, state string) command.CommandStatus {
	upperCased := strings.ToUpper(state)
	if strings.Contains(upperCased, "FAILED") {
		return PrestoStatusFailed
	} else if _, ok := PrestoStatuses[command.CommandStatus(upperCased)]; ok {
		return command.CommandStatus(upperCased)
	} else {
		logger.Warnf(ctx, "Invalid Presto Status found: %v", state)
		return PrestoStatusUnknown
	}
}
