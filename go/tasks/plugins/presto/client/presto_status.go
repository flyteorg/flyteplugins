package client

import (
	"context"
	"github.com/lyft/flyteplugins/go/tasks/plugins/cmd"
	"github.com/lyft/flytestdlib/logger"
	"strings"
)

// This type is meant only to encapsulate the response coming from Presto as a type, it is
// not meant to be stored locally.
const (
	PrestoStatusUnknown   cmd.CommandStatus = "UNKNOWN"
	PrestoStatusQueued    cmd.CommandStatus = "QUEUED"
	PrestoStatusRunning   cmd.CommandStatus = "RUNNING"
	PrestoStatusFinished  cmd.CommandStatus = "FINISHED"
	PrestoStatusFailed    cmd.CommandStatus = "FAILED"
	PrestoStatusCancelled cmd.CommandStatus = "CANCELLED"
)

var PrestoStatuses = map[cmd.CommandStatus]struct{}{
	PrestoStatusUnknown:   {},
	PrestoStatusQueued:    {},
	PrestoStatusRunning:   {},
	PrestoStatusFinished:  {},
	PrestoStatusFailed:    {},
	PrestoStatusCancelled: {},
}

func NewPrestoStatus(ctx context.Context, state string) cmd.CommandStatus {
	upperCased := strings.ToUpper(state)
	if strings.Contains(upperCased, "FAILED") {
		return PrestoStatusFailed
	} else if _, ok := PrestoStatuses[cmd.CommandStatus(upperCased)]; ok {
		return cmd.CommandStatus(upperCased)
	} else {
		logger.Warnf(ctx, "Invalid Presto Status found: %v", state)
		return PrestoStatusUnknown
	}
}
