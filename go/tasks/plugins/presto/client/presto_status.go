package client

import (
	"context"
	"strings"

	"github.com/lyft/flyteplugins/go/tasks/plugins/svc"
	"github.com/lyft/flytestdlib/logger"
)

// This type is meant only to encapsulate the response coming from Presto as a type, it is
// not meant to be stored locally.
const (
	PrestoStatusUnknown   svc.CommandStatus = "UNKNOWN"
	PrestoStatusQueued    svc.CommandStatus = "QUEUED"
	PrestoStatusRunning   svc.CommandStatus = "RUNNING"
	PrestoStatusFinished  svc.CommandStatus = "FINISHED"
	PrestoStatusFailed    svc.CommandStatus = "FAILED"
	PrestoStatusCancelled svc.CommandStatus = "CANCELLED"
)

var PrestoStatuses = map[svc.CommandStatus]struct{}{
	PrestoStatusUnknown:   {},
	PrestoStatusQueued:    {},
	PrestoStatusRunning:   {},
	PrestoStatusFinished:  {},
	PrestoStatusFailed:    {},
	PrestoStatusCancelled: {},
}

func NewPrestoStatus(ctx context.Context, state string) svc.CommandStatus {
	upperCased := strings.ToUpper(state)
	if strings.Contains(upperCased, "FAILED") {
		return PrestoStatusFailed
	} else if _, ok := PrestoStatuses[svc.CommandStatus(upperCased)]; ok {
		return svc.CommandStatus(upperCased)
	} else {
		logger.Warnf(ctx, "Invalid Presto Status found: %v", state)
		return PrestoStatusUnknown
	}
}
