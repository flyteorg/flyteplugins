package client

import (
	"context"
	"strings"

	"github.com/lyft/flytestdlib/logger"
)

type PrestoStatus string

// This type is meant only to encapsulate the response coming from Presto as a type, it is
// not meant to be stored locally.
const (
	PrestoStatusUnknown   PrestoStatus = "UNKNOWN"
	PrestoStatusQueued    PrestoStatus = "QUEUED"
	PrestoStatusRunning   PrestoStatus = "RUNNING"
	PrestoStatusFinished  PrestoStatus = "FINISHED"
	PrestoStatusFailed    PrestoStatus = "FAILED"
	PrestoStatusCancelled PrestoStatus = "CANCELLED"
)

var PrestoStatuses = map[PrestoStatus]struct{}{
	PrestoStatusUnknown:   {},
	PrestoStatusQueued:    {},
	PrestoStatusRunning:   {},
	PrestoStatusFinished:  {},
	PrestoStatusFailed:    {},
	PrestoStatusCancelled: {},
}

func NewPrestoStatus(ctx context.Context, state string) PrestoStatus {
	upperCased := strings.ToUpper(state)
	if strings.Contains(upperCased, "FAILED") {
		return PrestoStatusFailed
	} else if _, ok := PrestoStatuses[PrestoStatus(upperCased)]; ok {
		return PrestoStatus(upperCased)
	} else {
		logger.Warnf(ctx, "Invalid Presto Status found: %v", state)
		return PrestoStatusUnknown
	}
}
