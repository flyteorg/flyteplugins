package hive

import (
	"github.com/lyft/flyteplugins/go/tasks/plugins/array/core"
	"github.com/lyft/flyteplugins/go/tasks/plugins/hive/client"
)

func QuboleStatusToPhase(status client.QuboleStatus) core.Phase {
	switch status {
	case client.QuboleStatusUnknown:
		fallthrough
	case client.QuboleStatusWaiting:
		fallthrough
	case client.QuboleStatusCancelled:
		fallthrough
	case client.QuboleStatusDone:
		fallthrough
	case client.QuboleStatusError:
		fallthrough
	case client.QuboleStatusRunning:
		fallthrough
	}
}