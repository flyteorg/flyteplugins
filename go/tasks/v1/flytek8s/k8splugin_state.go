package flytek8s

import (
	"fmt"
	"github.com/lyft/flyteplugins/go/tasks/v1/types"
	"bytes"
)

const statusKey = "os"
const terminalTaskPhaseKey = "tp"

// This status internal state of the object not read/updated by upstream components (eg. Node manager)
type K8sObjectStatus int

const (
	k8sObjectUnknown K8sObjectStatus = iota
	k8sObjectExists
	k8sObjectDeleted
)

func (q K8sObjectStatus) String() string {
	switch q {
	case k8sObjectUnknown:
		return "NotStarted"
	case k8sObjectExists:
		return "Running"
	case k8sObjectDeleted:
		return "Deleted"
	}
	return "IllegalK8sObjectStatus"
}

func retrieveK8sObjectStatus(customState map[string]interface{}) (K8sObjectStatus, types.TaskPhase, error) {
	if customState == nil {
		return k8sObjectUnknown, types.TaskPhaseUnknown, nil
	}

	status := int(k8sObjectUnknown)
	terminalTaskPhase := int(types.TaskPhaseUnknown)
	foundStatus := false
	foundPhase := false
	for k, v := range customState {
		if k == statusKey {
			status, foundStatus = v.(int)
		} else if k == terminalTaskPhaseKey {
			terminalTaskPhase, foundPhase = v.(int)
		}
	}

	if !(foundPhase && foundStatus) {
		return k8sObjectUnknown, types.TaskPhaseUnknown, fmt.Errorf("invalid custom state %v", mapToString(customState))
	}

	return K8sObjectStatus(status), types.TaskPhase(terminalTaskPhase), nil
}

func storeK8sObjectStatus(status K8sObjectStatus, phase types.TaskPhase) map[string]interface{} {
	customState := make(map[string]interface{})
	customState[statusKey] = int(status)
	customState[terminalTaskPhaseKey] = int(phase)
	return customState
}

func mapToString(m map[string]interface{}) string {
	b := new(bytes.Buffer)
	for key, value := range m {
		fmt.Fprintf(b, "%s=\"%v\"\n", key, value)
	}
	return b.String()
}