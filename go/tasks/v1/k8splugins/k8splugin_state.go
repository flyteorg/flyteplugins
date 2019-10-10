package k8splugins

import (
	"fmt"
)

const statusKey = "ObjectStatus"

// This status internal state of the object not read/updated by upstream components (eg. Node manager)
type K8sObjectStatus int

const (
	K8sObjectUnknown K8sObjectStatus = iota
	K8sObjectExists
	K8sObjectDeleted
)

func (q K8sObjectStatus) String() string {
	switch q {
	case K8sObjectUnknown:
		return "NotStarted"
	case K8sObjectExists:
		return "Running"
	case K8sObjectDeleted:
		return "Deleted"
	}
	return "IllegalK8sObjectStatus"
}

func RetrieveK8sObjectStatus(customState map[string]interface{}) (K8sObjectStatus, error) {
	if customState == nil {
		return K8sObjectUnknown, nil
	}

	for k, v := range customState {
		if k == statusKey {
			status, ok := v.(K8sObjectStatus)
			if !ok {
				return K8sObjectUnknown, fmt.Errorf("invalid k8s status %v", v)
			}
			return status, nil
		}
	}

	return K8sObjectUnknown, nil
}

func StoreK8sObjectStatus(status K8sObjectStatus) map[string]interface{} {
	customState := make(map[string]interface{})
	customState[statusKey] = status
	return customState
}
