package awsutils

import (
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
)

func GetRoleFromSecurityContext(securityContext core.SecurityContext) string {
	if securityContext.GetRunAs() != nil {
		return securityContext.GetRunAs().GetIamRole()
	}
	return ""
}

func GetRole(roleKey string, keyValueMap map[string]string) string {
	if len(roleKey) > 0 {
		return keyValueMap[roleKey]
	}

	return ""
}
