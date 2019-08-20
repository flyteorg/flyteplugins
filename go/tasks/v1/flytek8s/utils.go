package flytek8s

import (
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	v1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
)

func ToK8sEnvVar(env []*core.KeyValuePair) []v1.EnvVar {
	envVars := make([]v1.EnvVar, 0, len(env))
	for _, kv := range env {
		envVars = append(envVars, v1.EnvVar{Name: kv.Key, Value: kv.Value})
	}
	return envVars
}

func IsK8sObjectNotExists(err error) bool {
	return k8serrors.IsNotFound(err) || k8serrors.IsGone(err) || k8serrors.IsResourceExpired(err)
}
