package awsutils

import "context"

func GetRole(_ context.Context, roleAnnotationKey string, annotations map[string]string) string {
	if len(roleAnnotationKey) > 0 {
		if role, found := annotations[roleAnnotationKey]; found {
			return role
		}
	}

	return ""
}
