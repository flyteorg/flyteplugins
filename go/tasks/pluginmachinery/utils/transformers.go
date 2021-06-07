package utils

func CopyMap(o map[string]string) (r map[string]string) {
	if o == nil {
		return nil
	}
	r = make(map[string]string, len(o))
	for k, v := range o {
		r[k] = v
	}
	return
}

func MergeMaps(base, patch map[string]string) (merged map[string]string) {
	if len(base) == 0 {
		return patch
	}
	if len(patch) == 0 {
		return base
	}
	merged = make(map[string]string)
	for k, v := range base {
		merged[k] = v
	}
	for k, v := range patch {
		merged[k] = v
	}
	return merged
}

func Contains(s []string, e string) bool {
	if s == nil {
		return false
	}

	for _, a := range s {
		if a == e {
			return true
		}
	}

	return false
}
