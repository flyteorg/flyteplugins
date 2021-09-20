package utils

import (
	"regexp"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

var dns1123InvalidRegex = regexp.MustCompile("[^-a-z0-9]")
var camelCaseRegex = regexp.MustCompile("([a-z0-9])([A-Z])")

func ConvertToDNS1123CompatibleString(name string) string {
	name = ConvertCamelCaseToKebabCase(name) // best effort to preserve readability for Java class name
	name = strings.ToLower(name)
	name = dns1123InvalidRegex.ReplaceAllString(name, "")
	name = strings.Trim(name, ".-")
	if len(name) > validation.DNS1123LabelMaxLength {
		name = name[:validation.DNS1123LabelMaxLength]
	}
	return name
}

func ConvertCamelCaseToKebabCase(name string) string {
	return strings.ToLower(camelCaseRegex.ReplaceAllString(name, "${1}-${2}"))
}
