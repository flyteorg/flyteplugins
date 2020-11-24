package utils

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/golang/protobuf/ptypes"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/logger"
	"github.com/pkg/errors"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
)

var inputFileRegex = regexp.MustCompile(`(?i){{\s*[\.$]Input\s*}}`)
var inputPrefixRegex = regexp.MustCompile(`(?i){{\s*[\.$]InputPrefix\s*}}`)
var outputRegex = regexp.MustCompile(`(?i){{\s*[\.$]OutputPrefix\s*}}`)
var inputVarRegex = regexp.MustCompile(`(?i){{\s*[\.$]Inputs\.(?P<input_name>[^}\s]+)\s*}}`)
var rawOutputDataPrefixRegex = regexp.MustCompile(`(?i){{\s*[\.$]RawOutputDataPrefix\s*}}`)
var perRetryUniqueKey = regexp.MustCompile(`(?i){{\s*[\.$]PerRetryUniqueKey\s*}}`)

func transformVarNameToStringVal(ctx context.Context, varName string, inputs *core.LiteralMap) (string, error) {
	inputVal, exists := inputs.Literals[varName]
	if !exists {
		return "", fmt.Errorf("requested input is not found [%s]", varName)
	}

	v, err := serializeLiteral(ctx, inputVal)
	if err != nil {
		return "", errors.Wrapf(err, "failed to bind a value to inputName [%s]", varName)
	}
	return v, nil
}

func ReplaceTemplateCommandArgs(ctx context.Context, perRetryKey string, commandTemplate string,
	in io.InputReader, out io.OutputFilePaths) (string, error) {

	val := inputFileRegex.ReplaceAllString(commandTemplate, in.GetInputPath().String())
	val = outputRegex.ReplaceAllString(val, out.GetOutputPrefixPath().String())
	val = inputPrefixRegex.ReplaceAllString(val, in.GetInputPrefixPath().String())
	val = rawOutputDataPrefixRegex.ReplaceAllString(val, out.GetRawOutputPrefix().String())
	val = perRetryUniqueKey.ReplaceAllString(val, perRetryKey)

	inputs, err := in.Get(ctx)
	if err != nil {
		return val, errors.Wrapf(err, "unable to read inputs")
	}
	if inputs == nil || inputs.Literals == nil {
		return val, nil
	}

	var errs ErrorCollection
	val = inputVarRegex.ReplaceAllStringFunc(val, func(s string) string {
		matches := inputVarRegex.FindAllStringSubmatch(s, 1)
		varName := matches[0][1]
		replaced, err := transformVarNameToStringVal(ctx, varName, inputs)
		if err != nil {
			errs.Errors = append(errs.Errors, errors.Wrapf(err, "input template [%s]", s))
			return ""
		}
		return replaced
	})

	if len(errs.Errors) > 0 {
		return "", errs
	}

	return val, nil
}

func serializePrimitive(p *core.Primitive) (string, error) {
	switch o := p.Value.(type) {
	case *core.Primitive_Integer:
		return fmt.Sprintf("%v", o.Integer), nil
	case *core.Primitive_Boolean:
		return fmt.Sprintf("%v", o.Boolean), nil
	case *core.Primitive_Datetime:
		return ptypes.TimestampString(o.Datetime), nil
	case *core.Primitive_Duration:
		return o.Duration.String(), nil
	case *core.Primitive_FloatValue:
		return fmt.Sprintf("%v", o.FloatValue), nil
	case *core.Primitive_StringValue:
		return o.StringValue, nil
	default:
		return "", fmt.Errorf("received an unexpected primitive type [%v]", reflect.TypeOf(p.Value))
	}
}

func serializeLiteralScalar(l *core.Scalar) (string, error) {
	switch o := l.Value.(type) {
	case *core.Scalar_Primitive:
		return serializePrimitive(o.Primitive)
	case *core.Scalar_Blob:
		return o.Blob.Uri, nil
	case *core.Scalar_Schema:
		return o.Schema.Uri, nil
	default:
		return "", fmt.Errorf("received an unexpected scalar type [%v]", reflect.TypeOf(l.Value))
	}
}

func serializeLiteral(ctx context.Context, l *core.Literal) (string, error) {
	switch o := l.Value.(type) {
	case *core.Literal_Collection:
		res := make([]string, 0, len(o.Collection.Literals))
		for _, sub := range o.Collection.Literals {
			s, err := serializeLiteral(ctx, sub)
			if err != nil {
				return "", err
			}

			res = append(res, s)
		}

		return fmt.Sprintf("[%v]", strings.Join(res, ",")), nil
	case *core.Literal_Scalar:
		return serializeLiteralScalar(o.Scalar)
	default:
		logger.Debugf(ctx, "received unexpected primitive type")
		return "", fmt.Errorf("received an unexpected primitive type [%v]", reflect.TypeOf(l.Value))
	}
}
