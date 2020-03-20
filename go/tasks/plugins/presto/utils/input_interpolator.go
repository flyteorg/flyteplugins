package presto

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/golang/protobuf/ptypes"
	pb "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/logger"
)

// Matches any pair of open/close mustaches that contains a variable inside (e.g. `{{ abc }}`)
var inputMustacheRegex = regexp.MustCompile(`{{\s*[^\s]+\s*}}`)

// Matches the variable content inside a pair of double mustaches except for spaces and the mustaches themselves
var inputVarNameRegex = regexp.MustCompile(`([^{{\s}}]+)`)

// This will interpolate any input variable mustaches that were assigned to the statement, routingGroup, catalog, and
// schema with the values of the input variables provided for the user.
//
// For example, if we have a Presto task like:
//
// presto_task = SdkPrestoTask(
//    query="SELECT * FROM hive.city.fact_airport_sessions WHERE ds = '{{ds}}' LIMIT 10",
//    output_schema=schema.schema_type,
//    routing_group="{{ routing_group }}",
//    catalog="hive",
//    schema="city",
//    task_inputs=inputs(ds=Types.String, routing_group=Types.String),
// )
//
// Then  this function takes care of replace '{{ds}}' and '{{ routing_group }}' with the appropriate input values.
func InterpolateInputs(
	ctx context.Context,
	inputs pb.LiteralMap,
	routingGroup string,
	catalog string,
	schema string,
	statement string) (string, string, string, string, error) {

	inputsAsStrings, err := literalMapToStringMap(ctx, inputs)
	if err != nil {
		return "", "", "", "", err
	}

	// Remove implicit inputs from the rest of the inputs used for interpolation
	delete(inputsAsStrings, "__implicit_routing_group")
	delete(inputsAsStrings, "__implicit_catalog")
	delete(inputsAsStrings, "__implicit_schema")

	statement = interpolate(inputsAsStrings, statement)
	routingGroup = interpolate(inputsAsStrings, routingGroup)
	catalog = interpolate(inputsAsStrings, catalog)
	schema = interpolate(inputsAsStrings, schema)

	return routingGroup, catalog, schema, statement, nil
}

func interpolate(inputs map[string]string, s string) string {
	mustacheInputs := inputMustacheRegex.FindAllString(s, -1)
	for _, inputMustache := range mustacheInputs {
		inputVarName := inputVarNameRegex.FindString(inputMustache)
		inputVarReplacement := inputs[inputVarName]
		s = strings.Replace(s, inputMustache, inputVarReplacement, -1)
	}

	return s
}

func literalMapToStringMap(ctx context.Context, literalMap pb.LiteralMap) (map[string]string, error) {
	stringMap := map[string]string{}

	for k, v := range literalMap.Literals {
		serializedLiteral, err := serializeLiteral(ctx, v)
		if err != nil {
			return nil, err
		}
		stringMap[k] = serializedLiteral
	}

	return stringMap, nil
}

func serializeLiteral(ctx context.Context, l *pb.Literal) (string, error) {
	switch o := l.Value.(type) {
	case *pb.Literal_Collection:
		res := make([]string, 0, len(o.Collection.Literals))
		for _, sub := range o.Collection.Literals {
			s, err := serializeLiteral(ctx, sub)
			if err != nil {
				return "", err
			}

			res = append(res, s)
		}

		return fmt.Sprintf("[%v]", strings.Join(res, ",")), nil
	case *pb.Literal_Scalar:
		return serializeLiteralScalar(o.Scalar)
	default:
		logger.Debugf(ctx, "received unexpected primitive type")
		return "", fmt.Errorf("received an unexpected primitive type [%v]", reflect.TypeOf(l.Value))
	}
}

func serializeLiteralScalar(l *pb.Scalar) (string, error) {
	switch o := l.Value.(type) {
	case *pb.Scalar_Primitive:
		return serializePrimitive(o.Primitive)
	case *pb.Scalar_Blob:
		return o.Blob.Uri, nil
	default:
		return "", fmt.Errorf("received an unexpected scalar type [%v]", reflect.TypeOf(l.Value))
	}
}

func serializePrimitive(p *pb.Primitive) (string, error) {
	switch o := p.Value.(type) {
	case *pb.Primitive_Integer:
		return fmt.Sprintf("%v", o.Integer), nil
	case *pb.Primitive_Boolean:
		return fmt.Sprintf("%v", o.Boolean), nil
	case *pb.Primitive_Datetime:
		return ptypes.TimestampString(o.Datetime), nil
	case *pb.Primitive_Duration:
		return o.Duration.String(), nil
	case *pb.Primitive_FloatValue:
		return fmt.Sprintf("%v", o.FloatValue), nil
	case *pb.Primitive_StringValue:
		return o.StringValue, nil
	default:
		return "", fmt.Errorf("received an unexpected primitive type [%v]", reflect.TypeOf(p.Value))
	}
}
