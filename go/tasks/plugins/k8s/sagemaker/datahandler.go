package sagemaker

import (
	"context"
	"fmt"
	"reflect"

	"github.com/lyft/flytestdlib/futures"

	"github.com/golang/protobuf/ptypes"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/storage"
	"github.com/pkg/errors"
)

type DataHandler struct {
}

type VarMap map[string]interface{}
type FutureMap map[string]futures.Future

// TODO add support for multipart blobs
func (d DataHandler) handleBlob(_ context.Context, blob *core.Blob) (interface{}, error) {
	ref := storage.DataReference(blob.Uri)
	_, _, _, err := ref.Split()
	if err != nil {
		return nil, errors.Wrapf(err, "Blob uri incorrectly formatted")
	}
	return blob.Uri, nil
}

func (d DataHandler) handleSchema(ctx context.Context, schema *core.Schema) (interface{}, error) {
	// TODO Handle schema type
	return d.handleBlob(ctx, &core.Blob{Uri: schema.Uri, Metadata: &core.BlobMetadata{Type: &core.BlobType{Dimensionality: core.BlobType_MULTIPART}}})
}

func (d DataHandler) handleBinary(_ context.Context, b *core.Binary) (interface{}, error) {
	// maybe we should return a map
	if b.GetValue() == nil {
		return nil, errors.Errorf("nil binary value")
	}
	v := b.GetValue()
	return v, nil
}

//func (d DataHandler) handleError(_ context.Context, b *core.Error, toFilePath string, writeToFile bool) (interface{}, error) {
//	// maybe we should return a map
//	if writeToFile {
//		return b.Message, ioutil.WriteFile(toFilePath, []byte(b.Message), os.ModePerm)
//	}
//	return b.Message, nil
//}

//func (d DataHandler) handleGeneric(_ context.Context, b *structpb.Struct) (interface{}, error) {
//	// TODO: Maybe we should return a json of the struct
//	return b, nil
//}

// Returns the primitive value in Golang native format and if the filePath is not empty, then writes the value to the given file path.
func (d DataHandler) handlePrimitive(primitive *core.Primitive) (interface{}, error) {

	var v interface{}
	var err error

	switch primitive.Value.(type) {
	case *core.Primitive_StringValue:
		v = primitive.GetStringValue()
	case *core.Primitive_Boolean:
		v = primitive.GetBoolean()
	case *core.Primitive_Integer:
		v = primitive.GetInteger()
	case *core.Primitive_FloatValue:
		v = primitive.GetFloatValue()
	case *core.Primitive_Datetime:
		v, err = ptypes.Timestamp(primitive.GetDatetime())
		if err != nil {
			return nil, err
		}
	case *core.Primitive_Duration:
		v, err := ptypes.Duration(primitive.GetDuration())
		if err != nil {
			return nil, err
		}
		return v, nil
	default:
		v = nil
	}
	return v, nil
}

func (d DataHandler) handleScalar(ctx context.Context, scalar *core.Scalar) (interface{}, error) {
	switch scalar.GetValue().(type) {
	case *core.Scalar_Primitive:
		p := scalar.GetPrimitive()
		i, err := d.handlePrimitive(p)
		return i, err
	case *core.Scalar_Blob:
		b := scalar.GetBlob()
		i, err := d.handleBlob(ctx, b)
		return i, err
	case *core.Scalar_Schema:
		b := scalar.GetSchema()
		i, err := d.handleSchema(ctx, b)
		return i, err
	case *core.Scalar_Binary:
		b := scalar.GetBinary()
		i, err := d.handleBinary(ctx, b)
		return i, err
	//case *core.Scalar_Error:
	//	b := scalar.GetError()
	//	i, err := d.handleError(ctx, b, toFilePath, writeToFile)
	//	return i, scalar, err
	//case *core.Scalar_Generic:
	//	b := scalar.GetGeneric()
	//	i, err := d.handleGeneric(ctx, b)
	//	return i, err
	case *core.Scalar_NoneType:
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported scalar type [%v]", reflect.TypeOf(scalar.GetValue()))
	}
}

func (d DataHandler) handleLiteral(ctx context.Context, lit *core.Literal) (interface{}, error) {
	switch lit.GetValue().(type) {
	case *core.Literal_Scalar:
		v, err := d.handleScalar(ctx, lit.GetScalar())
		if err != nil {
			return nil, err
		}
		return v, nil
	//case *core.Literal_Collection:
	//	v, c2, err := d.handleCollection(ctx, lit.GetCollection(), filePath, writeToFile)
	//	if err != nil {
	//		return nil, nil, err
	//	}
	//	return v, &core.Literal{Value: &core.Literal_Collection{
	//		Collection: c2,
	//	}}, nil
	//case *core.Literal_Map:
	//	v, m, err := d.RecursiveDownload(ctx, lit.GetMap(), filePath, writeToFile)
	//	if err != nil {
	//		return nil, nil, err
	//	}
	//	return v, &core.Literal{Value: &core.Literal_Map{
	//		Map: m,
	//	}}, nil
	default:
		return nil, fmt.Errorf("unsupported literal type [%v]", reflect.TypeOf(lit.GetValue()))
	}
}

//// Collection should be stored as a top level list file and may have accompanying files?
//func (d DataHandler) handleCollection(ctx context.Context, c *core.LiteralCollection, dir string, writePrimitiveToFile bool) ([]interface{}, *core.LiteralCollection, error) {
//	if c == nil || len(c.Literals) == 0 {
//		return []interface{}{}, c, nil
//	}
//	var collection []interface{}
//	litCollection := &core.LiteralCollection{}
//	for i, lit := range c.Literals {
//		filePath := path.Join(dir, strconv.Itoa(i))
//		v, lit, err := d.handleLiteral(ctx, lit, filePath, writePrimitiveToFile)
//		if err != nil {
//			return nil, nil, err
//		}
//		collection = append(collection, v)
//		litCollection.Literals = append(litCollection.Literals, lit)
//	}
//	return collection, litCollection, nil
//}
