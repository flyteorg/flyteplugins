package array

import (
	idlCore "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewLiteralScalarOfInteger(t *testing.T) {
	x := NewLiteralScalarOfInteger(int64(58432))
	assert.Equal(t, int64(58432), x.Value.(*idlCore.Literal_Scalar).Scalar.Value.(*idlCore.Scalar_Primitive).
		Primitive.Value.(*idlCore.Primitive_Integer).Integer)
}
