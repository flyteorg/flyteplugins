package utils

import (
	"context"
	"testing"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/stretchr/testify/assert"
)

func BenchmarkRegexCommandArgs(b *testing.B) {
	for i := 0; i < b.N; i++ {
		inputFileRegex.MatchString("{{ .InputFile }}")
	}
}

func TestInputRegexMatch(t *testing.T) {
	assert.True(t, inputFileRegex.MatchString("{{$input}}"))
	assert.True(t, inputFileRegex.MatchString("{{ $Input }}"))
	assert.True(t, inputFileRegex.MatchString("{{.input}}"))
	assert.True(t, inputFileRegex.MatchString("{{ .Input }}"))
	assert.True(t, inputFileRegex.MatchString("{{  .Input }}"))
	assert.True(t, inputFileRegex.MatchString("{{       .Input }}"))
	assert.True(t, inputFileRegex.MatchString("{{ .Input}}"))
	assert.True(t, inputFileRegex.MatchString("{{.Input }}"))
	assert.True(t, inputFileRegex.MatchString("--something={{.Input}}"))
	assert.False(t, inputFileRegex.MatchString("{{input}}"), "Missing $")
	assert.False(t, inputFileRegex.MatchString("{$input}}"), "Missing Brace")
}

func TestOutputRegexMatch(t *testing.T) {
	assert.True(t, outputRegex.MatchString("{{.OutputPrefix}}"))
	assert.True(t, outputRegex.MatchString("{{ .OutputPrefix }}"))
	assert.True(t, outputRegex.MatchString("{{  .OutputPrefix }}"))
	assert.True(t, outputRegex.MatchString("{{      .OutputPrefix }}"))
	assert.True(t, outputRegex.MatchString("{{ .OutputPrefix}}"))
	assert.True(t, outputRegex.MatchString("{{.OutputPrefix }}"))
	assert.True(t, outputRegex.MatchString("--something={{.OutputPrefix}}"))
	assert.False(t, outputRegex.MatchString("{{output}}"), "Missing $")
	assert.False(t, outputRegex.MatchString("{.OutputPrefix}}"), "Missing Brace")
}

func getBlobLiteral(uri string) *core.Literal {
	return &core.Literal{
		Value: &core.Literal_Scalar{
			Scalar: &core.Scalar{
				Value: &core.Scalar_Blob{
					Blob: &core.Blob{
						Metadata: nil,
						Uri:      uri,
					},
				},
			},
		},
	}
}

func getSchemaLiteral(uri string) *core.Literal {
	return &core.Literal{
		Value: &core.Literal_Scalar{
			Scalar: &core.Scalar{
				Value: &core.Scalar_Schema{
					Schema: &core.Schema{Type: nil, Uri: uri},
				},
			},
		},
	}
}

func TestSerializeLiteral(t *testing.T) {
	ctx := context.Background()

	t.Run("serialize blob", func(t *testing.T) {
		b := getBlobLiteral("asdf fdsa")
		interpolated, err := serializeLiteral(ctx, b)
		assert.NoError(t, err)
		assert.Equal(t, "asdf fdsa", interpolated)
	})

	t.Run("serialize blob", func(t *testing.T) {
		s := getSchemaLiteral("s3://some-bucket/fdsa/x.parquet")
		interpolated, err := serializeLiteral(ctx, s)
		assert.NoError(t, err)
		assert.Equal(t, "s3://some-bucket/fdsa/x.parquet", interpolated)
	})
}
