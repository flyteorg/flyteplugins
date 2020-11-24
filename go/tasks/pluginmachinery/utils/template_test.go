package utils

import (
	"context"
	"fmt"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/storage"
	"testing"

	"github.com/stretchr/testify/assert"
)

func BenchmarkRegexCommandArgs(b *testing.B) {
	for i := 0; i < b.N; i++ {
		inputFileRegex.MatchString("{{ .InputFile }}")
	}
}

type dummyInputReader struct {
	inputPrefix storage.DataReference
	inputPath   storage.DataReference
	inputs      *core.LiteralMap
	inputErr    bool
}

func (d dummyInputReader) GetInputPrefixPath() storage.DataReference {
	return d.inputPrefix
}

func (d dummyInputReader) GetInputPath() storage.DataReference {
	return d.inputPath
}

func (d dummyInputReader) Get(ctx context.Context) (*core.LiteralMap, error) {
	if d.inputErr {
		return nil, fmt.Errorf("expected input fetch error")
	}
	return d.inputs, nil
}

type dummyOutputPaths struct {
	outputPath          storage.DataReference
	rawOutputDataPrefix storage.DataReference
}

func (d dummyOutputPaths) GetRawOutputPrefix() storage.DataReference {
	return d.rawOutputDataPrefix
}

func (d dummyOutputPaths) GetOutputPrefixPath() storage.DataReference {
	return d.outputPath
}

func (d dummyOutputPaths) GetOutputPath() storage.DataReference {
	panic("should not be called")
}

func (d dummyOutputPaths) GetErrorPath() storage.DataReference {
	panic("should not be called")
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
