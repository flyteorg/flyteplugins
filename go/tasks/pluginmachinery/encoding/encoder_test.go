package encoding

import (
	"hash"
	"hash/fnv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFixedLengthUniqueID(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		maxLength   int
		output      string
		expectError bool
	}{
		{"smallerThanMax", "x", 5, "x", false},
		{"veryLowLimit", "xx", 1, "fbdyvab4vrxwm1", true},
		{"highLimit", "xxxxxx", 5, "fsbykgqw4gv441", true},
		{"higherLimit", "xxxxx", 10, "xxxxx", false},
		{"largeID", "xxxxxxxxxxxxxxxxxxxxxxxx", 20, "fyuigwrqamd3yk", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			i, err := FixedLengthUniqueID(test.input, test.maxLength)
			if test.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, test.output, i)
		})
	}
}

func TestFixedLengthUniqueIDForParts(t *testing.T) {
	tests := []struct {
		name        string
		parts       []string
		maxLength   int
		output      string
		expectError bool
	}{
		{"smallerThanMax", []string{"x", "y", "z"}, 10, "x-y-z", false},
		{"veryLowLimit", []string{"x", "y"}, 1, "fx2llkgl6golek", true},
		{"fittingID", []string{"x"}, 2, "x", false},
		{"highLimit", []string{"x", "y", "z"}, 4, "fvzirkr1kofp1m", true},
		{"largeID", []string{"x", "y", "z", "m", "n", "y", "z", "m", "n", "y", "z", "m", "n"}, 15, "fwp4bky2kucex5", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			i, err := FixedLengthUniqueIDForParts(test.maxLength, test.parts)
			if test.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, test.output, i)
		})
	}
}

func benchmarkKB(b *testing.B, h hash.Hash) {
	b.SetBytes(1024)
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i)
	}

	in := make([]byte, 0, h.Size())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Reset()
		h.Write(data)
		h.Sum(in)
	}
}

// Documented Results:
// goos: darwin
// goarch: amd64
// pkg: github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/encoding
// cpu: Intel(R) Core(TM) i9-9980HK CPU @ 2.40GHz
// BenchmarkFixedLengthUniqueID
// BenchmarkFixedLengthUniqueID/New32a
// BenchmarkFixedLengthUniqueID/New32a-16         	 1000000	      1088 ns/op	 941.25 MB/s
// BenchmarkFixedLengthUniqueID/New64a
// BenchmarkFixedLengthUniqueID/New64a-16         	 1239402	       951.3 ns/op	1076.39 MB/s
func BenchmarkFixedLengthUniqueID(b *testing.B) {
	b.Run("New32a", func(b *testing.B) {
		benchmarkKB(b, fnv.New32a())
	})

	b.Run("New64a", func(b *testing.B) {
		benchmarkKB(b, fnv.New64a())
	})
}
