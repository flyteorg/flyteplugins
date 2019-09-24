package awsbatch

import (
	"testing"

	"github.com/lyft/flyteplugins/go/tasks/array/bitarray"
)

func Test_calculateOriginalIndex(t *testing.T) {

	inputArr := bitarray.NewBitSet(8)
	inputArr.Set(1)
	inputArr.Set(2)
	inputArr.Set(3)
	inputArr.Set(6)

	tests := []struct {
		name     string
		childIdx int
		want     int
	}{
		{"0", 0, 0},
		{"1", 1, 4},
		{"0", 2, 5},
		{"0", 3, 7},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := calculateOriginalIndex(tt.childIdx, inputArr); got != tt.want {
				t.Errorf("calculateOriginalIndex() = %v, want %v", got, tt.want)
			}
		})
	}
}
