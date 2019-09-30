package awsbatch

import (
	"testing"

	"github.com/lyft/flytestdlib/bitarray"
)

func Test_calculateOriginalIndex(t *testing.T) {
	inputArr := bitarray.NewBitSet(7)
	inputArr.Set(3)
	inputArr.Set(4)
	inputArr.Set(5)

	tests := []struct {
		name     string
		childIdx int
		want     int
	}{
		{"0", 0, 0},
		{"1", 1, 1},
		{"2", 2, 2},
		{"3", 3, 6},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := calculateOriginalIndex(tt.childIdx, inputArr); got != tt.want {
				t.Errorf("calculateOriginalIndex() = %v, want %v", got, tt.want)
			}
		})
	}
}
