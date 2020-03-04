package ioutils

import (
	"testing"
)

func TestNewDataSharder(t *testing.T) {
	b := make([]rune, 0, 26)
	for i := 'a'; i <= 'z'; i++ {
		b = append(b, i)
	}
	print(b)
}
