/*
 * Copyright (c) 2018 Lyft. All rights reserved.
 */

package errorcollector

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexRangeCollection_simplify(t *testing.T) {
	c := &indexRangeCollection{}
	Add(0)
	Add(1)
	Add(5)
	Add(10)
	Add(4)
	Add(3)
	Add(2)
	Add(8)

	simplify()

	arr := make([]indexRange, 0, len(*c))
	for _, item := range *c {
		arr = append(arr, *item)
	}

	assert.Equal(t, []indexRange{
		{start: 0, end: 5},
		{start: 8, end: 8},
		{start: 10, end: 10},
	}, arr)
}
