package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestContains(t *testing.T) {

	assert.True(t, Contains([]string{"a", "b", "c"}, "b"))

	assert.False(t, Contains([]string{"a", "b", "c"}, "spark"))

	assert.False(t, Contains([]string{}, "spark"))

	assert.False(t, Contains(nil, "b"))
}

func TestCopyMap(t *testing.T) {
	assert.Nil(t, CopyMap(nil))
	m := map[string]string{
		"l": "v",
	}
	assert.Equal(t, m, CopyMap(m))
}

func TestMergeMaps(t *testing.T) {
	a := map[string]string{
		"1": "foo",
		"2": "bar",
	}
	b := map[string]string{
		"2": "baz",
		"3": "beef",
	}
	t.Run("empty base", func(t *testing.T) {
		merge := MergeMaps(nil, a)
		assert.EqualValues(t, merge, a)
	})
	t.Run("empty patch", func(t *testing.T) {
		merge := MergeMaps(a, nil)
		assert.EqualValues(t, merge, a)
	})
	t.Run("merge", func(t *testing.T) {
		merge := MergeMaps(a, b)
		assert.EqualValues(t, merge, map[string]string{
			"1": "foo",
			"2": "baz",
			"3": "beef",
		})
	})
}
