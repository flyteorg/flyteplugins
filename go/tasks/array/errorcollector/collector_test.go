/*
 * Copyright (c) 2018 Lyft. All rights reserved.
 */

package errorcollector

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrorMessageCollector_Collect(t *testing.T) {
	c := NewErrorMessageCollector()
	dupeMsg := "duplicate message"
	Collect(0, dupeMsg)
	Collect(1, dupeMsg)
	Collect(2, "unique message")
	Collect(3, dupeMsg)

	assert.Len(t, messages, 2)
	assert.Len(t, *messages[dupeMsg], 2)
}

func TestErrorMessageCollector_Summary(t *testing.T) {
	c := NewErrorMessageCollector()
	dupeMsg := "duplicate message"
	Collect(0, dupeMsg)
	Collect(1, dupeMsg)
	Collect(2, "unique message")
	Collect(3, dupeMsg)

	assert.Equal(t, "[0-1][3]: duplicate message\n[2]: unique message\n", Summary(1000))
	assert.Equal(t, "[0-1][3]: duplicate message\n... and many more.", Summary(30))
}
