package resourcemanager

import (
	"testing"

	"github.com/magiconair/properties/assert"
)

func TestCreateKey(t *testing.T) {
	assert.Equal(t, "asdf:fdsa", createKey("asdf", "fdsa"))
}
