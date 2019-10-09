package awsbatch

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestContainerImageRepository(t *testing.T) {
	testCases := [][]string{
		{"myrepo/test", "test"},
		{"ubuntu", "ubuntu"},
		{"registry/test:1.1", "test"},
		{"ubuntu:1.1", "ubuntu"},
		{"registry/ubuntu:1.1", "ubuntu"},
		{"library.iad.aws.core.av.lyft.net:5301/tool/grand_slam2/grand_slam:0.0.2dev_raz", "grand_slam"},
	}

	for _, testCase := range testCases {
		t.Run(testCase[1], func(t *testing.T) {
			actual := containerImageRepository(testCase[0])
			assert.Equal(t, testCase[1], actual)
		})
	}
}
