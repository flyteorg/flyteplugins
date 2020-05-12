package raw_container

import (
	"context"
	"testing"
	"time"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"
	config2 "github.com/lyft/flytestdlib/config"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	pluginsCoreMock "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/flytek8s/config"
)

func TestFlyteCoPilotContainer(t *testing.T) {
	cfg := config.FlyteCoPilotConfig{
		NamePrefix:           "test-",
		Image:                "test",
		DefaultInputDataPath: "/in",
		DefaultOutputPath:    "/out",
		InputVolumeName:      "inp",
		OutputVolumeName:     "out",
		StartTimeout: config2.Duration{
			Duration: time.Second * 1,
		},
		CPU:    "1024m",
		Memory: "1024Mi",
	}

	t.Run("happy", func(t *testing.T) {
		c, err := FlyteCoPilotContainer("x", cfg, []string{"hello"})
		assert.NoError(t, err)
		assert.Equal(t, "test-x", c.Name)
		assert.Equal(t, "test", c.Image)
		assert.Equal(t, []string{"/bin/flyte-copilot", "--config", "/etc/flyte/config**/*"}, c.Command)
		assert.Equal(t, []string{"hello"}, c.Args)
		assert.Equal(t, 0, len(c.VolumeMounts))
		assert.Equal(t, "/", c.WorkingDir)
		assert.Equal(t, 2, len(c.Resources.Limits))
		assert.Equal(t, 2, len(c.Resources.Requests))
	})

	t.Run("happy-vols", func(t *testing.T) {
		c, err := FlyteCoPilotContainer("x", cfg, []string{"hello"}, v1.VolumeMount{Name: "X", MountPath: "/"})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(c.VolumeMounts))
	})

	t.Run("bad-res-cpu", func(t *testing.T) {
		old := cfg.CPU
		cfg.CPU = "x"
		_, err := FlyteCoPilotContainer("x", cfg, []string{"hello"}, v1.VolumeMount{Name: "X", MountPath: "/"})
		assert.Error(t, err)
		cfg.CPU = old
	})

	t.Run("bad-res-mem", func(t *testing.T) {
		old := cfg.Memory
		cfg.Memory = "x"
		_, err := FlyteCoPilotContainer("x", cfg, []string{"hello"}, v1.VolumeMount{Name: "X", MountPath: "/"})
		assert.Error(t, err)
		cfg.Memory = old
	})
}

func TestDownloadCommandArgs(t *testing.T) {
	_, err := DownloadCommandArgs("", "", "", plugins.CoPilot_YAML, nil)
	assert.Error(t, err)

	iFace := &core.VariableMap{
		Variables: map[string]*core.Variable{
			"x": {Type: &core.LiteralType{Type: &core.LiteralType_Simple{Simple: core.SimpleType_INTEGER}}},
			"y": {Type: &core.LiteralType{Type: &core.LiteralType_Simple{Simple: core.SimpleType_INTEGER}}},
		},
	}
	d, err := DownloadCommandArgs("s3://from", "s3://output-meta", "/to", plugins.CoPilot_JSON, iFace)
	assert.NoError(t, err)
	assert.Equal(t, []string{"download", "--from-remote", "s3://from", "--to-output-prefix", "s3://output-meta", "--to-local-dir", "/to", "--format", "json", "--input-interface", "CgkKAXgSBAoCCAEKCQoBeRIECgIIAQ=="}, d)
}

func TestSidecarCommandArgs(t *testing.T) {
	_, err := SidecarCommandArgs("", "", "", time.Second * 10, nil)
	assert.Error(t, err)

	iFace := &core.VariableMap{
		Variables: map[string]*core.Variable{
			"x": {Type: &core.LiteralType{Type: &core.LiteralType_Simple{Simple: core.SimpleType_INTEGER}}},
			"y": {Type: &core.LiteralType{Type: &core.LiteralType_Simple{Simple: core.SimpleType_INTEGER}}},
		},
	}
	d, err := SidecarCommandArgs("/from", "s3://output-meta", "s3://raw-output", time.Second * 10, iFace)
	assert.NoError(t, err)
	assert.Equal(t, []string{"upload", "--start-timeout", "10s", "--to-raw-output", "s3://raw-output", "--to-output-prefix", "s3://output-meta", "--from-local-dir", "/from", "--output-interface", "CgkKAXgSBAoCCAEKCQoBeRIECgIIAQ=="}, d)
}

func TestToK8sPodSpec(t *testing.T) {
	ctx := context.TODO()
	cfg := config.FlyteCoPilotConfig{
		NamePrefix:           "test-",
		Image:                "test",
		DefaultInputDataPath: "/in",
		DefaultOutputPath:    "/out",
		InputVolumeName:      "inp",
		OutputVolumeName:     "out",
		StartTimeout: config2.Duration{
			Duration: time.Second * 1,
		},
		CPU:    "1024m",
		Memory: "1024Mi",
	}

	iface := &core.TypedInterface{
		Inputs: &core.VariableMap{
			Variables: map[string]*core.Variable{
				"x": {Type: &core.LiteralType{Type: &core.LiteralType_Simple{Simple: core.SimpleType_INTEGER}}},
				"y": {Type: &core.LiteralType{Type: &core.LiteralType_Simple{Simple: core.SimpleType_INTEGER}}},
			},
		},
		Outputs: &core.VariableMap{
			Variables: map[string]*core.Variable{
				"o": {Type: &core.LiteralType{Type: &core.LiteralType_Simple{Simple: core.SimpleType_INTEGER}}},
			},
		},
	}

	taskMetadata := &pluginsCoreMock.TaskExecutionMetadata{}
	taskMetadata.OnGetNamespace().Return("test-namespace")
	taskMetadata.OnGetAnnotations().Return(map[string]string{"annotation-1": "val1"})
	taskMetadata.OnGetLabels().Return(map[string]string{"label-1": "val1"})
	taskMetadata.OnGetOwnerReference().Return(metav1.OwnerReference{
		Kind: "node",
		Name: "blah",
	})
	taskMetadata.OnGetK8sServiceAccount().Return("")
	taskMetadata.OnGetOwnerID().Return(types.NamespacedName{
		Namespace: "test-namespace",
		Name:      "test-owner-name",
	})
	taskMetadata.OnIsInterruptible().Return(false)

	tID := &pluginsCoreMock.TaskExecutionID{}
	tID.OnGetID().Return(core.TaskExecutionIdentifier{
		NodeExecutionId: &core.NodeExecutionIdentifier{
			ExecutionId: &core.WorkflowExecutionIdentifier{
				Name:    "my_name",
				Project: "my_project",
				Domain:  "my_domain",
			},
		},
	})
	tID.OnGetGeneratedName().Return("name")
	taskMetadata.OnGetTaskExecutionID().Return(tID)

	to := &pluginsCoreMock.TaskOverrides{}
	to.OnGetResources().Return(resources)
	taskMetadata.OnGetOverrides().Return(to)

	ToK8sPodSpec(ctx, cfg, )
}