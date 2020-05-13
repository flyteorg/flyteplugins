package raw_container

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"
	config2 "github.com/lyft/flytestdlib/config"
	"github.com/lyft/flytestdlib/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	pluginsCoreMock "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/flytek8s/config"
	pluginsIOMock "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io/mocks"
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
	assert.ElementsMatch(t, []string{"download", "--from-remote", "s3://from", "--to-output-prefix", "s3://output-meta", "--to-local-dir", "/to", "--format", "json", "--input-interface", "CgkKAXgSBAoCCAEKCQoBeRIECgIIAQ=="}, d)
}

func TestSidecarCommandArgs(t *testing.T) {
	_, err := SidecarCommandArgs("", "", "", time.Second*10, nil)
	assert.Error(t, err)

	iFace := &core.VariableMap{
		Variables: map[string]*core.Variable{
			"x": {Type: &core.LiteralType{Type: &core.LiteralType_Simple{Simple: core.SimpleType_INTEGER}}},
			"y": {Type: &core.LiteralType{Type: &core.LiteralType_Simple{Simple: core.SimpleType_INTEGER}}},
		},
	}
	d, err := SidecarCommandArgs("/from", "s3://output-meta", "s3://raw-output", time.Second*10, iFace)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"upload", "--start-timeout", "10s", "--to-raw-output", "s3://raw-output", "--to-output-prefix", "s3://output-meta", "--from-local-dir", "/from", "--output-interface", "CgkKAXgSBAoCCAEKCQoBeRIECgIIAQ=="}, d)
}

func TestDataVolume(t *testing.T) {
	v := DataVolume("x", nil)
	assert.Equal(t, "x", v.Name)
	assert.NotNil(t, v.EmptyDir)
	assert.Nil(t, v.EmptyDir.SizeLimit)
	assert.Equal(t, v1.StorageMediumHugePages, v.EmptyDir.Medium)

	q := resource.MustParse("1024Mi")
	v = DataVolume("x", &q)
	assert.NotNil(t, v.EmptyDir.SizeLimit)
	assert.Equal(t, q, *v.EmptyDir.SizeLimit)
}

func assertPodHasSNPS(t *testing.T, pod *v1.PodSpec) {
	assert.NotNil(t, pod.ShareProcessNamespace)
	assert.True(t, *pod.ShareProcessNamespace)

	found := false
	for _, c := range pod.Containers {
		if c.Name == "test" {
			found = true
			assert.NotNil(t, c.SecurityContext)
			assert.NotNil(t, c.SecurityContext.Capabilities)
			assert.NotNil(t, c.SecurityContext.Capabilities.Add)
			capFound := false
			for _, cap := range c.SecurityContext.Capabilities.Add {
				if cap == pTraceCapability {
					capFound = true
				}
			}
			assert.True(t, capFound, "ptrace not found?")
		}
	}
	assert.False(t, found, "user container absent?")
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
	to.OnGetResources().Return(resourceRequirements)
	taskMetadata.OnGetOverrides().Return(to)

	inputReader := &pluginsIOMock.InputReader{}
	inputs := "/base/inputs"
	inputReader.OnGetInputPrefixPath().Return(storage.DataReference(inputs))
	inputReader.OnGetInputPath().Return(storage.DataReference(inputs + "/inputs.pb"))
	inputReader.OnGetMatch(mock.Anything).Return(&core.LiteralMap{}, nil)

	opath := &pluginsIOMock.OutputFilePaths{}
	opath.OnGetRawOutputPrefix().Return("/raw")
	opath.OnGetOutputPrefixPath().Return("/output")

	t.Run("input/output", func(t *testing.T) {
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
		task := &core.TaskTemplate{
			Type:      "test",
			Interface: iface,
			Target: &core.TaskTemplate_Container{
				Container: &core.Container{
					Image:   "busybox",
					Command: []string{"/bin/sh", "-c"},
					Args:    []string{"val"},
				},
			},
		}
		taskReader := &pluginsCoreMock.TaskReader{}
		taskReader.OnReadMatch(mock.Anything).Return(task, nil)
		pod, err := ToK8sPodSpec(ctx, cfg, taskMetadata, taskReader, inputReader, opath)
		assert.NoError(t, err)
		assert.NotNil(t, pod)
		assert.Len(t, pod.Containers, 2)
		assert.Len(t, pod.InitContainers, 1)
		assert.Len(t, pod.Volumes, 3)
		expectedVols := sets.NewString(cfg.InputVolumeName, cfg.OutputVolumeName, flyteDataConfigVolume)
		var actualVols []string
		for _, v := range pod.Volumes {
			actualVols = append(actualVols, v.Name)
		}
		assert.True(t, expectedVols.HasAll(actualVols...))
		assertPodHasSNPS(t, pod)
	})

	t.Run("inputsOnly", func(t *testing.T) {
		iface := &core.TypedInterface{
			Inputs: &core.VariableMap{
				Variables: map[string]*core.Variable{
					"x": {Type: &core.LiteralType{Type: &core.LiteralType_Simple{Simple: core.SimpleType_INTEGER}}},
					"y": {Type: &core.LiteralType{Type: &core.LiteralType_Simple{Simple: core.SimpleType_INTEGER}}},
				},
			},
		}
		task := &core.TaskTemplate{
			Type:      "test",
			Interface: iface,
			Target: &core.TaskTemplate_Container{
				Container: &core.Container{
					Image:   "busybox",
					Command: []string{"/bin/sh", "-c"},
					Args:    []string{"val"},
				},
			},
		}
		taskReader := &pluginsCoreMock.TaskReader{}
		taskReader.OnReadMatch(mock.Anything).Return(task, nil)
		pod, err := ToK8sPodSpec(ctx, cfg, taskMetadata, taskReader, inputReader, opath)
		assert.NoError(t, err)
		assert.NotNil(t, pod)
		assert.Len(t, pod.Containers, 1)
		assert.Len(t, pod.InitContainers, 1)
		assert.Len(t, pod.Volumes, 2)
		expectedVols := sets.NewString(cfg.InputVolumeName, flyteDataConfigVolume)
		var actualVols []string
		for _, v := range pod.Volumes {
			actualVols = append(actualVols, v.Name)
		}
		assert.True(t, expectedVols.HasAll(actualVols...))
		assertPodHasSNPS(t, pod)
	})

	t.Run("outputsOnly", func(t *testing.T) {
		iface := &core.TypedInterface{
			Outputs: &core.VariableMap{
				Variables: map[string]*core.Variable{
					"o": {Type: &core.LiteralType{Type: &core.LiteralType_Simple{Simple: core.SimpleType_INTEGER}}},
				},
			},
		}
		task := &core.TaskTemplate{
			Type:      "test",
			Interface: iface,
			Target: &core.TaskTemplate_Container{
				Container: &core.Container{
					Image:   "busybox",
					Command: []string{"/bin/sh", "-c"},
					Args:    []string{"val"},
				},
			},
		}
		taskReader := &pluginsCoreMock.TaskReader{}
		taskReader.OnReadMatch(mock.Anything).Return(task, nil)
		pod, err := ToK8sPodSpec(ctx, cfg, taskMetadata, taskReader, inputReader, opath)
		assert.NoError(t, err)
		assert.NotNil(t, pod)
		assert.Len(t, pod.Containers, 2)
		assert.Len(t, pod.InitContainers, 0)
		assert.Len(t, pod.Volumes, 2)
		expectedVols := sets.NewString(cfg.OutputVolumeName, flyteDataConfigVolume)
		var actualVols []string
		for _, v := range pod.Volumes {
			actualVols = append(actualVols, v.Name)
		}
		assert.True(t, expectedVols.HasAll(actualVols...), "Found %s vols", actualVols)
		assertPodHasSNPS(t, pod)
	})

	t.Run("NoInputOutput", func(t *testing.T) {
		task := &core.TaskTemplate{
			Type:      "test",
			Interface: &core.TypedInterface{},
			Target: &core.TaskTemplate_Container{
				Container: &core.Container{
					Image:   "busybox",
					Command: []string{"/bin/sh", "-c"},
					Args:    []string{"val"},
				},
			},
		}
		taskReader := &pluginsCoreMock.TaskReader{}
		taskReader.OnReadMatch(mock.Anything).Return(task, nil)
		pod, err := ToK8sPodSpec(ctx, cfg, taskMetadata, taskReader, inputReader, opath)
		assert.NoError(t, err)
		assert.NotNil(t, pod)
		assert.Len(t, pod.Containers, 1)
		assert.Len(t, pod.InitContainers, 0)
		assert.Len(t, pod.Volumes, 0)
		expectedVols := sets.NewString(flyteDataConfigVolume)
		var actualVols []string
		for _, v := range pod.Volumes {
			actualVols = append(actualVols, v.Name)
		}
		assert.True(t, expectedVols.HasAll(actualVols...))
		assertPodHasSNPS(t, pod)
	})
}

func TestCalculateStorageSize(t *testing.T) {
	twoG := resource.MustParse("2048Mi")
	oneG := resource.MustParse("1024Mi")
	tests := []struct {
		name string
		args *v1.ResourceRequirements
		want *resource.Quantity
	}{
		{"nil", nil, nil},
		{"empty", &v1.ResourceRequirements{}, nil},
		{"limits", &v1.ResourceRequirements{
			Limits: v1.ResourceList{
				v1.ResourceStorage: twoG,
			}}, &twoG},
		{"requests", &v1.ResourceRequirements{
			Requests: v1.ResourceList{
				v1.ResourceStorage: oneG,
			}}, &oneG},

		{"max", &v1.ResourceRequirements{
			Limits: v1.ResourceList{
				v1.ResourceStorage: twoG,
			},
			Requests: v1.ResourceList{
				v1.ResourceStorage: oneG,
			}}, &twoG},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CalculateStorageSize(tt.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CalculateStorageSize() = %v, want %v", got, tt.want)
			}
		})
	}
}
