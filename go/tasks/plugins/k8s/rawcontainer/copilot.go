package raw_container

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/storage"
	"github.com/pkg/errors"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	core2 "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/flytek8s"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/flytek8s/config"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/utils"
)

var pTraceCapability = v1.Capability("SYS_PTRACE")

func FlyteCoPilotContainer(name string, cfg config.FlyteCoPilotConfig, args []string, volumeMounts ...v1.VolumeMount) (v1.Container, error) {
	cpu, err := resource.ParseQuantity(cfg.CPU)
	if err != nil {
		return v1.Container{}, err
	}

	mem, err := resource.ParseQuantity(cfg.Memory)
	if err != nil {
		return v1.Container{}, err
	}

	return v1.Container{
		Name:       cfg.NamePrefix + name,
		Image:      cfg.Image,
		Command:    []string{"/bin/flyte-copilot", "--config", "/etc/flyte/config**/*"},
		Args:       args,
		WorkingDir: "/",
		Resources: v1.ResourceRequirements{
			Limits: v1.ResourceList{
				v1.ResourceCPU:    cpu,
				v1.ResourceMemory: mem,
			},
			Requests: v1.ResourceList{
				v1.ResourceCPU:    cpu,
				v1.ResourceMemory: mem,
			},
		},
		VolumeMounts:             volumeMounts,
		TerminationMessagePolicy: v1.TerminationMessageFallbackToLogsOnError,
		ImagePullPolicy:          v1.PullIfNotPresent,
	}, nil
}

func SidecarCommandArgs(fromLocalPath string, outputPrefix, rawOutputPath storage.DataReference, startTimeout time.Duration, outputInterface *core.VariableMap) ([]string, error) {
	if outputInterface == nil {
		return nil, fmt.Errorf("output Interface is required for CoPilot Sidecar")
	}
	b, err := proto.Marshal(outputInterface)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal given output interface")
	}
	return []string{
		"upload",
		"--start-timeout",
		startTimeout.String(),
		"--to-raw-output",
		rawOutputPath.String(),
		"--to-output-prefix",
		outputPrefix.String(),
		"--from-local-dir",
		fromLocalPath,
		"--output-interface",
		base64.StdEncoding.EncodeToString(b),
	}, nil
}

func DownloadCommandArgs(fromInputsPath, outputPrefix storage.DataReference, toLocalPath string, format plugins.CoPilot_MetadataFormat, inputInterface *core.VariableMap) ([]string, error) {
	if inputInterface == nil {
		return nil, fmt.Errorf("input Interface is required for CoPilot Downloader")
	}
	b, err := proto.Marshal(inputInterface)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal given input interface")
	}
	return []string{
		"download",
		"--from-remote",
		fromInputsPath.String(),
		"--to-output-prefix",
		outputPrefix.String(),
		"--to-local-dir",
		toLocalPath,
		"--format",
		strings.ToLower(format.String()),
		"--input-interface",
		base64.StdEncoding.EncodeToString(b),
	}, nil
}

func DataVolume(name string, size *resource.Quantity) v1.Volume {
	return v1.Volume{
		Name: name,
		VolumeSource: v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{
				Medium:    v1.StorageMediumDefault,
				SizeLimit: size,
			},
		},
	}
}

func CalculateStorageSize(requirements *v1.ResourceRequirements) *resource.Quantity {
	if requirements == nil {
		return nil
	}
	s, ok := requirements.Limits[v1.ResourceStorage]
	if ok {
		return &s
	}
	s, ok = requirements.Requests[v1.ResourceStorage]
	if ok {
		return &s
	}
	return nil
}

func ToK8sPodSpec(ctx context.Context, cfg config.FlyteCoPilotConfig, taskExecutionMetadata core2.TaskExecutionMetadata, taskReader core2.TaskReader,
	inputs io.InputReader, outputPaths io.OutputFilePaths) (*v1.PodSpec, error) {
	task, err := taskReader.Read(ctx)
	if err != nil {
		logger.Warnf(ctx, "failed to read task information when trying to construct Pod, err: %s", err.Error())
		return nil, err
	}

	c, err := flytek8s.ToK8sContainer(ctx, taskExecutionMetadata, task.GetContainer(), inputs, outputPaths)
	if err != nil {
		return nil, err
	}

	if c.SecurityContext == nil {
		c.SecurityContext = &v1.SecurityContext{}
	}
	if c.SecurityContext.Capabilities == nil {
		c.SecurityContext.Capabilities = &v1.Capabilities{}
	}
	c.SecurityContext.Capabilities.Add = append(c.SecurityContext.Capabilities.Add, pTraceCapability)

	shareProcessNamespaceEnabled := true
	coPilotPod := &v1.PodSpec{
		// We could specify Scheduler, Affinity, nodename etc
		RestartPolicy:         v1.RestartPolicyNever,
		Containers:            []v1.Container{},
		InitContainers:        []v1.Container{},
		Tolerations:           flytek8s.GetPodTolerations(taskExecutionMetadata.IsInterruptible(), c.Resources),
		ServiceAccountName:    taskExecutionMetadata.GetK8sServiceAccount(),
		ShareProcessNamespace: &shareProcessNamespaceEnabled,
		Volumes:               []v1.Volume{},
	}

	if task.Interface != nil {
		// TODO think about MountPropagationMode. Maybe we want to use that for acceleration in the future
		info := &plugins.CoPilot{}
		if task.Custom != nil {
			if err := utils.UnmarshalStruct(task.Custom, info); err != nil {
				return nil, errors.Wrap(err, "error decoding custom information")
			}
		}

		if task.Interface.Inputs != nil || task.Interface.Outputs != nil {
			// This is temporary. we have to mount the flyte data configuration into the pod
			// TODO Remove the data configuration requirements
			coPilotPod.Volumes = append(coPilotPod.Volumes, v1.Volume{
				Name: flyteDataConfigVolume,
				VolumeSource: v1.VolumeSource{
					ConfigMap: &v1.ConfigMapVolumeSource{
						LocalObjectReference: v1.LocalObjectReference{
							Name: flyteDataConfigMap,
						},
					},
				},
			})
		}
		cfgVMount := v1.VolumeMount{
			Name:      flyteDataConfigVolume,
			MountPath: flyteDataConfigPath,
		}

		if task.Interface.Inputs != nil {
			inPath := cfg.DefaultInputDataPath
			if info.GetInputPath() != "" {
				inPath = info.GetInputPath()
			}
			inputsVolumeMount := v1.VolumeMount{
				Name:      cfg.InputVolumeName,
				MountPath: inPath,
			}
			// Add the inputs volume to the container
			c.VolumeMounts = append(c.VolumeMounts, inputsVolumeMount)

			task.GetContainer().GetResources().GetLimits()
			// Lets add the InputsVolume
			// TODO we should calculate input volume size based on the size of the inputs which is known ahead of time. We should store that as part of the metadata
			coPilotPod.Volumes = append(coPilotPod.Volumes, DataVolume(cfg.InputVolumeName, CalculateStorageSize(taskExecutionMetadata.GetOverrides().GetResources())))

			// Lets add the Inputs init container
			args, err := DownloadCommandArgs(inputs.GetInputPath(), outputPaths.GetOutputPrefixPath(), inPath, info.Format, task.Interface.Inputs)
			if err != nil {
				return nil, err
			}
			downloader, err := FlyteCoPilotContainer("downloader", cfg, args, inputsVolumeMount, cfgVMount)
			if err != nil {
				return nil, err
			}
			coPilotPod.InitContainers = append(coPilotPod.InitContainers, downloader)
		}

		if task.Interface.Outputs != nil {
			outPath := cfg.DefaultOutputPath
			if info.GetOutputPath() != "" {
				outPath = info.GetOutputPath()
			}
			outputsVolumeMount := v1.VolumeMount{
				Name:      cfg.OutputVolumeName,
				MountPath: outPath,
			}
			// Add the outputs Volume to the container
			c.VolumeMounts = append(c.VolumeMounts, outputsVolumeMount)

			// Lets add the InputsVolume
			coPilotPod.Volumes = append(coPilotPod.Volumes, DataVolume(cfg.OutputVolumeName, CalculateStorageSize(taskExecutionMetadata.GetOverrides().GetResources())))

			// Lets add the Inputs init container
			args, err := SidecarCommandArgs(outPath, outputPaths.GetOutputPrefixPath(), outputPaths.GetRawOutputPrefix(), cfg.StartTimeout.Duration, task.Interface.Outputs)
			if err != nil {
				return nil, err
			}
			sidecar, err := FlyteCoPilotContainer("sidecar", cfg, args, outputsVolumeMount, cfgVMount)
			if err != nil {
				return nil, err
			}
			coPilotPod.Containers = append(coPilotPod.Containers, sidecar)
		}
	}

	coPilotPod.Containers = append(coPilotPod.Containers, *c)

	return coPilotPod, nil
}
