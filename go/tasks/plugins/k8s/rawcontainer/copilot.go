package raw_container

import (
	"context"
	"encoding/base64"

	"github.com/golang/protobuf/proto"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/storage"
	"github.com/pkg/errors"
	"k8s.io/api/core/v1"

	core2 "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/flytek8s"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/flytek8s/config"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
)

type MetadataFormat = string

const (
	MetadataFormatProto MetadataFormat = "proto"
	MetadataFormatJSON                 = "json"
	MetadataFormatYAML                 = "yaml"
)

type CustomInfo struct {
	Format         MetadataFormat
	InputDataPath  string
	OutputDataPath string
}

var pTraceCapability = v1.Capability("SYS_PTRACE")

func FlyteCoPilotContainer(name string, cfg *config.FlyteCoPilotConfig, args []string, volumeMounts ...v1.VolumeMount) v1.Container {
	volumeMounts = append(volumeMounts, v1.VolumeMount{
		Name:      flyteDataConfigVolume,
		MountPath: flyteDataConfigPath,
	})
	return v1.Container{
		Name:                     cfg.NamePrefix + name,
		Image:                    cfg.Image,
		Command:                  []string{"/bin/flyte-copilot", "--config", "/etc/flyte/config**/*"},
		Args:                     args,
		WorkingDir:               "/",
		Resources:                v1.ResourceRequirements{},
		VolumeMounts:             volumeMounts,
		TerminationMessagePolicy: v1.TerminationMessageFallbackToLogsOnError,
		ImagePullPolicy:          v1.PullIfNotPresent,
	}
}

func UploadCommandArgs(fromLocalPath string, outputPrefix, rawOutputPath storage.DataReference, outputInterface *core.VariableMap) ([]string, error) {
	args := []string{
		"upload",
		"--start-timeout",
		"30s",
		"--to-sandbox",
		rawOutputPath.String(),
		"--to-output-prefix",
		outputPrefix.String(),
		"--from-local-dir",
		fromLocalPath,
	}
	if outputInterface != nil {
		b, err := proto.Marshal(outputInterface)
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal given output interface")
		}
		args = append(args, "--output-interface", base64.StdEncoding.EncodeToString(b))
	}
	return args, nil
}

func DownloadCommandArgs(fromInputsPath, outputPrefix storage.DataReference, toLocalPath string, format MetadataFormat, inputInterface *core.VariableMap) ([]string, error) {
	args := []string{
		"download",
		"--from-remote",
		fromInputsPath.String(),
		"--to-output-prefix",
		outputPrefix.String(),
		"--to-local-dir",
		toLocalPath,
		"--format",
		format,

	}
	if inputInterface != nil {
		b, err := proto.Marshal(inputInterface)
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal given input interface")
		}
		args = append(args, "--input-interface", base64.StdEncoding.EncodeToString(b))
	}
	return args, nil
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

	// TODO we want to increase resource requirements maybe?
	c.Resources = v1.ResourceRequirements{}

	shareProcessNamespaceEnabled := true
	coPilotPod := &v1.PodSpec{
		// We could specify Scheduler, Affinity, nodename etc
		RestartPolicy:         v1.RestartPolicyNever,
		Containers:            []v1.Container{},
		InitContainers:        []v1.Container{},
		Tolerations:           flytek8s.GetPodTolerations(taskExecutionMetadata.IsInterruptible(), c.Resources),
		ServiceAccountName:    taskExecutionMetadata.GetK8sServiceAccount(),
		ShareProcessNamespace: &shareProcessNamespaceEnabled,
		Volumes: []v1.Volume{
			{
				// TODO Remove the data configuration requirements
				Name: flyteDataConfigVolume,
				VolumeSource: v1.VolumeSource{
					ConfigMap: &v1.ConfigMapVolumeSource{
						LocalObjectReference: v1.LocalObjectReference{
							Name: flyteDataConfigMap,
						},
					},
				},
			},
		},
	}

	if task.Interface != nil {
		// TODO think about MountPropagationMode. Maybe we want to use that for acceleration in the future
		// TODO CustomInfo to be added
		info := CustomInfo{Format: MetadataFormatJSON}

		if task.Interface.Inputs != nil {
			inPath := cfg.DefaultInputDataPath
			if info.InputDataPath != "" {
				inPath = info.InputDataPath
			}
			inputsVolumeMount := v1.VolumeMount{
				Name:      cfg.InputVolumeName,
				MountPath: inPath,
			}
			// Add the inputs volume to the container
			c.VolumeMounts = append(c.VolumeMounts, inputsVolumeMount)

			// Lets add the InputsVolume
			coPilotPod.Volumes = append(coPilotPod.Volumes, v1.Volume{
				Name: cfg.InputVolumeName,
				VolumeSource: v1.VolumeSource{
					EmptyDir: &v1.EmptyDirVolumeSource{},
				},
			})

			// Lets add the Inputs init container
			args, err := DownloadCommandArgs(inputs.GetInputPath(), outputPaths.GetOutputPrefixPath(), inPath, info.Format, task.Interface.Inputs)
			if err != nil {
				return nil, err
			}
			downloader := FlyteCoPilotContainer("downloader", cfg, args, inputsVolumeMount)
			coPilotPod.InitContainers = append(coPilotPod.InitContainers, downloader)
		}

		if task.Interface.Outputs != nil {
			outPath := cfg.DefaultOutputPath
			if info.OutputDataPath != "" {
				outPath = info.OutputDataPath
			}
			outputsVolumeMount := v1.VolumeMount{
				Name:      cfg.OutputVolumeName,
				MountPath: outPath,
			}
			// Add the outputs Volume to the container
			c.VolumeMounts = append(c.VolumeMounts, outputsVolumeMount)

			// Lets add the InputsVolume
			coPilotPod.Volumes = append(coPilotPod.Volumes, v1.Volume{
				Name: cfg.OutputVolumeName,
				VolumeSource: v1.VolumeSource{
					EmptyDir: &v1.EmptyDirVolumeSource{},
				},
			})

			// Lets add the Inputs init container
			args, err := UploadCommandArgs(outPath, outputPaths.GetOutputPrefixPath(), outputPaths.GetRawOutputPrefix(), task.Interface.Outputs)
			if err != nil {
				return nil, err
			}
			downloader := FlyteCoPilotContainer("sidecar", cfg, args, outputsVolumeMount)
			coPilotPod.Containers = append(coPilotPod.Containers, downloader)
		}
	}

	coPilotPod.Containers = append(coPilotPod.Containers, c)

	return coPilotPod, nil
}
