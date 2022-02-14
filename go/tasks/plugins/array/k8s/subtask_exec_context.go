package k8s

import (
	"context"
	"fmt"
	"strconv"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/io"
	pluginsCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/utils"
	"github.com/flyteorg/flyteplugins/go/tasks/plugins/array"
	podPlugin "github.com/flyteorg/flyteplugins/go/tasks/plugins/k8s/pod"
)

// TaskExecutionContext provides a layer on top of core TaskExecutionContext with a custom TaskExecutionMetadata.
type SubTaskExecutionContext struct {
	pluginsCore.TaskExecutionContext
	arrayInputReader io.InputReader
	metadataOverride pluginsCore.TaskExecutionMetadata
	originalIndex    int
	retryAttempt     uint64
	subtaskReader    SubTaskReader
}

// InputReader overrides the TaskExecutionContext from base and returns a specialized context for Array
func (s SubTaskExecutionContext) InputReader() io.InputReader {
	return s.arrayInputReader
}

func (s SubTaskExecutionContext) TaskExecutionMetadata() pluginsCore.TaskExecutionMetadata {
	return s.metadataOverride
}

func (s SubTaskExecutionContext) TaskReader() pluginsCore.TaskReader {
	return s.subtaskReader
}

func newSubTaskExecutionContext(tCtx pluginsCore.TaskExecutionContext, taskTemplate *core.TaskTemplate, originalIndex int, retryAttempt uint64) SubTaskExecutionContext {
	arrayInputReader := array.GetInputReader(tCtx, taskTemplate) 
	taskExecutionMetadata := tCtx.TaskExecutionMetadata()
	taskExecutionID := taskExecutionMetadata.GetTaskExecutionID()
	metadataOverride := SubTaskExecutionMetadata{
		taskExecutionMetadata,
		SubTaskExecutionID{
			taskExecutionID,
			originalIndex,
			taskExecutionID.GetGeneratedName(),
			retryAttempt,
		},
	}

	subtaskTemplate := &core.TaskTemplate{}
	//var subtaskTemplate *core.TaskTemplate
	*subtaskTemplate = *taskTemplate

	if subtaskTemplate != nil {
		subtaskTemplate.TaskTypeVersion = 2
		if subtaskTemplate.GetContainer() != nil {
			subtaskTemplate.Type = podPlugin.ContainerTaskType
		} else if taskTemplate.GetK8SPod() != nil {
			subtaskTemplate.Type = podPlugin.SidecarTaskType
		}
	}

	subtaskReader := SubTaskReader{tCtx.TaskReader(), subtaskTemplate}

	return SubTaskExecutionContext{
		TaskExecutionContext: tCtx,
		arrayInputReader:     arrayInputReader,
		metadataOverride:     metadataOverride,
		originalIndex:        originalIndex,
		retryAttempt:         retryAttempt,
		subtaskReader:        subtaskReader,
	}
}

type SubTaskReader struct {
	pluginsCore.TaskReader
	subtaskTemplate *core.TaskTemplate
}

func (s SubTaskReader) Read(ctx context.Context) (*core.TaskTemplate, error) {
	return s.subtaskTemplate, nil
}

//s.stCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()
type SubTaskExecutionID struct {
	pluginsCore.TaskExecutionID
	originalIndex   int
	parentName      string
	retryAttempt    uint64
}

func (s SubTaskExecutionID) GetGeneratedName() string {
	indexStr := strconv.Itoa(s.originalIndex)

	// If the retryAttempt is 0 we do not include it in the pod name. The gives us backwards
	// compatibility in the ability to dynamically transition running map tasks to use subtask retries.
	if s.retryAttempt == 0 {
		return utils.ConvertToDNS1123SubdomainCompatibleString(fmt.Sprintf("%v-%v", s.parentName, indexStr))
	}

	retryAttemptStr := strconv.FormatUint(s.retryAttempt, 10)
	return utils.ConvertToDNS1123SubdomainCompatibleString(fmt.Sprintf("%v-%v-%v", s.parentName, indexStr, retryAttemptStr))
}

// TODO hamersaw - enable secrets
// TaskExecutionMetadata provides a layer on top of the core TaskExecutionMetadata with customized annotations and labels
// for k8s plugins.
type SubTaskExecutionMetadata struct {
	pluginsCore.TaskExecutionMetadata

	subtaskExecutionID SubTaskExecutionID
	//annotations map[string]string
	//labels      map[string]string
}

/*func (t TaskExecutionMetadata) GetLabels() map[string]string {
	return t.labels
}

func (t TaskExecutionMetadata) GetAnnotations() map[string]string {
	return t.annotations
}

// newTaskExecutionMetadata creates a TaskExecutionMetadata with secrets serialized as annotations and a label added
// to trigger the flyte pod webhook
func newTaskExecutionMetadata(tCtx pluginsCore.TaskExecutionMetadata, taskTmpl *core.TaskTemplate) (TaskExecutionMetadata, error) {
	var err error
	secretsMap := make(map[string]string)
	injectSecretsLabel := make(map[string]string)
	if taskTmpl.SecurityContext != nil && len(taskTmpl.SecurityContext.Secrets) > 0 {
		secretsMap, err = secrets.MarshalSecretsToMapStrings(taskTmpl.SecurityContext.Secrets)
		if err != nil {
			return TaskExecutionMetadata{}, err
		}

		injectSecretsLabel = map[string]string{
			secrets.PodLabel: secrets.PodLabelValue,
		}
	}

	return TaskExecutionMetadata{
		TaskExecutionMetadata: tCtx,
		annotations:           utils.UnionMaps(tCtx.GetAnnotations(), secretsMap),
		labels:                utils.UnionMaps(tCtx.GetLabels(), injectSecretsLabel),
	}, nil
}*/
