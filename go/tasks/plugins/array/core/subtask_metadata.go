package core

import (
	"context"

	idlCore "github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
)

type SubTaskMetadata struct {
	ChildIndex    int
	Logs          []*idlCore.TaskLog
	OriginalIndex int
	SubTaskID     *string
}

func InitializeSubTaskMetadata(ctx context.Context, tCtx core.TaskExecutionContext, state *State,
	generateSubTaskID func(core.TaskExecutionContext, int) string) ([]*SubTaskMetadata, error) {

	subTaskMetadata := make([]*SubTaskMetadata, state.GetOriginalArraySize())

	executeSubTaskCount := 0
	cachedSubTaskCount := 0
	for i := 0; i < int(state.GetOriginalArraySize()); i++ {
		var childIndex int
		if state.IndexesToCache.IsSet(uint(i)) {
			childIndex = executeSubTaskCount
			executeSubTaskCount++
		} else {
			childIndex = state.GetExecutionArraySize() + cachedSubTaskCount
			cachedSubTaskCount++
		}

		subTaskID := generateSubTaskID(tCtx, childIndex)
		subTaskMetadata[i] = &SubTaskMetadata{
			ChildIndex:    childIndex,
			Logs:          nil,
			OriginalIndex: i,
			SubTaskID:     &subTaskID,
		}
	}

	return subTaskMetadata, nil
}
