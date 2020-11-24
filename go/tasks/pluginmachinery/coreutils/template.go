package coreutils

import (
	"context"
	"fmt"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/utils"
)

// Evaluates templates in each command with the equivalent value from passed args. Templates are case-insensitive
// Supported templates are:
// - {{ .InputFile }} to receive the input file path. The protocol used will depend on the underlying system
// 		configuration. E.g. s3://bucket/key/to/file.pb or /var/run/local.pb are both valid.
// - {{ .OutputPrefix }} to receive the path prefix for where to store the outputs.
// - {{ .Inputs.myInput }} to receive the actual value of the input passed. See docs on LiteralMapToTemplateArgs for how
// 		what to expect each literal type to be serialized as.
// If a command isn't a valid template or failed to evaluate, it'll be returned as is.
// NOTE: I wanted to do in-place replacement, until I realized that in-place replacement will alter the definition of the
// graph. This is not desirable, as we may have to retry and in that case the replacement will not work and we want
// to create a new location for outputs
func ReplaceTemplateCommandArgs(ctx context.Context, tExecMeta core.TaskExecutionMetadata, command []string, in io.InputReader,
	out io.OutputFilePaths) ([]string, error) {

	if len(command) == 0 {
		return []string{}, nil
	}
	if in == nil || out == nil {
		return nil, fmt.Errorf("input reader and output path cannot be nil")
	}
	res := make([]string, 0, len(command))
	for _, commandTemplate := range command {
		updated, err := utils.ReplaceTemplateCommandArgs(ctx, tExecMeta.GetTaskExecutionID().GetGeneratedName(), commandTemplate, in, out)
		if err != nil {
			return res, err
		}

		res = append(res, updated)
	}

	return res, nil
}
