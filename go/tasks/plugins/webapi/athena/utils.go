package athena

import (
	"context"

	"github.com/lyft/flyteplugins/go/tasks/errors"

	pluginsIdl "github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"
	"github.com/lyft/flytestdlib/utils"

	pb "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/ioutils"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/webapi"
	"github.com/lyft/flytestdlib/logger"
)

func writeOutput(ctx context.Context, tCtx webapi.StatusContext, externalLocation string) error {
	taskTemplate, err := tCtx.TaskReader().Read(ctx)
	if err != nil {
		return err
	}

	resultsSchema, exists := taskTemplate.Interface.Outputs.Variables["results"]
	if !exists {
		logger.Infof(ctx, "The task declares no outputs. Skipping writing the outputs.")
		return nil
	}

	return tCtx.OutputWriter().Put(ctx, ioutils.NewInMemoryOutputReader(
		&pb.LiteralMap{
			Literals: map[string]*pb.Literal{
				"results": {
					Value: &pb.Literal_Scalar{
						Scalar: &pb.Scalar{Value: &pb.Scalar_Schema{
							Schema: &pb.Schema{
								Uri:  externalLocation,
								Type: resultsSchema.GetType().GetSchema(),
							},
						},
						},
					},
				},
			},
		}, nil))
}

type QueryInfo struct {
	QueryString string
	Workgroup   string
	Catalog     string
	Database    string
}

func extractQueryInfo(task *pb.TaskTemplate) (QueryInfo, error) {
	switch task.Type {
	case "hive":
		custom := task.GetCustom()
		hiveQuery := pluginsIdl.QuboleHiveJob{}
		err := utils.UnmarshalStructToPb(custom, &hiveQuery)
		if err == nil && hiveQuery.Query != nil && len(hiveQuery.Query.Query) > 0 {
			return QueryInfo{
				QueryString: hiveQuery.Query.Query,
				Database:    hiveQuery.ClusterLabel,
			}, nil
		}

		return QueryInfo{}, errors.Errorf(ErrUser, "Expects a valid QuboleHiveJob proto in custom field.")
	case "presto":
		custom := task.GetCustom()
		prestoQuery := pluginsIdl.PrestoQuery{}
		err := utils.UnmarshalStructToPb(custom, &prestoQuery)
		if err == nil && len(prestoQuery.Statement) > 0 {
			return QueryInfo{
				QueryString: prestoQuery.Statement,
				Database:    prestoQuery.Schema,
				Catalog:     prestoQuery.Catalog,
				Workgroup:   prestoQuery.RoutingGroup,
			}, nil
		}

		return QueryInfo{}, errors.Errorf(ErrUser, "Expects a valid PrestoQuery proto in custom field.")
	}

	return QueryInfo{}, errors.Errorf(ErrUser, "Unexpected task type [%v].", task.Type)
}
