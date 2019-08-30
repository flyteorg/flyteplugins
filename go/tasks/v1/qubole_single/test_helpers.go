package qubole_single

import (
	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/utils"
)

func GetSingleHiveQueryTaskTemplate() core.TaskTemplate {
	hiveJob := plugins.QuboleHiveJob{
		ClusterLabel: "default",
		Tags:         []string{"flyte_plugin_test"},
		Query: &plugins.HiveQuery{
			TimeoutSec: 500,
			Query:      "select 'one'",
			RetryCount: 0,
		},
		// Even though it's deprecated, we might have one element in the query collection for backwards compatibility
		QueryCollection: &plugins.HiveQueryCollection{
			Queries: []*plugins.HiveQuery{
				{
					TimeoutSec: 500,
					Query:      "select 'one'",
					RetryCount: 0,
				},
			},
		},
	}
	stObj := &structpb.Struct{}
	_ = utils.MarshalStruct(&hiveJob, stObj)
	tt := core.TaskTemplate{
		Type:   "hive",
		Custom: stObj,
		Id: &core.Identifier{
			Name:         "sample_hive_task_test_name",
			Project:      "flyteplugins",
			Version:      "1",
			ResourceType: core.ResourceType_TASK,
		},
	}

	return tt
}
