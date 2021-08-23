package snowflake

import (
	"testing"

	pluginUtils "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/utils"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestUnmarshalSnowflakeQueryConfig(t *testing.T) {
	custom := structpb.Struct{
		Fields: map[string]*structpb.Value{
			"Account":   structpb.NewStringValue("test-account"),
			"Warehouse": structpb.NewStringValue("test-warehouse"),
			"Schema":    structpb.NewStringValue("test-schema"),
			"Database":  structpb.NewStringValue("test-database"),
			"Statement": structpb.NewStringValue("SELECT 1"),
		},
	}

	prestoQuery := QueryJobConfig{}
	err := pluginUtils.UnmarshalStructToObj(&custom, &prestoQuery)
	assert.NoError(t, err)

	assert.Equal(t, prestoQuery, QueryJobConfig{
		Account:   "test-account",
		Warehouse: "test-warehouse",
		Schema:    "test-schema",
		Database:  "test-database",
		Statement: "SELECT 1",
	})
}
