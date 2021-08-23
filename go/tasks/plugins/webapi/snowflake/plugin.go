package snowflake

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"

	flyteIdlCore "github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	pluginErrors "github.com/flyteorg/flyteplugins/go/tasks/errors"
	pluginsCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core/template"
	pluginUtils "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/utils"
	"github.com/flyteorg/flytestdlib/errors"
	"github.com/flyteorg/flytestdlib/logger"

	"github.com/flyteorg/flytestdlib/promutils"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/webapi"
)

const (
	ErrUser   errors.ErrorCode = "User"
	ErrSystem errors.ErrorCode = "System"
)

type Plugin struct {
	metricScope    promutils.Scope
	cfg            *Config
	snowflakeToken string
}

type ResourceWrapper struct {
	Status  string
	Message string
}

type ResourceMetaWrapper struct {
	QueryID string
	Account string
}

func (p Plugin) GetConfig() webapi.PluginConfig {
	return GetConfig().WebAPI
}

type QueryInfo struct {
	Account   string
	Warehouse string
	Schema    string
	Database  string
	Statement string
}

// TODO: Add QueryJobConfig in Flyteidl
type QueryJobConfig struct {
	Account   string `json:"account"`
	Warehouse string `json:"warehouse"`
	Schema    string `json:"schema"`
	Database  string `json:"database"`
	Statement string `json:"statement"`
}

func (p Plugin) ResourceRequirements(_ context.Context, _ webapi.TaskExecutionContextReader) (
	namespace core.ResourceNamespace, constraints core.ResourceConstraintsSpec, err error) {

	// Resource requirements are assumed to be the same.
	return "default", p.cfg.ResourceConstraints, nil
}

func (p Plugin) Create(ctx context.Context, taskCtx webapi.TaskExecutionContextReader) (webapi.ResourceMeta,
	webapi.Resource, error) {
	task, err := taskCtx.TaskReader().Read(ctx)
	if err != nil {
		return nil, nil, err
	}

	custom := task.GetCustom()
	snowflakeQuery := QueryJobConfig{}
	err = pluginUtils.UnmarshalStructToObj(custom, &snowflakeQuery)
	if err != nil {
		return nil, nil, errors.Wrapf(ErrUser, err, "Expects a valid PrestoQuery proto in custom field.")
	}
	outputs, err := template.Render(ctx, []string{
		snowflakeQuery.Account,
		snowflakeQuery.Warehouse,
		snowflakeQuery.Schema,
		snowflakeQuery.Database,
		snowflakeQuery.Statement,
	}, template.Parameters{
		TaskExecMetadata: taskCtx.TaskExecutionMetadata(),
		Inputs:           taskCtx.InputReader(),
		OutputPath:       taskCtx.OutputWriter(),
		Task:             taskCtx.TaskReader(),
	})
	if err != nil {
		return nil, nil, err
	}
	queryInfo := QueryInfo{
		Account:   outputs[0],
		Warehouse: outputs[1],
		Schema:    outputs[2],
		Database:  outputs[3],
		Statement: outputs[4],
	}

	if len(queryInfo.Warehouse) == 0 {
		queryInfo.Warehouse = p.cfg.DefaultWarehouse
	}
	req, err := buildRequest("POST", queryInfo, queryInfo.Account, p.snowflakeToken, "", false)
	if err != nil {
		return nil, nil, err
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	data, err := buildResponse(resp)
	if err != nil {
		return nil, nil, err
	}
	queryID := fmt.Sprintf("%v", data["statementHandle"])
	message := fmt.Sprintf("%v", data["message"])

	return ResourceMetaWrapper{queryID, queryInfo.Account},
		ResourceWrapper{Status: resp.Status, Message: message}, nil
}

func (p Plugin) Get(ctx context.Context, taskCtx webapi.GetContext) (latest webapi.Resource, err error) {
	exec := taskCtx.ResourceMeta().(*ResourceMetaWrapper)
	req, err := buildRequest("GET", QueryInfo{}, exec.Account, p.snowflakeToken, exec.QueryID, false)
	if err != nil {
		return nil, err
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := buildResponse(resp)
	if err != nil {
		return nil, err
	}
	message := fmt.Sprintf("%v", data["message"])
	return ResourceWrapper{
		Status:  resp.Status,
		Message: message,
	}, nil
}

func (p Plugin) Delete(ctx context.Context, taskCtx webapi.DeleteContext) error {
	exec := taskCtx.ResourceMeta().(*ResourceMetaWrapper)
	req, err := buildRequest("POST", QueryInfo{}, exec.Account, p.snowflakeToken, exec.QueryID, true)
	if err != nil {
		return err
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	logger.Info(ctx, "Deleted query execution [%v]", resp)

	return nil
}

func (p Plugin) Status(_ context.Context, taskCtx webapi.StatusContext) (phase core.PhaseInfo, err error) {
	exec := taskCtx.ResourceMeta().(*ResourceMetaWrapper)
	status := taskCtx.Resource().(*ResourceWrapper).Status
	if status == "" {
		return core.PhaseInfoUndefined, errors.Errorf(ErrSystem, "No Status field set.")
	}
	statusCode, err := strconv.Atoi(status)
	if err != nil {
		return core.PhaseInfoUndefined, pluginErrors.Errorf(pluginsCore.SystemErrorCode, "unknown execution phase [%v].", status)
	}
	taskInfo := createTaskInfo(exec.QueryID, exec.Account)
	switch statusCode {
	case http.StatusAccepted:
		return core.PhaseInfoRunning(pluginsCore.DefaultPhaseVersion, createTaskInfo(exec.QueryID, exec.Account)), nil
	case http.StatusOK:
		return pluginsCore.PhaseInfoSuccess(taskInfo), nil
	case http.StatusUnprocessableEntity:
		return pluginsCore.PhaseInfoFailure(status, "phaseReason", taskInfo), nil
	}
	return core.PhaseInfoUndefined, pluginErrors.Errorf(pluginsCore.SystemErrorCode, "unknown execution phase [%v].", status)
}

func buildRequest(method string, queryInfo QueryInfo, account string, token string,
	queryID string, isCancel bool) (*http.Request, error) {
	snowflakeURL := "https://" + account + ".snowflakecomputing.com/api/statements"
	var data []byte
	if method == "POST" && !isCancel {
		snowflakeURL += "?async=true"
		data = []byte(fmt.Sprintf(`{
		  "statement": "%v",
		  "database": "%v",
		  "schema": "%v",
		  "warehouse": "%v"
		}`, queryInfo.Statement, queryInfo.Database, queryInfo.Schema, queryInfo.Warehouse))
	} else {
		snowflakeURL += "/" + queryID
	}
	if isCancel {
		snowflakeURL += "/cancel"
	}

	req, err := http.NewRequest(method, snowflakeURL, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", "Bearer "+token)
	req.Header.Add("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")
	return req, nil
}

func buildResponse(response *http.Response) (map[string]interface{}, error) {
	responseBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	var data map[string]interface{}
	err = json.Unmarshal(responseBody, &data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func createTaskInfo(queryID string, account string) *core.TaskInfo {
	timeNow := time.Now()

	return &core.TaskInfo{
		OccurredAt: &timeNow,
		Logs: []*flyteIdlCore.TaskLog{
			{
				Uri: fmt.Sprintf("https://%v.snowflakecomputing.com/console#/monitoring/queries/detail?queryId=%v",
					account,
					queryID),
				Name: "Snowflake Console",
			},
		},
	}
}

func getSnowflakeToken() string {
	return os.Getenv("SNOWFLAKE_TOKEN")
}

func newSnowflakeJobTaskPlugin() webapi.PluginEntry {
	return webapi.PluginEntry{
		ID:                 "snowflake",
		SupportedTaskTypes: []core.TaskType{"snowflake"},
		PluginLoader: func(ctx context.Context, iCtx webapi.PluginSetupContext) (webapi.AsyncPlugin, error) {
			return &Plugin{
				metricScope:    iCtx.MetricsScope(),
				cfg:            GetConfig(),
				snowflakeToken: getSnowflakeToken(),
			}, nil
		},
	}
}

func init() {
	gob.Register(ResourceMetaWrapper{})
	gob.Register(ResourceWrapper{})

	pluginmachinery.PluginRegistry().RegisterRemotePlugin(newSnowflakeJobTaskPlugin())
}
