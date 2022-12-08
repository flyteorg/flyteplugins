package databricks

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	flyteIdlCore "github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/plugins"
	pluginErrors "github.com/flyteorg/flyteplugins/go/tasks/errors"
	pluginsCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core/template"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/utils"
	"github.com/flyteorg/flytestdlib/errors"
	"github.com/flyteorg/flytestdlib/logger"

	"github.com/flyteorg/flytestdlib/promutils"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/webapi"
)

const (
	ErrSystem errors.ErrorCode = "System"
	post      string           = "POST"
	get       string           = "GET"
)

// for mocking/testing purposes, and we'll override this method
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type Plugin struct {
	metricScope promutils.Scope
	cfg         *Config
	client      HTTPClient
}

type ResourceWrapper struct {
	StatusCode     int
	LifeCycleState string
	ResultState    string
	JobID          string
	Message        string
}

type ResourceMetaWrapper struct {
	RunID              string
	DatabricksInstance string
	Token              string
}

func (p Plugin) GetConfig() webapi.PluginConfig {
	return GetConfig().WebAPI
}

func (p Plugin) ResourceRequirements(_ context.Context, _ webapi.TaskExecutionContextReader) (
	namespace core.ResourceNamespace, constraints core.ResourceConstraintsSpec, err error) {

	// Resource requirements are assumed to be the same.
	return "default", p.cfg.ResourceConstraints, nil
}

func (p Plugin) Create(ctx context.Context, taskCtx webapi.TaskExecutionContextReader) (webapi.ResourceMeta,
	webapi.Resource, error) {
	taskTemplate, err := taskCtx.TaskReader().Read(ctx)
	if err != nil {
		return nil, nil, err
	}

	token, err := taskCtx.SecretManager().Get(ctx, p.cfg.TokenKey)
	if err != nil {
		return nil, nil, err
	}

	container := taskTemplate.GetContainer()
	sparkJob := plugins.SparkJob{}
	err = utils.UnmarshalStruct(taskTemplate.GetCustom(), &sparkJob)
	if err != nil {
		return nil, nil, errors.Wrapf(pluginErrors.BadTaskSpecification, err, "invalid TaskSpecification [%v], failed to unmarshal", taskTemplate.GetCustom())
	}

	modifiedArgs, err := template.Render(ctx, container.GetArgs(), template.Parameters{
		TaskExecMetadata: taskCtx.TaskExecutionMetadata(),
		Inputs:           taskCtx.InputReader(),
		OutputPath:       taskCtx.OutputWriter(),
		Task:             taskCtx.TaskReader(),
	})
	if err != nil {
		return nil, nil, err
	}

	decodeBytes, err := base64.StdEncoding.DecodeString(sparkJob.DatabricksConf)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to decode databricksJob: %v: %v", sparkJob.DatabricksConf, err)
	}

	databricksJob := make(map[string]interface{})
	err = json.Unmarshal(decodeBytes, &databricksJob)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal databricksJob: %v: %v", decodeBytes, err)
	}

	if err != nil {
		return nil, nil, err
	}

	if _, ok := databricksJob["new_cluster"]; ok {
		databricksJob["new_cluster"].(map[string]interface{})["docker_image"] = map[string]string{"url": container.Image}
	}
	databricksJob["spark_python_task"] = map[string]interface{}{"python_file": p.cfg.EntrypointFile, "parameters": modifiedArgs}

	req, err := buildRequest(post, databricksJob, p.cfg.databricksEndpoint,
		p.cfg.DatabricksInstance, token, "", false)
	if err != nil {
		return nil, nil, err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	data, err := buildResponse(resp)
	if err != nil {
		return nil, nil, err
	}

	if data["run_id"] == "" {
		return nil, nil, pluginErrors.Wrapf(pluginErrors.RuntimeFailure, err,
			"Unable to fetch statementHandle from http response")
	}
	runID := fmt.Sprintf("%v", data["run_id"])

	return &ResourceMetaWrapper{runID, p.cfg.DatabricksInstance, token},
		&ResourceWrapper{StatusCode: resp.StatusCode}, nil
}

func (p Plugin) Get(ctx context.Context, taskCtx webapi.GetContext) (latest webapi.Resource, err error) {
	exec := taskCtx.ResourceMeta().(*ResourceMetaWrapper)
	req, err := buildRequest(get, nil, p.cfg.databricksEndpoint,
		p.cfg.DatabricksInstance, exec.Token, exec.RunID, false)
	if err != nil {
		logger.Errorf(ctx, "Failed to build databricks job request [%v]", err)
		return nil, err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		logger.Errorf(ctx, "Failed to get databricks job status [%v]", resp)
		return nil, err
	}
	defer resp.Body.Close()
	data, err := buildResponse(resp)
	if err != nil {
		return nil, err
	}
	jobState := data["state"].(map[string]interface{})
	message := fmt.Sprintf("%s", jobState["state_message"])
	jobID := fmt.Sprintf("%.0f", data["job_id"])
	lifeCycleState := fmt.Sprintf("%s", jobState["life_cycle_state"])
	resultState := fmt.Sprintf("%s", jobState["result_state"])
	return &ResourceWrapper{
		StatusCode:     resp.StatusCode,
		JobID:          jobID,
		LifeCycleState: lifeCycleState,
		ResultState:    resultState,
		Message:        message,
	}, nil
}

func (p Plugin) Delete(ctx context.Context, taskCtx webapi.DeleteContext) error {
	exec := taskCtx.ResourceMeta().(ResourceMetaWrapper)
	req, err := buildRequest(post, nil, p.cfg.databricksEndpoint,
		p.cfg.DatabricksInstance, exec.Token, exec.RunID, true)
	if err != nil {
		return err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	logger.Info(ctx, "Deleted query execution [%v]", resp)

	return nil
}

func (p Plugin) Status(ctx context.Context, taskCtx webapi.StatusContext) (phase core.PhaseInfo, err error) {
	exec := taskCtx.ResourceMeta().(*ResourceMetaWrapper)
	resource := taskCtx.Resource().(*ResourceWrapper)
	message := resource.Message
	statusCode := resource.StatusCode
	jobID := resource.JobID
	lifeCycleState := resource.LifeCycleState
	resultState := resource.ResultState

	if statusCode == 0 {
		return core.PhaseInfoUndefined, errors.Errorf(ErrSystem, "No Status field set.")
	}

	taskInfo := createTaskInfo(exec.RunID, jobID, exec.DatabricksInstance)
	switch statusCode {
	// Job response format. https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunsSubmit
	case http.StatusAccepted:
		return core.PhaseInfoRunning(pluginsCore.DefaultPhaseVersion, taskInfo), nil
	case http.StatusOK:
		if lifeCycleState == "TERMINATED" {
			if resultState == "SUCCESS" {
				return pluginsCore.PhaseInfoSuccess(taskInfo), nil
			}
			return pluginsCore.PhaseInfoFailure(string(rune(statusCode)), message, taskInfo), nil
		}
		return core.PhaseInfoRunning(pluginsCore.DefaultPhaseVersion, taskInfo), nil
	case http.StatusBadRequest:
		fallthrough
	case http.StatusInternalServerError:
		fallthrough
	case http.StatusUnauthorized:
		return pluginsCore.PhaseInfoFailure(string(rune(statusCode)), message, taskInfo), nil
	}
	return core.PhaseInfoUndefined, pluginErrors.Errorf(pluginsCore.SystemErrorCode, "unknown execution phase [%v].", statusCode)
}

func buildRequest(
	method string,
	databricksJob map[string]interface{},
	databricksEndpoint string,
	databricksInstance string,
	token string,
	runID string,
	isCancel bool,
) (*http.Request, error) {
	var databricksURL string
	// for mocking/testing purposes
	if databricksEndpoint == "" {
		databricksURL = fmt.Sprintf("https://%v.cloud.databricks.com/api/2.0/jobs/runs", databricksInstance)
	} else {
		databricksURL = fmt.Sprintf("%v/api/2.0/jobs/runs", databricksEndpoint)
	}

	var data []byte
	var req *http.Request
	var err error
	if isCancel {
		databricksURL += "/cancel"
		data = []byte(fmt.Sprintf("{ run_id: %v }", runID))
	} else if method == post {
		databricksURL += "/submit"
		mJSON, err := json.Marshal(databricksJob)
		if err != nil {
			fmt.Println(err.Error())
			return nil, err
		}
		data = []byte(string(mJSON))
	} else {
		databricksURL += "/get?run_id=" + runID
	}

	if data == nil {
		req, err = http.NewRequest(method, databricksURL, nil)
	} else {
		req, err = http.NewRequest(method, databricksURL, bytes.NewBuffer(data))
	}
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", "Bearer "+token)
	req.Header.Add("Content-Type", "application/json")
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

func createTaskInfo(runID, jobID, databricksInstance string) *core.TaskInfo {
	timeNow := time.Now()

	return &core.TaskInfo{
		OccurredAt: &timeNow,
		Logs: []*flyteIdlCore.TaskLog{
			{
				Uri: fmt.Sprintf("https://%s.cloud.databricks.com/#job/%s/run/%s",
					databricksInstance,
					jobID,
					runID),
				Name: "Databricks Console",
			},
		},
	}
}

func newDatabricksJobTaskPlugin() webapi.PluginEntry {
	return webapi.PluginEntry{
		ID:                 "databricks",
		SupportedTaskTypes: []core.TaskType{"spark"},
		PluginLoader: func(ctx context.Context, iCtx webapi.PluginSetupContext) (webapi.AsyncPlugin, error) {
			return &Plugin{
				metricScope: iCtx.MetricsScope(),
				cfg:         GetConfig(),
				client:      &http.Client{},
			}, nil
		},
	}
}

func init() {
	gob.Register(ResourceMetaWrapper{})
	gob.Register(ResourceWrapper{})

	pluginmachinery.PluginRegistry().RegisterRemotePlugin(newDatabricksJobTaskPlugin())
}
