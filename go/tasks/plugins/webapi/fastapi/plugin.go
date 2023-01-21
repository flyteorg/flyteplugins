package fastapi

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/ioutils"

	flyteIdlCore "github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	pluginErrors "github.com/flyteorg/flyteplugins/go/tasks/errors"
	pluginsCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flytestdlib/errors"
	"github.com/flyteorg/flytestdlib/logger"

	"github.com/flyteorg/flytestdlib/promutils"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/webapi"
)

const (
	ErrSystem    errors.ErrorCode = "System"
	postMethod   string           = "POST"
	getMethod    string           = "GET"
	deleteMethod string           = "DELETE"
	pluginAPI    string           = "/plugins/v1/bigquery/v1"
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
	StatusCode int
	State      string
	JobID      string
}

type ResourceMetaWrapper struct {
	Token string
	JobID string
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

	mJSON, err := json.Marshal(taskTemplate)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal post data: %v: %v", taskTemplate, err)
	}

	postData := []byte(string(mJSON))
	req, err := buildRequest(postMethod, postData, p.cfg.fastApiEndpoint, token, "")
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
	if data["job_id"] == "" {
		return nil, nil, pluginErrors.Wrapf(pluginErrors.RuntimeFailure, err,
			"Unable to extract job_id from http response")
	}

	jobID := fmt.Sprintf("%.0f", data["job_id"])

	return &ResourceMetaWrapper{Token: token, JobID: jobID}, &ResourceWrapper{JobID: jobID}, nil
}

func (p Plugin) Get(ctx context.Context, taskCtx webapi.GetContext) (latest webapi.Resource, err error) {
	exec := taskCtx.ResourceMeta().(*ResourceMetaWrapper)

	req, err := buildRequest(getMethod, nil, p.cfg.fastApiEndpoint, exec.Token, exec.JobID)
	if err != nil {
		logger.Errorf(ctx, "Failed to build fast api job request [%v]", err)
		return nil, err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		logger.Errorf(ctx, "Failed to get job status [%v]", resp)
		return nil, err
	}
	defer resp.Body.Close()
	data, err := buildResponse(resp)
	if err != nil {
		return nil, err
	}

	jobID := fmt.Sprintf("%.0f", data["job_id"])
	state := fmt.Sprintf("%s", data["state"])
	return &ResourceWrapper{
		StatusCode: resp.StatusCode,
		JobID:      jobID,
		State:      state,
	}, nil
}

func (p Plugin) Delete(ctx context.Context, taskCtx webapi.DeleteContext) error {
	exec := taskCtx.ResourceMeta().(ResourceMetaWrapper)
	req, err := buildRequest(deleteMethod, nil, p.cfg.fastApiEndpoint, exec.Token, exec.JobID)
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
	resource := taskCtx.Resource().(*ResourceWrapper)
	statusCode := resource.StatusCode
	state := resource.State
	// jobID := resource.JobID

	if statusCode == 0 {
		return core.PhaseInfoUndefined, errors.Errorf(ErrSystem, "No Status field set.")
	}

	// TODO: Add task link
	// taskInfo := createTaskInfo(exec.RunID, jobID, exec.DatabricksInstance)
	taskInfo := &core.TaskInfo{}
	message := ""

	switch statusCode {
	case http.StatusAccepted:
		return core.PhaseInfoRunning(pluginsCore.DefaultPhaseVersion, taskInfo), nil
	case http.StatusOK:
		switch state {
		case "succeeded":
			return pluginsCore.PhaseInfoSuccess(taskInfo), nil
		//case "pending":
		//	return core.PhaseInfoQueuedWithTaskInfo(pluginsCore.DefaultPhaseVersion, message, taskInfo), nil
		default:
			return core.PhaseInfoRunning(pluginsCore.DefaultPhaseVersion, taskInfo), nil
		}
	case http.StatusBadRequest:
		fallthrough
	case http.StatusInternalServerError:
		fallthrough
	case http.StatusUnauthorized:
		return pluginsCore.PhaseInfoFailure(string(rune(statusCode)), message, taskInfo), nil
	}
	return core.PhaseInfoUndefined, pluginErrors.Errorf(pluginsCore.SystemErrorCode, "unknown execution phase [%v].", statusCode)
}

func writeOutput(ctx context.Context, taskCtx webapi.StatusContext) error {
	taskTemplate, err := taskCtx.TaskReader().Read(ctx)
	if err != nil {
		return err
	}
	if taskTemplate.Interface == nil || taskTemplate.Interface.Outputs == nil || taskTemplate.Interface.Outputs.Variables == nil {
		logger.Infof(ctx, "The task declares no outputs. Skipping writing the outputs.")
		return nil
	}

	outputReader := ioutils.NewRemoteFileOutputReader(ctx, taskCtx.DataStore(), taskCtx.OutputWriter(), taskCtx.MaxDatasetSizeBytes())
	return taskCtx.OutputWriter().Put(ctx, outputReader)
}

func buildRequest(method string, data []byte, fastAPIEndpoint string, token string, jobID string) (*http.Request, error) {
	var fastAPIURL string
	// for mocking/testing purposes
	if fastAPIEndpoint == "" {
		fastAPIURL = fmt.Sprintf("http://backend-plugin-service:8000%v", pluginAPI)
	} else {
		fastAPIURL = fmt.Sprintf("%v%v", fastAPIEndpoint, pluginAPI)
	}

	if method == deleteMethod || method == getMethod {
		fastAPIURL = fmt.Sprintf("%v/?job_id=%v", fastAPIURL, jobID)
	}

	var req *http.Request
	var err error
	if data == nil {
		req, err = http.NewRequest(method, fastAPIURL, nil)
	} else {
		req, err = http.NewRequest(method, fastAPIURL, bytes.NewBuffer(data))
	}
	if err != nil {
		return nil, err
	}

	// TODO: authentication support
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
				Uri: fmt.Sprintf("https://%s/#job/%s/run/%s",
					databricksInstance,
					jobID,
					runID),
				Name: "FastAPI Console",
			},
		},
	}
}

func newFastAPIPlugin() webapi.PluginEntry {
	return webapi.PluginEntry{
		ID:                 "fastapi",
		SupportedTaskTypes: []core.TaskType{"bigquery", "snowflake", "spark"},
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

	pluginmachinery.PluginRegistry().RegisterRemotePlugin(newFastAPIPlugin())
}
