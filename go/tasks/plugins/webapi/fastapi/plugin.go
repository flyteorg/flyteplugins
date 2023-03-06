package fastapi

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	pluginErrors "github.com/flyteorg/flyteplugins/go/tasks/errors"
	pluginsCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flytestdlib/errors"
	"github.com/flyteorg/flytestdlib/logger"
	"io/ioutil"
	"net/http"
	"time"

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
	pluginAPI    string           = "plugins/v1/dummy"
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
}

type ResourceMetaWrapper struct {
	OutputPrefix string
	Token        string
	JobID        string
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
	taskTemplatePath, err := taskCtx.TaskReader().Path(ctx)
	if err != nil {
		return nil, nil, err
	}

	body := map[string]string{
		"inputs_path":        taskCtx.InputReader().GetInputPath().String(),
		"task_template_path": taskTemplatePath.String(),
	}

	mJSON, err := json.Marshal(body)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal data: %v: %v", body, err)
	}

	postDataJSON := []byte(string(mJSON))
	req, err := buildRequest(postMethod, postDataJSON, p.cfg.fastAPIEndpoint, "token", "")
	if err != nil {
		return nil, nil, err
	}

	start := time.Now()
	resp, err := p.client.Do(req)
	logger.Infof(ctx, "fastapi create request latency [%v]", time.Since(start).Round(time.Microsecond).String())
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

	jobID := fmt.Sprintf("%s", data["job_id"])

	return &ResourceMetaWrapper{
		OutputPrefix: taskCtx.OutputWriter().GetOutputPrefixPath().String(),
		JobID:        jobID,
		Token:        "",
	}, &ResourceWrapper{StatusCode: resp.StatusCode}, nil
}

func (p Plugin) Get(ctx context.Context, taskCtx webapi.GetContext) (latest webapi.Resource, err error) {
	metadata := taskCtx.ResourceMeta().(*ResourceMetaWrapper)
	prevState := "running"
	if taskCtx.Resource() != nil {
		resource := taskCtx.Resource().(*ResourceWrapper)
		prevState = resource.State
	}

	body := map[string]string{
		"output_prefix": metadata.OutputPrefix,
		"job_id":        metadata.JobID,
		"prev_state":    prevState,
	}

	mJSON, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal data: %v: %v", body, err)
	}

	getDataJSON := []byte(string(mJSON))
	req, err := buildRequest(getMethod, getDataJSON, p.cfg.fastAPIEndpoint, metadata.Token, metadata.JobID)
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

	state := fmt.Sprintf("%s", data["state"])
	return &ResourceWrapper{
		StatusCode: resp.StatusCode,
		State:      state,
	}, nil
}

func (p Plugin) Delete(ctx context.Context, taskCtx webapi.DeleteContext) error {
	if taskCtx.ResourceMeta() == nil {
		return nil
	}
	exec := taskCtx.ResourceMeta().(ResourceMetaWrapper)
	req, err := buildRequest(deleteMethod, nil, p.cfg.fastAPIEndpoint, exec.Token, exec.JobID)
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

func (p Plugin) Status(_ context.Context, taskCtx webapi.StatusContext) (phase core.PhaseInfo, err error) {
	resource := taskCtx.Resource().(*ResourceWrapper)
	statusCode := resource.StatusCode
	state := resource.State

	if statusCode == 0 {
		return core.PhaseInfoUndefined, errors.Errorf(ErrSystem, "No Status field set.")
	}

	taskInfo := &core.TaskInfo{}
	message := ""

	switch statusCode {
	case http.StatusAccepted:
		return core.PhaseInfoRunning(pluginsCore.DefaultPhaseVersion, taskInfo), nil
	case http.StatusOK:
		switch state {
		case "succeeded":
			return pluginsCore.PhaseInfoSuccess(taskInfo), nil
		case "failed":
			return core.PhaseInfoFailure(string(rune(statusCode)), "failed to run the job", taskInfo), nil
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

func buildRequest(method string, data []byte, fastAPIEndpoint string, token string, jobID string) (*http.Request, error) {
	var fastAPIURL string
	// for mocking/testing purposes
	if fastAPIEndpoint == "" {
		fastAPIURL = fmt.Sprintf("http://backend-plugin-system.flyte.svc.cluster.local:8000/%v", pluginAPI)
	} else {
		fastAPIURL = fmt.Sprintf("%v%v", fastAPIEndpoint, pluginAPI)
	}

	if method == deleteMethod {
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

func newFastAPIPlugin() webapi.PluginEntry {
	return webapi.PluginEntry{
		ID:                 "fastapi",
		SupportedTaskTypes: []core.TaskType{"bigquery_query_job_task", "snowflake", "spark"},
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
