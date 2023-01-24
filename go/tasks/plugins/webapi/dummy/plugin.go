package databricks

import (
	"context"
	"encoding/gob"
	flyteIdlCore "github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"math/rand"
	"net/http"
	"time"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/ioutils"

	pluginsCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flytestdlib/errors"

	"github.com/flyteorg/flytestdlib/promutils"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/webapi"
)

const (
	ErrSystem errors.ErrorCode = "System"
	post      string           = "POST"
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
	JobID      string
	Message    string
}

type ResourceMetaWrapper struct {
	RunID string
	Token string
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
	_, err := taskCtx.TaskReader().Read(ctx)
	if err != nil {
		return nil, nil, err
	}

	// Sending requests and deserialization times
	time.Sleep(10 * time.Millisecond)

	return &ResourceMetaWrapper{RunID: "runID", Token: "token"},
		&ResourceWrapper{StatusCode: 200}, nil
}

func (p Plugin) Get(ctx context.Context, taskCtx webapi.GetContext) (latest webapi.Resource, err error) {
	// Sending requests and deserialization times
	time.Sleep(10 * time.Millisecond)

	return &ResourceWrapper{
		StatusCode: 200,
		JobID:      "jobID",
	}, nil
}

func (p Plugin) Delete(ctx context.Context, taskCtx webapi.DeleteContext) error {
	return nil
}

func (p Plugin) Status(ctx context.Context, taskCtx webapi.StatusContext) (phase core.PhaseInfo, err error) {
	x := rand.Intn(100)
	if x < 50 {
		err := writeOutput(ctx, taskCtx, "s3://bucket/key")
		if err != nil {
			return core.PhaseInfo{}, err
		}
		return pluginsCore.PhaseInfoSuccess(&core.TaskInfo{}), nil
	}
	return core.PhaseInfoRunning(pluginsCore.DefaultPhaseVersion, &core.TaskInfo{}), nil
}

func writeOutput(ctx context.Context, tCtx webapi.StatusContext, OutputLocation string) error {
	_, err := tCtx.TaskReader().Read(ctx)
	if err != nil {
		return err
	}

	return tCtx.OutputWriter().Put(ctx, ioutils.NewInMemoryOutputReader(
		&flyteIdlCore.LiteralMap{
			Literals: map[string]*flyteIdlCore.Literal{
				"results": {
					Value: &flyteIdlCore.Literal_Scalar{
						Scalar: &flyteIdlCore.Scalar{
							Value: &flyteIdlCore.Scalar_StructuredDataset{
								StructuredDataset: &flyteIdlCore.StructuredDataset{
									Uri: OutputLocation,
									Metadata: &flyteIdlCore.StructuredDatasetMetadata{
										StructuredDatasetType: &flyteIdlCore.StructuredDatasetType{Format: ""},
									},
								},
							},
						},
					},
				},
			},
		}, nil, nil))
}

func newDummyTaskPlugin() webapi.PluginEntry {
	return webapi.PluginEntry{
		ID:                 "dummy",
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

	pluginmachinery.PluginRegistry().RegisterRemotePlugin(newDummyTaskPlugin())
}
