package databricks

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/plugins"
	"github.com/flyteorg/flytestdlib/utils"

	"github.com/flyteorg/flyteidl/clients/go/coreutils"
	coreIdl "github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	flyteIdlCore "github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery"
	pluginCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	pluginCoreMocks "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	"github.com/flyteorg/flyteplugins/tests"
	"github.com/flyteorg/flytestdlib/contextutils"
	"github.com/flyteorg/flytestdlib/promutils"
	"github.com/flyteorg/flytestdlib/promutils/labeled"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestEndToEnd(t *testing.T) {
	server := newFakeDatabricksServer()
	defer server.Close()

	iter := func(ctx context.Context, tCtx pluginCore.TaskExecutionContext) error {
		return nil
	}

	cfg := defaultConfig
	cfg.databricksEndpoint = server.URL
	cfg.WebAPI.Caching.Workers = 1
	cfg.WebAPI.Caching.ResyncInterval.Duration = 5 * time.Second
	err := SetConfig(&cfg)
	assert.NoError(t, err)

	pluginEntry := pluginmachinery.CreateRemotePlugin(newDatabricksJobTaskPlugin())
	plugin, err := pluginEntry.LoadPlugin(context.TODO(), newFakeSetupContext())
	assert.NoError(t, err)

	t.Run("run a spark job", func(t *testing.T) {
		sparkJob := plugins.SparkJob{DatabricksConf: "eyJydW5fbmFtZSI6ICJmbHl0ZWtpdCBkYXRhYnJpY2tzIHBsdWdpbiBleGFtcGxlIiwgIm5ld19jbHVzdGVyIjogeyJzcGFya192ZXJzaW9uIjogIjExLjAueC1zY2FsYTIuMTIiLCAibm9kZV90eXBlX2lkIjogInIzLnhsYXJnZSIsICJhd3NfYXR0cmlidXRlcyI6IHsiYXZhaWxhYmlsaXR5IjogIk9OX0RFTUFORCIsICJpbnN0YW5jZV9wcm9maWxlX2FybiI6ICJhcm46YXdzOmlhbTo6NTkwMzc1MjY0NDYwOmluc3RhbmNlLXByb2ZpbGUvZGF0YWJyaWNrcy1zMy1yb2xlIn0sICJudW1fd29ya2VycyI6IDR9LCAidGltZW91dF9zZWNvbmRzIjogMzYwMCwgIm1heF9yZXRyaWVzIjogMX0="}
		st, err := utils.MarshalPbToStruct(&sparkJob)
		assert.NoError(t, err)
		inputs, _ := coreutils.MakeLiteralMap(map[string]interface{}{"x": 1})
		template := flyteIdlCore.TaskTemplate{
			Type:   "databricks",
			Custom: st,
			Target: &coreIdl.TaskTemplate_Container{
				Container: &coreIdl.Container{
					Command: []string{"command"},
					Args:    []string{"pyflyte-execute"},
				},
			},
		}

		phase := tests.RunPluginEndToEndTest(t, plugin, &template, inputs, nil, nil, iter)

		assert.Equal(t, true, phase.Phase().IsSuccess())
	})
}

func newFakeDatabricksServer() *httptest.Server {
	runID := "065168461"
	jobID := "019e7546"
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/api/2.0/jobs/runs/submit" && request.Method == "POST" {
			writer.WriteHeader(202)
			bytes := []byte(fmt.Sprintf(`{
			  "run_id": "%v"
			}`, runID))
			_, _ = writer.Write(bytes)
			return
		}

		if request.URL.Path == "/api/2.0/jobs/runs/get" && request.Method == "GET" {
			writer.WriteHeader(200)
			bytes := []byte(fmt.Sprintf(`{
			  "job_id": "%v",
			  "state": {"state_message": "execution in progress.", "life_cycle_state": "TERMINATED", "result_state": "SUCCESS"}
			}`, jobID))
			_, _ = writer.Write(bytes)
			return
		}

		if request.URL.Path == "/api/2.0/jobs/runs/cancel" && request.Method == "POST" {
			writer.WriteHeader(200)
			return
		}

		writer.WriteHeader(500)
	}))
}

func newFakeSetupContext() *pluginCoreMocks.SetupContext {
	fakeResourceRegistrar := pluginCoreMocks.ResourceRegistrar{}
	fakeResourceRegistrar.On("RegisterResourceQuota", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	labeled.SetMetricKeys(contextutils.NamespaceKey)

	fakeSetupContext := pluginCoreMocks.SetupContext{}
	fakeSetupContext.OnMetricsScope().Return(promutils.NewScope("test"))
	fakeSetupContext.OnResourceRegistrar().Return(&fakeResourceRegistrar)

	return &fakeSetupContext
}
