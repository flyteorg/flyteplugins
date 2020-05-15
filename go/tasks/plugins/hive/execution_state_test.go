package hive

import (
	"context"
	"testing"

	"github.com/lyft/flytestdlib/contextutils"
	"github.com/lyft/flytestdlib/promutils/labeled"

	idlCore "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	pluginsCoreMocks "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	"github.com/lyft/flyteplugins/go/tasks/plugins/hive/config"
)

const (
	DefaultClusterPrimaryLabel = "default"
)

func init() {
	labeled.SetMetricKeys(contextutils.NamespaceKey)
}

func TestGetQueryInfo(t *testing.T) {
	ctx := context.Background()

	taskTemplate := GetSingleHiveQueryTaskTemplate()
	mockTaskReader := &mocks.TaskReader{}
	mockTaskReader.On("Read", mock.Anything).Return(&taskTemplate, nil)

	mockTaskExecutionContext := mocks.TaskExecutionContext{}
	mockTaskExecutionContext.On("TaskReader").Return(mockTaskReader)

	taskMetadata := &pluginsCoreMocks.TaskExecutionMetadata{}
	taskMetadata.On("GetNamespace").Return("myproject-staging")
	taskMetadata.On("GetLabels").Return(map[string]string{"sample": "label"})
	mockTaskExecutionContext.On("TaskExecutionMetadata").Return(taskMetadata)

	query, cluster, tags, timeout, err := GetQueryInfo(ctx, &mockTaskExecutionContext)
	assert.NoError(t, err)
	assert.Equal(t, "select 'one'", query)
	assert.Equal(t, "default", cluster)
	assert.Equal(t, []string{"flyte_plugin_test", "ns:myproject-staging", "sample:label"}, tags)
	assert.Equal(t, 500, int(timeout))
}

func TestValidateQuboleHiveJob(t *testing.T) {
	hiveJob := plugins.QuboleHiveJob{
		ClusterLabel: "default",
		Tags:         []string{"flyte_plugin_test", "sample:label"},
		Query:        nil,
	}
	err := validateQuboleHiveJob(hiveJob)
	assert.Error(t, err)
}

func createMockQuboleCfg() *config.Config {
	return &config.Config{
		DefaultClusterLabel: "default",
		ClusterConfigs: []config.ClusterConfig{
			{PrimaryLabel: "primary A", Labels: []string{"primary A", "A", "label A", "A-prod"}, Limit: 10},
			{PrimaryLabel: "primary B", Labels: []string{"B"}, Limit: 10},
			{PrimaryLabel: "primary C", Labels: []string{"C-prod"}, Limit: 1},
		},
		DestinationClusterConfigs: []config.DestinationClusterConfig{
			{Project: "project A", Domain: "domain X", ClusterLabel: "A-prod"},
			{Project: "project A", Domain: "domain Y", ClusterLabel: "A"},
			{Project: "project A", Domain: "domain Z", ClusterLabel: "B"},
			{Project: "project C", Domain: "domain X", ClusterLabel: "C-prod"},
		},
	}
}

func Test_mapLabelToPrimaryLabel(t *testing.T) {
	ctx := context.TODO()
	mockQuboleCfg := createMockQuboleCfg()

	type args struct {
		ctx       context.Context
		quboleCfg *config.Config
		label     string
	}
	tests := []struct {
		name      string
		args      args
		want      string
		wantFound bool
	}{
		{name: "Label has a mapping", args: args{ctx: ctx, quboleCfg: mockQuboleCfg, label: "A-prod"}, want: "primary A", wantFound: true},
		{name: "Label has a typo", args: args{ctx: ctx, quboleCfg: mockQuboleCfg, label: "a"}, want: DefaultClusterPrimaryLabel, wantFound: false},
		{name: "Label has a mapping 2", args: args{ctx: ctx, quboleCfg: mockQuboleCfg, label: "C-prod"}, want: "primary C", wantFound: true},
		{name: "Label has a typo 2", args: args{ctx: ctx, quboleCfg: mockQuboleCfg, label: "C_prod"}, want: DefaultClusterPrimaryLabel, wantFound: false},
		{name: "Label has a mapping 3", args: args{ctx: ctx, quboleCfg: mockQuboleCfg, label: "primary A"}, want: "primary A", wantFound: true},
		{name: "Label has no mapping", args: args{ctx: ctx, quboleCfg: mockQuboleCfg, label: "D"}, want: DefaultClusterPrimaryLabel, wantFound: false},
		{name: "Label is an empty string", args: args{ctx: ctx, quboleCfg: mockQuboleCfg, label: ""}, want: DefaultClusterPrimaryLabel, wantFound: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, found := mapLabelToPrimaryLabel(tt.args.ctx, tt.args.quboleCfg, tt.args.label); got != tt.want || found != tt.wantFound {
				t.Errorf("mapLabelToPrimaryLabel() = (%v, %v), want (%v, %v)", got, found, tt.want, tt.wantFound)
			}
		})
	}
}

func createMockTaskExecutionContextWithProjectDomain(project string, domain string) *mocks.TaskExecutionContext {
	mockTaskExecutionContext := mocks.TaskExecutionContext{}
	taskExecID := &pluginsCoreMocks.TaskExecutionID{}
	taskExecID.OnGetID().Return(idlCore.TaskExecutionIdentifier{
		NodeExecutionId: &idlCore.NodeExecutionIdentifier{ExecutionId: &idlCore.WorkflowExecutionIdentifier{
			Project: project,
			Domain:  domain,
			Name:    "random name",
		}},
	})

	taskMetadata := &pluginsCoreMocks.TaskExecutionMetadata{}
	taskMetadata.OnGetTaskExecutionID().Return(taskExecID)
	mockTaskExecutionContext.On("TaskExecutionMetadata").Return(taskMetadata)
	return &mockTaskExecutionContext
}

func Test_getClusterPrimaryLabel(t *testing.T) {
	ctx := context.TODO()
	err := config.SetQuboleConfig(createMockQuboleCfg())
	assert.Nil(t, err)

	type args struct {
		ctx                  context.Context
		tCtx                 core.TaskExecutionContext
		clusterLabelOverride string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{name: "Override is not empty + override has NO existing mapping + project-domain has an existing mapping", args: args{ctx: ctx, tCtx: createMockTaskExecutionContextWithProjectDomain("project A", "domain Z"), clusterLabelOverride: "AAAA"}, want: "primary B"},
		{name: "Override is not empty + override has NO existing mapping + project-domain has NO existing mapping", args: args{ctx: ctx, tCtx: createMockTaskExecutionContextWithProjectDomain("project A", "domain blah"), clusterLabelOverride: "blh"}, want: DefaultClusterPrimaryLabel},
		{name: "Override is not empty + override has an existing mapping + project-domain has NO existing mapping", args: args{ctx: ctx, tCtx: createMockTaskExecutionContextWithProjectDomain("project blah", "domain blah"), clusterLabelOverride: "C-prod"}, want: "primary C"},
		{name: "Override is not empty + override has an existing mapping + project-domain has an existing mapping", args: args{ctx: ctx, tCtx: createMockTaskExecutionContextWithProjectDomain("project A", "domain A"), clusterLabelOverride: "C-prod"}, want: "primary C"},
		{name: "Override is empty + project-domain has an existing mapping", args: args{ctx: ctx, tCtx: createMockTaskExecutionContextWithProjectDomain("project A", "domain X"), clusterLabelOverride: ""}, want: "primary A"},
		{name: "Override is empty + project-domain has an existing mapping2", args: args{ctx: ctx, tCtx: createMockTaskExecutionContextWithProjectDomain("project A", "domain Z"), clusterLabelOverride: ""}, want: "primary B"},
		{name: "Override is empty + project-domain has NO existing mapping", args: args{ctx: ctx, tCtx: createMockTaskExecutionContextWithProjectDomain("project A", "domain blah"), clusterLabelOverride: ""}, want: DefaultClusterPrimaryLabel},
		{name: "Override is empty + project-domain has NO existing mapping2", args: args{ctx: ctx, tCtx: createMockTaskExecutionContextWithProjectDomain("project blah", "domain X"), clusterLabelOverride: ""}, want: DefaultClusterPrimaryLabel},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getClusterPrimaryLabel(tt.args.ctx, tt.args.tCtx, tt.args.clusterLabelOverride); got != tt.want {
				t.Errorf("getClusterPrimaryLabel() = %v, want %v", got, tt.want)
			}
		})
	}
}
