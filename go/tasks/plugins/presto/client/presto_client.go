package client

import (
	"context"
	"github.com/lyft/flyteplugins/go/tasks/plugins/cmd"
	"net/http"
	"net/url"

	"time"

	"github.com/lyft/flyteplugins/go/tasks/plugins/presto/config"
)

const (
	httpRequestTimeoutSecs = 30
	//AcceptHeaderKey          = "Accept"
	//ContentTypeHeaderKey     = "Content-Type"
	//ContentTypeJSON          = "application/json"
	//ContentTypeTextPlain     = "text/plain"
	//PrestoCatalogHeader      = "X-Presto-Catalog"
	//PrestoRoutingGroupHeader = "X-Presto-Routing-Group"
	//PrestoSchemaHeader       = "X-Presto-Schema"
	//PrestoSourceHeader       = "X-Presto-Source"
	//PrestoUserHeader         = "X-Presto-User"
)

type prestoClient struct {
	client      *http.Client
	environment *url.URL
}

type PrestoExecuteArgs struct {
	RoutingGroup string `json:"routing_group,omitempty"`
	Catalog      string `json:"catalog,omitempty"`
	Schema       string `json:"schema,omitempty"`
	Source       string `json:"source,omitempty"`
}
type PrestoExecuteResponse struct {
	ID      string
	Status  cmd.CommandStatus
	NextURI string
}

func (p *prestoClient) ExecuteCommand(
	ctx context.Context,
	queryStr string,
	extraArgs interface{}) (interface{}, error) {

	return PrestoExecuteResponse{}, nil
}

func (p *prestoClient) KillCommand(ctx context.Context, commandID string) error {
	return nil
}

func (p *prestoClient) GetCommandStatus(ctx context.Context, commandID string) (cmd.CommandStatus, error) {
	return NewPrestoStatus(ctx, "UNKNOWN"), nil
}

func NewPrestoClient(cfg *config.Config) cmd.CommandClient {
	return &prestoClient{
		client:      &http.Client{Timeout: httpRequestTimeoutSecs * time.Second},
		environment: cfg.Environment.ResolveReference(&cfg.Environment.URL),
	}
}
