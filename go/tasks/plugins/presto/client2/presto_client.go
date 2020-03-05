package client

import (
	"context"
	"github.com/lyft/flyteplugins/go/tasks/plugins/command"
	"net/http"
	"net/url"

	"time"

	"github.com/lyft/flyteplugins/go/tasks/plugins/presto/config"
)

const (
	httpRequestTimeoutSecs = 30

	AcceptHeaderKey          = "Accept"
	ContentTypeHeaderKey     = "Content-Type"
	ContentTypeJSON          = "application/json"
	ContentTypeTextPlain     = "text/plain"
	PrestoCatalogHeader      = "X-Presto-Catalog"
	PrestoRoutingGroupHeader = "X-Presto-Routing-Group"
	PrestoSchemaHeader       = "X-Presto-Schema"
	PrestoSourceHeader       = "X-Presto-Source"
	PrestoUserHeader         = "X-Presto-User"
)

type prestoClient struct {
	client              *http.Client
	environment         *url.URL
	awsS3ShardFormatter string
	awsS3ShardCount     int
}

type PrestoExecuteArgs struct {
	RoutingGroup string
	Catalog      string
	Schema       string
	Source       string
}
type PrestoExecuteResponse struct {
	Id      string
	Status  command.CommandStatus
	NextUri string
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

func (p *prestoClient) GetCommandStatus(ctx context.Context, commandId string) (interface{}, error) {
	return PrestoStatusUnknown, nil
}

func NewPrestoClient(cfg *config.Config) command.CommandClient {
	return &prestoClient{
		client:              &http.Client{Timeout: httpRequestTimeoutSecs * time.Second},
		environment:         cfg.Environment.ResolveReference(&cfg.Environment.URL),
		awsS3ShardFormatter: cfg.AwsS3ShardFormatter,
		awsS3ShardCount:     cfg.AwsS3ShardCount,
	}
}
