package client

import (
	"context"
	"net/http"
	"net/url"

	"time"

	"github.com/lyft/flyteplugins/go/tasks/plugins/presto/config"
)

const (
	httpRequestTimeoutSecs = 30
)

type noopPrestoClient struct {
	client      *http.Client
	environment *url.URL
}

type PrestoExecuteArgs struct {
	RoutingGroup string `json:"routingGroup,omitempty"`
	Catalog      string `json:"catalog,omitempty"`
	Schema       string `json:"schema,omitempty"`
	Source       string `json:"source,omitempty"`
}
type PrestoExecuteResponse struct {
	ID      string
	Status  PrestoStatus
	NextURI string
}

//go:generate mockery -all -case=snake

type PrestoClient interface {
	ExecuteCommand(ctx context.Context, commandStr string, extraArgs interface{}) (interface{}, error)
	KillCommand(ctx context.Context, commandID string) error
	GetCommandStatus(ctx context.Context, commandID string) (PrestoStatus, error)
}

func (p noopPrestoClient) ExecuteCommand(
	ctx context.Context,
	queryStr string,
	extraArgs interface{}) (interface{}, error) {

	return PrestoExecuteResponse{}, nil
}

func (p noopPrestoClient) KillCommand(ctx context.Context, commandID string) error {
	return nil
}

func (p noopPrestoClient) GetCommandStatus(ctx context.Context, commandID string) (PrestoStatus, error) {
	return NewPrestoStatus(ctx, "UNKNOWN"), nil
}

func NewNoopPrestoClient(cfg *config.Config) PrestoClient {
	return &noopPrestoClient{
		client:      &http.Client{Timeout: httpRequestTimeoutSecs * time.Second},
		environment: cfg.Environment.ResolveReference(&cfg.Environment.URL),
	}
}
