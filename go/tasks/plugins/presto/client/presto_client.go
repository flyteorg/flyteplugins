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

// Contains information needed to execute a Presto query
type PrestoExecuteArgs struct {
	RoutingGroup string `json:"routingGroup,omitempty"`
	Catalog      string `json:"catalog,omitempty"`
	Schema       string `json:"schema,omitempty"`
	Source       string `json:"source,omitempty"`
}

// Representation of a response after submitting a query to Presto
type PrestoExecuteResponse struct {
	ID      string
	Status  PrestoStatus
	NextURI string
}

//go:generate mockery -all -case=snake

// Interface to interact with PrestoClient for Presto tasks
type PrestoClient interface {
	// Submits a query to Presto
	ExecuteCommand(ctx context.Context, commandStr string, executeArgs PrestoExecuteArgs) (interface{}, error)

	// Cancels a currently running Presto query
	KillCommand(ctx context.Context, commandID string) error

	// Gets the status of a Presto query
	GetCommandStatus(ctx context.Context, commandID string) (PrestoStatus, error)
}

func (p noopPrestoClient) ExecuteCommand(
	ctx context.Context,
	queryStr string,
	executeArgs PrestoExecuteArgs) (interface{}, error) {

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
