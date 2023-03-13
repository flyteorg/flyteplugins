package grpc

import (
	"context"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/service"
	"google.golang.org/grpc"
)

type MockClient struct {
}

func (m *MockClient) CreateTask(ctx context.Context, in *service.TaskCreateRequest, opts ...grpc.CallOption) (*service.TaskCreateResponse, error) {
	return &service.TaskCreateResponse{JobId: "job-id", Message: "succeed"}, nil
}

func (m *MockClient) GetTask(ctx context.Context, in *service.TaskGetRequest, opts ...grpc.CallOption) (*service.TaskGetResponse, error) {
	return &service.TaskGetResponse{State: service.State_SUCCEEDED, Message: "succeed"}, nil
}

func (m *MockClient) DeleteTask(ctx context.Context, in *service.TaskDeleteRequest, opts ...grpc.CallOption) (*service.TaskDeleteResponse, error) {
	return &service.TaskDeleteResponse{}, nil
}
