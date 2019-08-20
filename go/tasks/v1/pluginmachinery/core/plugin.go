package core

import (
	"context"
)

//go:generate mockery -all -case=underscore

type TaskType = string

type PluginLoader func(ctx context.Context, iCtx SetupContext) (Plugin, error)

type PluginEntry struct {
	ID                  TaskType
	RegisteredTaskTypes []TaskType
	LoadPlugin          PluginLoader
	IsDefault           bool
}

type PluginProperties struct {
	DisableNodeLevelCaching bool
}

type Plugin interface {
	GetID() string
	GetProperties() PluginProperties
	Handle(ctx context.Context, tCtx TaskExecutionContext) (Transition, error)
	Abort(ctx context.Context, tCtx TaskExecutionContext) error
	Finalize(ctx context.Context, tCtx TaskExecutionContext) error
}
