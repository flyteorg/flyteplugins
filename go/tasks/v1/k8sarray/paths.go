package k8sarray

import (
	"context"

	"github.com/lyft/flytestdlib/storage"
)

const (
	InputsSuffix      = "inputs.pb"
	FuturesSuffix     = "futures.pb"
	OutputsSuffix     = "outputs.pb"
	ErrorsSuffix      = "error.pb"
	IndexLookupSuffix = "indexlookup.pb"
)

func GetPath(ctx context.Context, store storage.ReferenceConstructor, root storage.DataReference, subNames ...string) (res storage.DataReference, err error) {
	return store.ConstructReference(ctx, root, subNames...)
}

func GetArrayOutputsPrefixPath(ctx context.Context, store storage.ReferenceConstructor, temp storage.DataReference, subName ...string) (res storage.DataReference, err error) {
	return store.ConstructReference(ctx, temp, subName...)
}

func GetMasterOutputsPath(ctx context.Context, store storage.ReferenceConstructor, output storage.DataReference) (res storage.DataReference, err error) {
	return store.ConstructReference(ctx, output, OutputsSuffix)
}

func GetInputsPath(ctx context.Context, store storage.ReferenceConstructor, prefix storage.DataReference) (res storage.DataReference, err error) {
	return store.ConstructReference(ctx, prefix, InputsSuffix)
}

func GetOutputsPath(ctx context.Context, store storage.ReferenceConstructor, prefix storage.DataReference) (res storage.DataReference, err error) {
	return store.ConstructReference(ctx, prefix, OutputsSuffix)
}

func GetFuturesPath(ctx context.Context, store storage.ReferenceConstructor, prefix storage.DataReference) (res storage.DataReference, err error) {
	return store.ConstructReference(ctx, prefix, FuturesSuffix)
}

func GetErrorsPath(ctx context.Context, store storage.ReferenceConstructor, prefix storage.DataReference) (res storage.DataReference, err error) {
	return store.ConstructReference(ctx, prefix, ErrorsSuffix)
}

func GetIndexLookupPath(ctx context.Context, store storage.ReferenceConstructor, prefix storage.DataReference) (res storage.DataReference, err error) {
	return store.ConstructReference(ctx, prefix, IndexLookupSuffix)
}
