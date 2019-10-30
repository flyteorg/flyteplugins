package mocks

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type FakeKubeClient struct {
	syncObj sync.RWMutex
	Cache   map[string]runtime.Object
}

func formatKey(name types.NamespacedName, kind schema.GroupVersionKind) string {
	key := fmt.Sprintf("%v:%v", name.String(), kind.String())
	return key
}

func (m *FakeKubeClient) Get(ctx context.Context, key client.ObjectKey, out runtime.Object) error {
	m.syncObj.RLock()
	defer m.syncObj.RUnlock()

	item, found := m.Cache[formatKey(key, out.GetObjectKind().GroupVersionKind())]
	if found {
		// deep copy to avoid mutating cache
		item = item.(runtime.Object).DeepCopyObject()
		_, isUnstructured := out.(*unstructured.Unstructured)
		if isUnstructured {
			// Copy the value of the item in the cache to the returned value
			outVal := reflect.ValueOf(out)
			objVal := reflect.ValueOf(item)
			if !objVal.Type().AssignableTo(outVal.Type()) {
				return fmt.Errorf("cache had type %s, but %s was asked for", objVal.Type(), outVal.Type())
			}
			reflect.Indirect(outVal).Set(reflect.Indirect(objVal))
			return nil
		}

		p, err := runtime.DefaultUnstructuredConverter.ToUnstructured(item)
		if err != nil {
			return err
		}

		return runtime.DefaultUnstructuredConverter.FromUnstructured(p, out)
	}

	return errors.NewNotFound(schema.GroupResource{}, key.Name)
}

func (m *FakeKubeClient) List(ctx context.Context, opts *client.ListOptions, list runtime.Object) error {
	m.syncObj.RLock()
	defer m.syncObj.RUnlock()

	objs := make([]runtime.Object, 0, len(m.Cache))

	for _, val := range m.Cache {
		if opts.Raw != nil {
			if val.GetObjectKind().GroupVersionKind().Kind != opts.Raw.Kind {
				continue
			}

			if val.GetObjectKind().GroupVersionKind().GroupVersion().String() != opts.Raw.APIVersion {
				continue
			}
		}

		objs = append(objs, val.(runtime.Object).DeepCopyObject())
	}

	return meta.SetList(list, objs)
}

func (m *FakeKubeClient) Create(ctx context.Context, obj runtime.Object) (err error) {
	m.syncObj.Lock()
	defer m.syncObj.Unlock()

	accessor, err := meta.Accessor(obj)
	if err != nil {
		return err
	}

	key := formatKey(types.NamespacedName{
		Name:      accessor.GetName(),
		Namespace: accessor.GetNamespace(),
	}, obj.GetObjectKind().GroupVersionKind())

	if _, exists := m.Cache[key]; !exists {
		m.Cache[key] = obj
		return nil
	}

	return errors.NewAlreadyExists(schema.GroupResource{}, accessor.GetName())
}

func (m *FakeKubeClient) Delete(ctx context.Context, obj runtime.Object, opts ...client.DeleteOptionFunc) error {
	m.syncObj.Lock()
	defer m.syncObj.Unlock()

	accessor, err := meta.Accessor(obj)
	if err != nil {
		return err
	}

	key := formatKey(types.NamespacedName{
		Name:      accessor.GetName(),
		Namespace: accessor.GetNamespace(),
	}, obj.GetObjectKind().GroupVersionKind())

	if _, exists := m.Cache[key]; exists {
		delete(m.Cache, key)
	}

	return nil
}

func (m *FakeKubeClient) Update(ctx context.Context, obj runtime.Object) error {
	m.syncObj.Lock()
	defer m.syncObj.Unlock()

	accessor, err := meta.Accessor(obj)
	if err != nil {
		return err
	}

	key := formatKey(types.NamespacedName{
		Name:      accessor.GetName(),
		Namespace: accessor.GetNamespace(),
	}, obj.GetObjectKind().GroupVersionKind())

	if _, exists := m.Cache[key]; exists {
		m.Cache[key] = obj
		return nil
	}

	return errors.NewNotFound(schema.GroupResource{}, accessor.GetName())
}

func (*FakeKubeClient) Status() client.StatusWriter {
	panic("implement me")
}

func NewFakeKubeClient() *FakeKubeClient {
	return &FakeKubeClient{
		syncObj: sync.RWMutex{},
		Cache:   map[string]runtime.Object{},
	}
}
