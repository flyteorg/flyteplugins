package flytek8s

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lyft/flytestdlib/logger"

	"sigs.k8s.io/controller-runtime/pkg/cache/informertest"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var instance *flytek8s
var once sync.Once

type flytek8s struct {
	watchNamespace string
	kubeClient     client.Client
	informersCache cache.Cache
}

func (f *flytek8s) InjectClient(c client.Client) error {
	f.kubeClient = c
	return nil
}

func InjectClient(c client.Client) error {
	if instance == nil {
		return fmt.Errorf("instance not initialized")
	}

	return instance.InjectClient(c)
}

func (f *flytek8s) InjectCache(c cache.Cache) error {
	f.informersCache = c
	return nil
}

func InjectCache(c cache.Cache) error {
	if instance == nil {
		return fmt.Errorf("instance not initialized")
	}

	return instance.InjectCache(c)
}

func InitializeFake() client.Client {
	once.Do(func() {
		instance = &flytek8s{
			watchNamespace: "",
		}

		instance.kubeClient = fake.NewFakeClient()
		instance.informersCache = &informertest.FakeInformers{}
	})

	return instance.kubeClient
}

func Initialize(ctx context.Context, watchNamespace string, resyncPeriod time.Duration) (err error) {
	once.Do(func() {
		instance = &flytek8s{
			watchNamespace: watchNamespace,
		}

		kubeConfig := config.GetConfigOrDie()
		instance.kubeClient, err = client.New(kubeConfig, client.Options{})
		if err != nil {
			return
		}

		instance.informersCache, err = cache.New(kubeConfig, cache.Options{
			Namespace: watchNamespace,
			Resync:    &resyncPeriod,
		})

		if err == nil {
			go func() {
				logger.Infof(ctx, "Starting informers cache.")
				err = instance.informersCache.Start(ctx.Done())
				if err != nil {
					logger.Panicf(ctx, "Failed to start informers cache. Error: %v", err)
				}
			}()
		}
	})

	if err != nil {
		return err
	}

	if watchNamespace != instance.watchNamespace {
		return fmt.Errorf("flytek8s is supposed to be used under single namespace."+
			" configured-for: %v, requested-for: %v", instance.watchNamespace, watchNamespace)
	}

	return nil
}
