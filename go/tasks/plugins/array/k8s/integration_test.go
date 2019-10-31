package k8s

import (
	"strconv"
	"testing"

	"github.com/lyft/flyteplugins/go/tasks/plugins/array"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"

	"github.com/lyft/flytestdlib/storage"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"

	"context"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/workqueue"

	"github.com/lyft/flytestdlib/contextutils"
	"github.com/lyft/flytestdlib/promutils/labeled"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/stretchr/testify/assert"

	"github.com/lyft/flyteidl/clients/go/coreutils"
)

func init() {
	labeled.SetMetricKeys(contextutils.WorkflowIDKey)
}

func newMockExecutor(ctx context.Context, t testing.TB) (Executor, array.AdvanceIteration) {
	kubeClient := &mocks.KubeClient{}
	kubeClient.OnGetClient().Return(mocks.NewFakeKubeClient())
	e, err := NewExecutor(kubeClient, &Config{
		MaxErrorStringLength: 200,
		OutputAssembler: workqueue.Config{
			Workers:            2,
			MaxRetries:         0,
			IndexCacheMaxItems: 100,
		},
		ErrorAssembler: workqueue.Config{
			Workers:            2,
			MaxRetries:         0,
			IndexCacheMaxItems: 100,
		},
	}, promutils.NewTestScope())
	assert.NoError(t, err)

	assert.NoError(t, e.Start(ctx))
	return e, func(ctx context.Context, tCtx core.TaskExecutionContext) error {
		return advancePodPhases(context.Background(), tCtx.DataStore(), tCtx.OutputWriter(), kubeClient.GetClient())
	}
}

func TestEndToEnd(t *testing.T) {
	ctx := context.Background()
	executor, iter := newMockExecutor(ctx, t)
	array.RunArrayTestsEndToEnd(t, executor, iter)
}

func advancePodPhases(ctx context.Context, store *storage.DataStore, outputWriter io.OutputWriter, runtimeClient client.Client) error {
	podList := &v1.PodList{}
	err := runtimeClient.List(ctx, podList, &client.ListOptions{
		Raw: &metav1.ListOptions{
			TypeMeta: metav1.TypeMeta{
				Kind:       "pod",
				APIVersion: v1.SchemeGroupVersion.String(),
			},
		},
	})
	if err != nil {
		return err
	}

	for _, pod := range podList.Items {
		newPhase := nextHappyPodPhase(pod.Status.Phase)
		pod.Status.ContainerStatuses = []v1.ContainerStatus{
			{ContainerID: "cont_123"},
		}

		if pod.Status.Phase != newPhase && newPhase == v1.PodSucceeded {
			idx := -1
			env := pod.Spec.Containers[0].Env
			for _, v := range env {
				if v.Name == "FLYTE_K8S_ARRAY_INDEX" {
					idx, err = strconv.Atoi(v.Value)
					if err != nil {
						return err
					}

					break
				}
			}

			ref := outputWriter.GetOutputPath()
			if idx > -1 {
				ref, err = store.ConstructReference(ctx, outputWriter.GetOutputPrefixPath(), strconv.Itoa(idx), "outputs.pb")
				if err != nil {
					return err
				}
			}

			err = store.WriteProtobuf(ctx, ref, storage.Options{},
				coreutils.MustMakeLiteral(map[string]interface{}{
					"x": 5,
				}).GetMap())
			if err != nil {
				return err
			}
		}

		pod.Status.Phase = newPhase

		err = runtimeClient.Update(ctx, pod.DeepCopy())
		if err != nil {
			return err
		}
	}

	return nil
}

func nextHappyPodPhase(phase v1.PodPhase) v1.PodPhase {
	switch phase {
	case v1.PodUnknown:
		fallthrough
	case v1.PodPending:
		fallthrough
	case "":
		return v1.PodRunning
	case v1.PodRunning:
		return v1.PodSucceeded
	case v1.PodSucceeded:
		return v1.PodSucceeded
	}

	return v1.PodUnknown
}
