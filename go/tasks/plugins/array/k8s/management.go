package k8s

import (
	"context"
	"fmt"
	"strings"

	idlCore "github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/flyteorg/flyteplugins/go/tasks/errors"
	"github.com/flyteorg/flyteplugins/go/tasks/logs"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/plugins/array"
	"github.com/flyteorg/flyteplugins/go/tasks/plugins/array/arraystatus"
	arrayCore "github.com/flyteorg/flyteplugins/go/tasks/plugins/array/core"
	"github.com/flyteorg/flyteplugins/go/tasks/plugins/array/errorcollector"

	"github.com/flyteorg/flytestdlib/bitarray"
	"github.com/flyteorg/flytestdlib/logger"
	"github.com/flyteorg/flytestdlib/storage"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
)

func allocateResource(ctx context.Context, tCtx core.TaskExecutionContext, config *Config, podName string) (core.AllocationStatus, error) {
	if !IsResourceConfigSet(config.ResourceConfig) {
		return core.AllocationStatusGranted, nil
	}

	resourceNamespace := core.ResourceNamespace(config.ResourceConfig.PrimaryLabel)
	resourceConstraintSpec := core.ResourceConstraintsSpec{
		ProjectScopeResourceConstraint:   nil,
		NamespaceScopeResourceConstraint: nil,
	}

	allocationStatus, err := tCtx.ResourceManager().AllocateResource(ctx, resourceNamespace, podName, resourceConstraintSpec)
	if err != nil {
		return core.AllocationUndefined, err
	}

	return allocationStatus, nil
}

func deallocateResource(ctx context.Context, tCtx core.TaskExecutionContext, config *Config, podName string) error {
	if !IsResourceConfigSet(config.ResourceConfig) {
		return nil
	}
	resourceNamespace := core.ResourceNamespace(config.ResourceConfig.PrimaryLabel)

	err := tCtx.ResourceManager().ReleaseResource(ctx, resourceNamespace, podName)
	if err != nil {
		logger.Errorf(ctx, "Error releasing token [%s]. error %s", podName, err)
		return err
	}

	return nil
}

func LaunchAndCheckSubTasksState(ctx context.Context, tCtx core.TaskExecutionContext, kubeClient core.KubeClient,
	config *Config, dataStore *storage.DataStore, outputPrefix, baseOutputDataSandbox storage.DataReference, currentState *arrayCore.State) (
	newState *arrayCore.State, logLinks []*idlCore.TaskLog, subTaskIDs []*string, err error) {
	if int64(currentState.GetExecutionArraySize()) > config.MaxArrayJobSize {
		ee := fmt.Errorf("array size > max allowed. Requested [%v]. Allowed [%v]", currentState.GetExecutionArraySize(), config.MaxArrayJobSize)
		logger.Info(ctx, ee)
		currentState = currentState.SetPhase(arrayCore.PhasePermanentFailure, 0).SetReason(ee.Error())
		return currentState, logLinks, subTaskIDs, nil
	}

	logLinks = make([]*idlCore.TaskLog, 0, 4)
	newState = currentState
	messageCollector := errorcollector.NewErrorMessageCollector()
	newArrayStatus := &arraystatus.ArrayStatus{
		Summary:  arraystatus.ArraySummary{},
		Detailed: arrayCore.NewPhasesCompactArray(uint(currentState.GetExecutionArraySize())),
	}
	subTaskIDs = make([]*string, 0, len(currentState.GetArrayStatus().Detailed.GetItems()))

	// If we have arrived at this state for the first time then currentState has not been
	// initialized with number of sub tasks.
	if len(currentState.GetArrayStatus().Detailed.GetItems()) == 0 {
		currentState.ArrayStatus = *newArrayStatus
	}

	// If the current State is newly minted then we must initialize RetryAttempts to track how many
	// times each subtask is executed.
	if len(currentState.RetryAttempts.GetItems()) == 0 {
		count := uint(currentState.GetExecutionArraySize())
		maxValue := bitarray.Item(tCtx.TaskExecutionMetadata().GetMaxAttempts())

		retryAttemptsArray, err := bitarray.NewCompactArray(count, maxValue)
		if err != nil {
			logger.Errorf(context.Background(), "Failed to create attempts compact array with [count: %v, maxValue: %v]", count, maxValue)
			return currentState, logLinks, subTaskIDs, nil
		}

		// Initialize subtask retryAttempts to 0 so that, in tandem with the podName logic, we
		// maintain backwards compatibility.
		for i := 0; i < currentState.GetExecutionArraySize(); i++ {
			retryAttemptsArray.SetItem(i, 0)
		}

		currentState.RetryAttempts = retryAttemptsArray
	}

	// initialize log plugin
	logPlugin, err := logs.InitializeLogPlugins(&config.LogConfig.Config)
	if err != nil {
		return currentState, logLinks, subTaskIDs, err
	}

	// identify max parallelism
	taskTemplate, err := tCtx.TaskReader().Read(ctx)
	if err != nil {
		return currentState, logLinks, subTaskIDs, err
	} else if taskTemplate == nil {
		return currentState, logLinks, subTaskIDs, errors.Errorf(errors.BadTaskSpecification, "Required value not set, taskTemplate is nil")
	}

	arrayJob, err := arrayCore.ToArrayJob(taskTemplate.GetCustom(), taskTemplate.TaskTypeVersion)
	if err != nil {
		return currentState, logLinks, subTaskIDs, err
	}

	currentParallelism := 0
	maxParallelism := int(arrayJob.Parallelism)

	for childIdx, existingPhaseIdx := range currentState.GetArrayStatus().Detailed.GetItems() {
		existingPhase := core.Phases[existingPhaseIdx]
		retryAttempt := currentState.RetryAttempts.GetItem(childIdx)

		if existingPhase == core.PhaseRetryableFailure {
			retryAttempt++
			newState.RetryAttempts.SetItem(childIdx, retryAttempt)
		} else if existingPhase.IsTerminal() {
			newArrayStatus.Detailed.SetItem(childIdx, bitarray.Item(existingPhase))
			continue
		}

		originalIdx := arrayCore.CalculateOriginalIndex(childIdx, newState.GetIndexesToCache())
		stCtx := newSubTaskExecutionContext(tCtx, taskTemplate, childIdx, originalIdx, retryAttempt)
		podName := stCtx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()

		if existingPhase == core.PhaseUndefined || existingPhase == core.PhaseWaitingForResources || existingPhase == core.PhaseRetryableFailure {
			// attempt to allocateResource
			allocationStatus, err := allocateResource(ctx, stCtx, config, podName)
			if err != nil {
				logger.Errorf(ctx, "Resource manager failed for TaskExecId [%s] token [%s]. error %s",
					stCtx.TaskExecutionMetadata().GetTaskExecutionID().GetID(), podName, err)
				return currentState, logLinks, subTaskIDs, err
			}

			logger.Infof(ctx, "Allocation result for [%s] is [%s]", podName, allocationStatus)
			if allocationStatus != core.AllocationStatusGranted {
				newArrayStatus.Detailed.SetItem(childIdx, bitarray.Item(core.PhaseWaitingForResources))
				continue
			}

			// create subtask
			err = launchSubtask(ctx, stCtx, config, kubeClient)
			if err != nil && !k8serrors.IsAlreadyExists(err) {
				if k8serrors.IsForbidden(err) {
					if strings.Contains(err.Error(), "exceeded quota") {
						// TODO: Quota errors are retried forever, it would be good to have support for backoff strategy.
						logger.Infof(ctx, "Failed to launch  job, resource quota exceeded. Err: %v", err)
						newState = newState.SetPhase(arrayCore.PhaseWaitingForResources, 0).SetReason("Not enough resources to launch job")
					} else {
						newState = newState.SetPhase(arrayCore.PhaseRetryableFailure, 0).SetReason("Failed to launch job.")
					}

					newState.SetReason(err.Error())
					return newState, logLinks, subTaskIDs, nil
				}

				return currentState, logLinks, subTaskIDs, err
			}
		}

		// monitor pod
		phaseInfo, err := getSubtaskPhaseInfo(ctx, stCtx, config, kubeClient, logPlugin)
		if err != nil {
			return currentState, logLinks, subTaskIDs, err
		}

		if phaseInfo.Err() != nil {
			messageCollector.Collect(childIdx, phaseInfo.Err().String())
		}

		subTaskIDs = append(subTaskIDs, &podName)
		if phaseInfo.Info() != nil {
			logLinks = append(logLinks, phaseInfo.Info().Logs...)
		}

		// process subtask phase
		actualPhase := phaseInfo.Phase()
		if actualPhase.IsSuccess() {
			actualPhase, err = array.CheckTaskOutput(ctx, dataStore, outputPrefix, baseOutputDataSandbox, childIdx, originalIdx)
			if err != nil {
				return currentState, logLinks, subTaskIDs, err
			}
		}

		if actualPhase == core.PhaseRetryableFailure && uint32(retryAttempt+1) >= stCtx.TaskExecutionMetadata().GetMaxAttempts() {
			// If we see a retryable failure we must check if the number of retries exceeds the maximum
			// attempts. If so, transition to a permanent failure so that is not attempted again.
			newArrayStatus.Detailed.SetItem(childIdx, bitarray.Item(core.PhasePermanentFailure))
		} else {
			newArrayStatus.Detailed.SetItem(childIdx, bitarray.Item(actualPhase))
		}

		if actualPhase.IsTerminal() {
			err = deallocateResource(ctx, stCtx, config, podName)
			if err != nil {
				logger.Errorf(ctx, "Error releasing allocation token [%s] in Finalize [%s]", podName, err)
				return currentState, logLinks, subTaskIDs, err
			}

			// TODO - finalize and delete resource?
		}

		// validate parallelism
		if !actualPhase.IsTerminal() || actualPhase == core.PhaseRetryableFailure {
			currentParallelism++
		}

		if maxParallelism != 0 && currentParallelism >= maxParallelism {
			break
		}
	}

	// compute task phase from array status summary
	for _, phaseIdx := range newArrayStatus.Detailed.GetItems() {
		newArrayStatus.Summary.Inc(core.Phases[phaseIdx])
	}

	phase := arrayCore.SummaryToPhase(ctx, currentState.GetOriginalMinSuccesses()-currentState.GetOriginalArraySize()+int64(currentState.GetExecutionArraySize()), newArrayStatus.Summary)

	// process new state
	newState = newState.SetArrayStatus(*newArrayStatus)
	if phase == arrayCore.PhaseWriteToDiscoveryThenFail {
		errorMsg := messageCollector.Summary(GetConfig().MaxErrorStringLength)
		newState = newState.SetReason(errorMsg)
	}

	if phase == arrayCore.PhaseCheckingSubTaskExecutions {
		newPhaseVersion := uint32(0)

		// For now, the only changes to PhaseVersion and PreviousSummary occur for running array jobs.
		for phase, count := range newState.GetArrayStatus().Summary {
			newPhaseVersion += uint32(phase) * uint32(count)
		}

		newState = newState.SetPhase(phase, newPhaseVersion).SetReason("Task is still running.")
	} else {
		newState = newState.SetPhase(phase, core.DefaultPhaseVersion)
	}

	return newState, logLinks, subTaskIDs, nil
}

func TerminateSubTasks(ctx context.Context, tCtx core.TaskExecutionContext, kubeClient core.KubeClient, config *Config, currentState *arrayCore.State) error {
	// TODO - fix
	/*size := currentState.GetExecutionArraySize()
	messageCollector := errorcollector.NewErrorMessageCollector()
	for childIdx := 0; childIdx < size; childIdx++ {
		task := Task{
			ChildIdx: childIdx,
			Config:   config,
			State:    currentState,
		}

		err := task.Abort(ctx, tCtx, kubeClient)
		if err != nil {
			messageCollector.Collect(childIdx, err.Error())
		}
		err = task.Finalize(ctx, tCtx, kubeClient)
		if err != nil {
			messageCollector.Collect(childIdx, err.Error())
		}
	}

	if errs.Length() > 0 {
		return fmt.Errorf(errs.Summary(config.MaxErrorStringLength))
	}*/

	return nil
}
