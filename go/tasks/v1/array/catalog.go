package array

import (
	"context"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/v1/workqueue"
	"github.com/lyft/flyteplugins/go/tasks/v1/errors"
	"github.com/lyft/flytestdlib/logger"
)

func SubmitAndCheck(ctx context.Context, worker workqueue.IndexedWorkQueue, workItems []workqueue.WorkItem) (allFinished bool,
	retrievedItems []workqueue.WorkItem, err error) {

	retrievedItems = make([]workqueue.WorkItem, 0, len(workItems))
	allFinished = true

	for _, w := range workItems {
		err := worker.Queue(w)
		if err != nil {
			allFinished = false
			return allFinished, retrievedItems, errors.Wrapf(ErrorWorkQueue, err,
				"Error enqueuing work item %s", w.GetId())
		}
	}

	// Immediately read back from the work queue, and check to see if they are done
	for _, w := range workItems {
		retrievedItem, found, err := worker.Get(w.GetId())
		if err != nil {
			allFinished = false
			return allFinished, retrievedItems, err
		}
		if !found {
			logger.Warnf(ctx, "Item just placed into work queue has disappeared")
			allFinished = false
		}

		retrievedItems = append(retrievedItems, retrievedItem)
		if retrievedItem.GetWorkStatus() != workqueue.WorkStatusDone{
			allFinished = false
		}
	}

	return
}
