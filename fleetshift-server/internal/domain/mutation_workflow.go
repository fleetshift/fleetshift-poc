package domain

import (
	"errors"
	"fmt"
	"time"
)

// MutationResult is the outcome of a mutation activity: the
// deployment view snapshot after mutation, the fulfillment ID, and
// the generation it wrote.
type MutationResult struct {
	View          DeploymentView
	FulfillmentID FulfillmentID
	MyGen         Generation
}

const convergencePollInterval = 500 * time.Millisecond

// convergenceLoop ensures that orchestration eventually reconciles (or
// supersedes) the generation written by a mutation workflow. It is
// shared by all mutation workflows.
func convergenceLoop(
	record Record,
	orchestration OrchestrationWorkflow,
	loadFulfillment Activity[FulfillmentID, *Fulfillment],
	fulfillmentID FulfillmentID,
	myGen Generation,
	missingMeansDone bool,
) error {
	for {
		f, err := RunActivity(record, loadFulfillment, fulfillmentID)
		if err != nil {
			return fmt.Errorf("load fulfillment for convergence: %w", err)
		}
		if f == nil && missingMeansDone {
			// Successful delete; done
			return nil
		}
		if f == nil {
			return fmt.Errorf("fulfillment %q: %w", fulfillmentID, ErrNotFound)
		}
		if f.ObservedGeneration >= myGen {
			// Reconciled already to at least this gen; done
			return nil
		}
		if f.Generation > myGen {
			// Something else updated, let that convergence loop take over
			return nil
		}

		_, err = orchestration.Start(record.Context(), fulfillmentID)
		if err != nil && !errors.Is(err, ErrAlreadyRunning) {
			return fmt.Errorf("start orchestration: %w", err)
		}
		if err == nil {
			// Succesfully started
			return nil
		}

		// Already running–lock finalization race. Wait for the other workflow to complete.
		if err := record.Sleep(convergencePollInterval); err != nil {
			return fmt.Errorf("durable sleep: %w", err)
		}
	}
}
