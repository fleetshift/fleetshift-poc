package domain

import (
	"errors"
	"fmt"
	"time"
)

// MutationResult is the outcome of a mutation activity: the
// deployment snapshot after mutation and the generation it wrote.
type MutationResult struct {
	Deployment Deployment
	MyGen      Generation
}

const convergencePollInterval = 500 * time.Millisecond

// convergenceLoop ensures that orchestration eventually reconciles (or
// supersedes) the generation written by a mutation workflow. It is
// shared by all mutation workflows.
func convergenceLoop(
	record Record,
	orchestration OrchestrationWorkflow,
	loadDeployment Activity[DeploymentID, *Deployment],
	deploymentID DeploymentID,
	myGen Generation,
	missingMeansDone bool,
) error {
	for {
		dep, err := RunActivity(record, loadDeployment, deploymentID)
		if err != nil {
			return fmt.Errorf("load deployment for convergence: %w", err)
		}
		if dep == nil && missingMeansDone {
			// Successful delete; done
			return nil
		}
		if dep == nil {
			return fmt.Errorf("deployment %q: %w", deploymentID, ErrNotFound)
		}
		if dep.ObservedGeneration >= myGen {
			// Reconciled already to at least this gen; done
			return nil
		}
		if dep.Generation > myGen {
			// Something else updated, let that convergence loop take over
			return nil
		}

		_, err = orchestration.Start(record.Context(), deploymentID)
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
