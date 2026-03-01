package application

import (
	"context"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// OrchestrationService executes the deployment pipeline as a durable workflow.
type OrchestrationService struct {
	Workflow domain.OrchestrationRunner
}

// Orchestrate starts the deployment pipeline workflow and waits for
// it to complete.
func (o *OrchestrationService) Orchestrate(ctx context.Context, deploymentID domain.DeploymentID) error {
	handle, err := o.Workflow.Run(ctx, deploymentID)
	if err != nil {
		return fmt.Errorf("start orchestration workflow: %w", err)
	}
	_, err = handle.AwaitResult(ctx)
	return err
}
