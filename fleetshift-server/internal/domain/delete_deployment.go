package domain

import (
	"context"
	"errors"
	"fmt"
)

// DeleteDeploymentWorkflowSpec transitions a deployment to
// [DeploymentStateDeleting], bumps its generation, and runs a
// convergence loop to guarantee orchestration picks up the new state.
//
// Pass this spec to [Registry.RegisterDeleteDeployment] to obtain a
// [DeleteDeploymentWorkflow] that can start instances.
type DeleteDeploymentWorkflowSpec struct {
	Store         Store
	Orchestration OrchestrationWorkflow
}

func (s *DeleteDeploymentWorkflowSpec) Name() string { return "delete-deployment" }

// MutateToDeleting transitions the deployment to [DeploymentStateDeleting]
// and bumps its generation inside a serialized write transaction.
//
// TODO: move delete transition rules onto Deployment so other mutations
// cannot accidentally clear Deleting and effectively "undelete" later.
func (s *DeleteDeploymentWorkflowSpec) MutateToDeleting() Activity[DeploymentID, MutationResult] {
	return NewActivity("mutate-to-deleting", func(ctx context.Context, id DeploymentID) (MutationResult, error) {
		tx, err := s.Store.Begin(ctx)
		if err != nil {
			return MutationResult{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		dep, err := tx.Deployments().Get(ctx, id)
		if err != nil {
			return MutationResult{}, err
		}
		dep.State = DeploymentStateDeleting
		dep.BumpGeneration()
		if err := tx.Deployments().Update(ctx, dep); err != nil {
			return MutationResult{}, fmt.Errorf("update deployment: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return MutationResult{}, fmt.Errorf("commit: %w", err)
		}
		return MutationResult{Deployment: dep, MyGen: dep.Generation}, nil
	})
}

// LoadDeployment reads the current deployment state for convergence checks.
func (s *DeleteDeploymentWorkflowSpec) LoadDeployment() Activity[DeploymentID, *Deployment] {
	return NewActivity("load-deployment-for-delete", func(ctx context.Context, id DeploymentID) (*Deployment, error) {
		tx, err := s.Store.BeginReadOnly(ctx)
		if err != nil {
			return nil, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		dep, err := tx.Deployments().Get(ctx, id)
		if errors.Is(err, ErrNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return &dep, tx.Commit()
	})
}

// Run is the workflow body: mutate, then run the convergence-start
// loop. [ErrNotFound] during convergence is terminal success (the
// delete completed while waiting).
func (s *DeleteDeploymentWorkflowSpec) Run(record Record, deploymentID DeploymentID) (Deployment, error) {
	mr, err := RunActivity(record, s.MutateToDeleting(), deploymentID)
	if err != nil {
		return Deployment{}, fmt.Errorf("mutate to deleting: %w", err)
	}

	if err := convergenceLoop(record, s.Orchestration, s.LoadDeployment(), deploymentID, mr.MyGen, true); err != nil {
		return Deployment{}, err
	}

	return mr.Deployment, nil
}
