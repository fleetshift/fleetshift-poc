package domain_test

import (
	"context"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

type stubCreateDeploymentRunner struct {
	ctx                context.Context
	orchestrationStart domain.DeploymentID
}

func (r *stubCreateDeploymentRunner) ID() string              { return "create-test" }
func (r *stubCreateDeploymentRunner) Context() context.Context { return r.ctx }
func (r *stubCreateDeploymentRunner) Run(activity domain.Activity[any, any], in any) (any, error) {
	return activity.Run(r.ctx, in)
}
func (r *stubCreateDeploymentRunner) StartOrchestration(id domain.DeploymentID) error {
	r.orchestrationStart = id
	return nil
}

func TestCreateDeploymentWorkflow_PersistsThenStartsOrchestration(t *testing.T) {
	depRepo := &stubDeploymentRepo{}
	fixedTime := time.Date(2026, 3, 2, 12, 0, 0, 0, time.UTC)

	wf := &domain.CreateDeploymentWorkflow{
		Deployments: depRepo,
		Now:         func() time.Time { return fixedTime },
	}

	ctx := context.Background()
	runner := &stubCreateDeploymentRunner{ctx: ctx}

	input := domain.CreateDeploymentInput{
		ID: "d1",
		ManifestStrategy: domain.ManifestStrategySpec{
			Type: domain.ManifestStrategyInline,
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type: domain.PlacementStrategyAll,
		},
	}

	dep, err := wf.Run(runner, input)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if dep.ID != "d1" {
		t.Errorf("Deployment.ID = %q, want %q", dep.ID, "d1")
	}
	if dep.State != domain.DeploymentStateCreating {
		t.Errorf("Deployment.State = %q, want %q", dep.State, domain.DeploymentStateCreating)
	}
	if dep.UID == "" {
		t.Error("Deployment.UID is empty, want non-empty UUID")
	}
	if dep.CreatedAt.IsZero() {
		t.Error("Deployment.CreatedAt is zero, want non-zero")
	}
	if !dep.CreatedAt.Equal(fixedTime) {
		t.Errorf("Deployment.CreatedAt = %v, want %v", dep.CreatedAt, fixedTime)
	}
	if !dep.UpdatedAt.Equal(fixedTime) {
		t.Errorf("Deployment.UpdatedAt = %v, want %v", dep.UpdatedAt, fixedTime)
	}
	if dep.Etag == "" {
		t.Error("Deployment.Etag is empty, want non-empty")
	}

	persisted, err := depRepo.Get(ctx, "d1")
	if err != nil {
		t.Fatalf("Get(d1) after persist: %v", err)
	}
	if persisted.ID != "d1" {
		t.Errorf("persisted ID = %q, want %q", persisted.ID, "d1")
	}

	if runner.orchestrationStart != "d1" {
		t.Errorf("StartOrchestration called with %q, want %q", runner.orchestrationStart, "d1")
	}
}
