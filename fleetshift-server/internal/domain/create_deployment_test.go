package domain_test

import (
	"context"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// stubRecord is a minimal Record that runs activities synchronously.
type stubRecord struct {
	ctx context.Context
}

func (r *stubRecord) ID() string               { return "create-test" }
func (r *stubRecord) Context() context.Context { return r.ctx }
func (r *stubRecord) Run(activity domain.Activity[any, any], in any) (any, error) {
	return activity.Run(r.ctx, in)
}
func (r *stubRecord) Await(_ string) (any, error) {
	return nil, nil
}

// fakeOrchestrationWorkflow records the deployment ID it was started with.
type fakeOrchestrationWorkflow struct {
	started domain.DeploymentID
}

func (f *fakeOrchestrationWorkflow) Start(_ context.Context, deploymentID domain.DeploymentID) (domain.Execution[struct{}], error) {
	f.started = deploymentID
	return &immediateExecution[struct{}]{}, nil
}

type immediateExecution[T any] struct {
	val T
}

func (e *immediateExecution[T]) WorkflowID() string                       { return "fake" }
func (e *immediateExecution[T]) AwaitResult(_ context.Context) (T, error) { return e.val, nil }

func TestCreateDeploymentWorkflow_PersistsThenStartsOrchestration(t *testing.T) {
	store, _ := setupStore(t)
	fixedTime := time.Date(2026, 3, 2, 12, 0, 0, 0, time.UTC)

	fakeOrch := &fakeOrchestrationWorkflow{}

	wf := &domain.CreateDeploymentWorkflowSpec{
		Store:         store,
		Orchestration: fakeOrch,
		Now:           func() time.Time { return fixedTime },
	}

	ctx := context.Background()
	rec := &stubRecord{ctx: ctx}

	input := domain.CreateDeploymentInput{
		ID: "d1",
		ManifestStrategy: domain.ManifestStrategySpec{
			Type: domain.ManifestStrategyInline,
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type: domain.PlacementStrategyAll,
		},
	}

	dep, err := wf.Run(rec, input)
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

	persisted := getDeployment(t, store, "d1")
	if persisted.ID != "d1" {
		t.Errorf("persisted ID = %q, want %q", persisted.ID, "d1")
	}

	if fakeOrch.started != "d1" {
		t.Errorf("Orchestration.Start called with %q, want %q", fakeOrch.started, "d1")
	}
}
