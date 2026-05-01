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
func (r *stubRecord) Sleep(_ time.Duration) error {
	return nil
}

// fakeOrchestrationWorkflow records the fulfillment ID it was started with.
type fakeOrchestrationWorkflow struct {
	started domain.FulfillmentID
}

func (f *fakeOrchestrationWorkflow) Start(_ context.Context, fulfillmentID domain.FulfillmentID) (domain.Execution[struct{}], error) {
	f.started = fulfillmentID
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

	if dep.Deployment.ID != "d1" {
		t.Errorf("Deployment.ID = %q, want %q", dep.Deployment.ID, "d1")
	}
	if dep.Fulfillment.State != domain.FulfillmentStateCreating {
		t.Errorf("Fulfillment.State = %q, want %q", dep.Fulfillment.State, domain.FulfillmentStateCreating)
	}
	if dep.Deployment.UID == "" {
		t.Error("Deployment.UID is empty, want non-empty UUID")
	}
	if dep.Deployment.CreatedAt.IsZero() {
		t.Error("Deployment.CreatedAt is zero, want non-zero")
	}
	if !dep.Deployment.CreatedAt.Equal(fixedTime) {
		t.Errorf("Deployment.CreatedAt = %v, want %v", dep.Deployment.CreatedAt, fixedTime)
	}
	if !dep.Deployment.UpdatedAt.Equal(fixedTime) {
		t.Errorf("Deployment.UpdatedAt = %v, want %v", dep.Deployment.UpdatedAt, fixedTime)
	}
	if dep.Deployment.Etag == "" {
		t.Error("Deployment.Etag is empty, want non-empty")
	}

	persisted := getThinDeployment(t, store, "d1")
	if persisted.ID != "d1" {
		t.Errorf("persisted ID = %q, want %q", persisted.ID, "d1")
	}

	if fakeOrch.started != dep.Deployment.FulfillmentID {
		t.Errorf("Orchestration.Start called with %q, want %q", fakeOrch.started, dep.Deployment.FulfillmentID)
	}
}
