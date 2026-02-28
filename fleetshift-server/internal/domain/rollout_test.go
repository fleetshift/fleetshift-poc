package domain_test

import (
	"context"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestImmediateRollout_AllTargetsInOneBatch(t *testing.T) {
	r := &domain.ImmediateRollout{}
	delta := domain.TargetDelta{
		Added: []domain.TargetInfo{
			{ID: "t1"},
			{ID: "t2"},
			{ID: "t3"},
		},
	}
	plan, err := r.Plan(context.Background(), delta)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(plan.Batches))
	}
	if len(plan.Batches[0].Targets) != 3 {
		t.Fatalf("expected 3 targets in batch, got %d", len(plan.Batches[0].Targets))
	}
}

func TestImmediateRollout_IncludesUnchanged(t *testing.T) {
	r := &domain.ImmediateRollout{}
	delta := domain.TargetDelta{
		Added:     []domain.TargetInfo{{ID: "t1"}},
		Unchanged: []domain.TargetInfo{{ID: "t2"}},
	}
	plan, err := r.Plan(context.Background(), delta)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Batches[0].Targets) != 2 {
		t.Fatalf("expected 2 targets (added+unchanged), got %d", len(plan.Batches[0].Targets))
	}
}

func TestImmediateRollout_EmptyDelta(t *testing.T) {
	r := &domain.ImmediateRollout{}
	plan, err := r.Plan(context.Background(), domain.TargetDelta{})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Batches) != 0 {
		t.Fatalf("expected 0 batches for empty delta, got %d", len(plan.Batches))
	}
}
