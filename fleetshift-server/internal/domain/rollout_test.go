package domain_test

import (
	"context"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestImmediateRollout_EmitsRemoveThenDeliverSteps(t *testing.T) {
	r := &domain.ImmediateRollout{}
	delta := domain.TargetDelta{
		Removed:   []domain.TargetInfo{{ID: "gone"}},
		Added:    []domain.TargetInfo{{ID: "t1"}, {ID: "t2"}},
		Unchanged: []domain.TargetInfo{{ID: "t3"}},
	}
	plan, err := r.Plan(context.Background(), delta)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 2 {
		t.Fatalf("expected 2 steps (remove, deliver), got %d", len(plan.Steps))
	}
	if plan.Steps[0].Remove == nil {
		t.Fatal("first step should be remove")
	}
	if len(plan.Steps[0].Remove.Targets) != 1 || plan.Steps[0].Remove.Targets[0].ID != "gone" {
		t.Fatalf("remove step should have one target gone, got %v", plan.Steps[0].Remove.Targets)
	}
	if plan.Steps[1].Deliver == nil {
		t.Fatal("second step should be deliver")
	}
	if len(plan.Steps[1].Deliver.Targets) != 3 {
		t.Fatalf("deliver step should have 3 targets (added+unchanged), got %d", len(plan.Steps[1].Deliver.Targets))
	}
}

func TestImmediateRollout_OnlyAddedAndUnchangedInDeliverStep(t *testing.T) {
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
	if len(plan.Steps) != 1 {
		t.Fatalf("expected 1 step (deliver only), got %d", len(plan.Steps))
	}
	if plan.Steps[0].Deliver == nil {
		t.Fatal("only step should be deliver")
	}
	if len(plan.Steps[0].Deliver.Targets) != 3 {
		t.Fatalf("expected 3 targets in deliver step, got %d", len(plan.Steps[0].Deliver.Targets))
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
	if len(plan.Steps) != 1 || plan.Steps[0].Deliver == nil {
		t.Fatal("expected single deliver step")
	}
	if len(plan.Steps[0].Deliver.Targets) != 2 {
		t.Fatalf("expected 2 targets (added+unchanged), got %d", len(plan.Steps[0].Deliver.Targets))
	}
}

func TestImmediateRollout_OnlyRemoved(t *testing.T) {
	r := &domain.ImmediateRollout{}
	delta := domain.TargetDelta{
		Removed: []domain.TargetInfo{{ID: "r1"}, {ID: "r2"}},
	}
	plan, err := r.Plan(context.Background(), delta)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 1 {
		t.Fatalf("expected 1 step (remove only), got %d", len(plan.Steps))
	}
	if plan.Steps[0].Remove == nil {
		t.Fatal("only step should be remove")
	}
	if len(plan.Steps[0].Remove.Targets) != 2 {
		t.Fatalf("expected 2 targets in remove step, got %d", len(plan.Steps[0].Remove.Targets))
	}
}

func TestImmediateRollout_EmptyDelta(t *testing.T) {
	r := &domain.ImmediateRollout{}
	plan, err := r.Plan(context.Background(), domain.TargetDelta{})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 0 {
		t.Fatalf("expected 0 steps for empty delta, got %d", len(plan.Steps))
	}
}
