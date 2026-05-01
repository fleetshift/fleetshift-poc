package domain

import (
	"testing"
	"time"
)

func TestFulfillment_AdvanceManifestStrategy(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	f := Fulfillment{
		ID:         "f1",
		Generation: 1,
	}
	spec := ManifestStrategySpec{Type: ManifestStrategyInline}

	f.AdvanceManifestStrategy(spec, now)

	if f.ManifestStrategyVersion != 1 {
		t.Errorf("ManifestStrategyVersion = %d, want 1", f.ManifestStrategyVersion)
	}
	if f.ManifestStrategy.Type != ManifestStrategyInline {
		t.Errorf("ManifestStrategy.Type = %q, want %q", f.ManifestStrategy.Type, ManifestStrategyInline)
	}
	if f.Generation != 2 {
		t.Errorf("Generation = %d, want 2", f.Generation)
	}

	pending := f.DrainPendingStrategyRecords()
	if len(pending.Manifest) != 1 {
		t.Fatalf("pending.Manifest len = %d, want 1", len(pending.Manifest))
	}
	rec := pending.Manifest[0]
	if rec.FulfillmentID != "f1" {
		t.Errorf("record.FulfillmentID = %q, want %q", rec.FulfillmentID, "f1")
	}
	if rec.Version != 1 {
		t.Errorf("record.Version = %d, want 1", rec.Version)
	}
	if !rec.CreatedAt.Equal(now) {
		t.Errorf("record.CreatedAt = %v, want %v", rec.CreatedAt, now)
	}

	// Drain again should be empty.
	if p := f.DrainPendingStrategyRecords(); len(p.Manifest) != 0 {
		t.Errorf("second drain: got %d manifest records, want 0", len(p.Manifest))
	}
}

func TestFulfillment_AdvancePlacementStrategy(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	f := Fulfillment{
		ID:         "f1",
		Generation: 1,
	}
	spec := PlacementStrategySpec{Type: PlacementStrategyAll}

	f.AdvancePlacementStrategy(spec, now)

	if f.PlacementStrategyVersion != 1 {
		t.Errorf("PlacementStrategyVersion = %d, want 1", f.PlacementStrategyVersion)
	}
	if f.PlacementStrategy.Type != PlacementStrategyAll {
		t.Errorf("PlacementStrategy.Type = %q, want %q", f.PlacementStrategy.Type, PlacementStrategyAll)
	}
	if f.Generation != 2 {
		t.Errorf("Generation = %d, want 2", f.Generation)
	}

	pending := f.DrainPendingStrategyRecords()
	if len(pending.Placement) != 1 {
		t.Fatalf("pending.Placement len = %d, want 1", len(pending.Placement))
	}
}

func TestFulfillment_AdvanceRolloutStrategy(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	f := Fulfillment{
		ID:         "f1",
		Generation: 1,
	}
	spec := &RolloutStrategySpec{Type: RolloutStrategyImmediate}

	f.AdvanceRolloutStrategy(spec, now)

	if f.RolloutStrategyVersion != 1 {
		t.Errorf("RolloutStrategyVersion = %d, want 1", f.RolloutStrategyVersion)
	}
	if f.RolloutStrategy == nil || f.RolloutStrategy.Type != RolloutStrategyImmediate {
		t.Errorf("RolloutStrategy.Type = %v, want %q", f.RolloutStrategy, RolloutStrategyImmediate)
	}
	if f.Generation != 2 {
		t.Errorf("Generation = %d, want 2", f.Generation)
	}

	pending := f.DrainPendingStrategyRecords()
	if len(pending.Rollout) != 1 {
		t.Fatalf("pending.Rollout len = %d, want 1", len(pending.Rollout))
	}
}

func TestFulfillment_MultipleAdvances_AccumulatePending(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	f := Fulfillment{ID: "f1", Generation: 0}

	f.AdvanceManifestStrategy(ManifestStrategySpec{Type: ManifestStrategyInline}, now)
	f.AdvancePlacementStrategy(PlacementStrategySpec{Type: PlacementStrategyAll}, now)
	f.AdvanceRolloutStrategy(nil, now)

	// Three advances = 3 generation bumps (0 -> 1 -> 2 -> 3).
	if f.Generation != 3 {
		t.Errorf("Generation = %d, want 3", f.Generation)
	}
	if f.ManifestStrategyVersion != 1 {
		t.Errorf("ManifestStrategyVersion = %d, want 1", f.ManifestStrategyVersion)
	}
	if f.PlacementStrategyVersion != 1 {
		t.Errorf("PlacementStrategyVersion = %d, want 1", f.PlacementStrategyVersion)
	}
	if f.RolloutStrategyVersion != 1 {
		t.Errorf("RolloutStrategyVersion = %d, want 1", f.RolloutStrategyVersion)
	}

	pending := f.DrainPendingStrategyRecords()
	if len(pending.Manifest) != 1 || len(pending.Placement) != 1 || len(pending.Rollout) != 1 {
		t.Errorf("pending counts: manifest=%d, placement=%d, rollout=%d; want 1,1,1",
			len(pending.Manifest), len(pending.Placement), len(pending.Rollout))
	}
}

func TestFulfillment_AcquireOrchestrationLock(t *testing.T) {
	f := Fulfillment{Generation: 5}

	if !f.AcquireOrchestrationLock() {
		t.Error("first AcquireOrchestrationLock returned false, want true")
	}
	if f.ActiveWorkflowGen == nil || *f.ActiveWorkflowGen != 5 {
		t.Errorf("ActiveWorkflowGen = %v, want 5", f.ActiveWorkflowGen)
	}

	if f.AcquireOrchestrationLock() {
		t.Error("second AcquireOrchestrationLock returned true, want false")
	}
}

func TestFulfillment_ReleaseOrchestrationLock(t *testing.T) {
	f := Fulfillment{Generation: 5}
	f.AcquireOrchestrationLock()

	f.ReleaseOrchestrationLock()

	if f.ActiveWorkflowGen != nil {
		t.Errorf("ActiveWorkflowGen = %v, want nil", f.ActiveWorkflowGen)
	}
}

func TestFulfillment_CompleteReconciliation_Converged(t *testing.T) {
	f := Fulfillment{Generation: 3}
	f.AcquireOrchestrationLock()

	needsRestart := f.CompleteReconciliation(3)

	if needsRestart {
		t.Error("needsRestart = true, want false")
	}
	if f.ObservedGeneration != 3 {
		t.Errorf("ObservedGeneration = %d, want 3", f.ObservedGeneration)
	}
	if f.ActiveWorkflowGen != nil {
		t.Errorf("ActiveWorkflowGen = %v, want nil (lock cleared)", f.ActiveWorkflowGen)
	}
}

func TestFulfillment_CompleteReconciliation_NeedsRestart(t *testing.T) {
	f := Fulfillment{Generation: 5}
	f.AcquireOrchestrationLock()

	needsRestart := f.CompleteReconciliation(3)

	if !needsRestart {
		t.Error("needsRestart = false, want true")
	}
	if f.ObservedGeneration != 3 {
		t.Errorf("ObservedGeneration = %d, want 3", f.ObservedGeneration)
	}
	if f.ActiveWorkflowGen == nil {
		t.Error("ActiveWorkflowGen = nil, want non-nil (lock kept)")
	}
}

func TestFulfillment_ApplyReconciliationResult(t *testing.T) {
	f := Fulfillment{
		ID:         "f1",
		Generation: 3,
		State:      FulfillmentStateCreating,
	}
	result := ReconciliationResult{
		FulfillmentID:   "f1",
		State:           FulfillmentStateActive,
		ResolvedTargets: []TargetID{"t1", "t2"},
		Auth:            DeliveryAuth{},
	}

	f.ApplyReconciliationResult(result)

	if f.State != FulfillmentStateActive {
		t.Errorf("State = %q, want %q", f.State, FulfillmentStateActive)
	}
	if len(f.ResolvedTargets) != 2 {
		t.Errorf("ResolvedTargets len = %d, want 2", len(f.ResolvedTargets))
	}
	if f.Generation != 3 {
		t.Error("Generation changed; should be untouched by ApplyReconciliationResult")
	}
}
