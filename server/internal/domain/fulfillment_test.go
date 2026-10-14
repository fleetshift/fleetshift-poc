package domain

import (
	"errors"
	"testing"
	"time"
)

func TestFulfillment_AdvanceManifestStrategy(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	f := FulfillmentFromSnapshot(FulfillmentSnapshot{
		ID:         "f1",
		Generation: 1,
	})
	spec := ManifestStrategySpec{Type: ManifestStrategyInline}

	f.AdvanceManifestStrategy(spec, now)

	if f.manifestStrategyVersion != 1 {
		t.Errorf("ManifestStrategyVersion = %d, want 1", f.manifestStrategyVersion)
	}
	if f.manifestStrategy.Type != ManifestStrategyInline {
		t.Errorf("ManifestStrategy.Type = %q, want %q", f.manifestStrategy.Type, ManifestStrategyInline)
	}
	if f.generation != 2 {
		t.Errorf("Generation = %d, want 2", f.generation)
	}

	pending := f.Snapshot().PendingStrategyRecords
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

	// Snapshot captures pending records without clearing the aggregate buffers.
	if snap := f.Snapshot(); len(snap.PendingStrategyRecords.Manifest) != 1 {
		t.Errorf("snapshot pending manifest = %d, want 1", len(snap.PendingStrategyRecords.Manifest))
	}
}

func TestFulfillment_AdvancePlacementStrategy(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	f := FulfillmentFromSnapshot(FulfillmentSnapshot{
		ID:         "f1",
		Generation: 1,
	})
	spec := PlacementStrategySpec{Type: PlacementStrategyAll}

	f.AdvancePlacementStrategy(spec, now)

	if f.placementStrategyVersion != 1 {
		t.Errorf("PlacementStrategyVersion = %d, want 1", f.placementStrategyVersion)
	}
	if f.placementStrategy.Type != PlacementStrategyAll {
		t.Errorf("PlacementStrategy.Type = %q, want %q", f.placementStrategy.Type, PlacementStrategyAll)
	}
	if f.generation != 2 {
		t.Errorf("Generation = %d, want 2", f.generation)
	}

	pending := f.Snapshot().PendingStrategyRecords
	if len(pending.Placement) != 1 {
		t.Fatalf("pending.Placement len = %d, want 1", len(pending.Placement))
	}
}

func TestFulfillment_AdvanceRolloutStrategy(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	f := FulfillmentFromSnapshot(FulfillmentSnapshot{
		ID:         "f1",
		Generation: 1,
	})
	spec := &RolloutStrategySpec{Type: RolloutStrategyImmediate}

	f.AdvanceRolloutStrategy(spec, now)

	if f.rolloutStrategyVersion != 1 {
		t.Errorf("RolloutStrategyVersion = %d, want 1", f.rolloutStrategyVersion)
	}
	if f.rolloutStrategy == nil || f.rolloutStrategy.Type != RolloutStrategyImmediate {
		t.Errorf("RolloutStrategy.Type = %v, want %q", f.rolloutStrategy, RolloutStrategyImmediate)
	}
	if f.generation != 2 {
		t.Errorf("Generation = %d, want 2", f.generation)
	}

	pending := f.Snapshot().PendingStrategyRecords
	if len(pending.Rollout) != 1 {
		t.Fatalf("pending.Rollout len = %d, want 1", len(pending.Rollout))
	}
}

func TestFulfillment_MultipleAdvances_AccumulatePending(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	f := FulfillmentFromSnapshot(FulfillmentSnapshot{ID: "f1", Generation: 0})

	f.AdvanceManifestStrategy(ManifestStrategySpec{Type: ManifestStrategyInline}, now)
	f.AdvancePlacementStrategy(PlacementStrategySpec{Type: PlacementStrategyAll}, now)
	f.AdvanceRolloutStrategy(nil, now)

	// All three advances happen in the same "transaction" (loadedGeneration=0),
	// so generation advances exactly once to 1 regardless of how many calls.
	if f.generation != 1 {
		t.Errorf("Generation = %d, want 1", f.generation)
	}
	if f.manifestStrategyVersion != 1 {
		t.Errorf("ManifestStrategyVersion = %d, want 1", f.manifestStrategyVersion)
	}
	if f.placementStrategyVersion != 1 {
		t.Errorf("PlacementStrategyVersion = %d, want 1", f.placementStrategyVersion)
	}
	if f.rolloutStrategyVersion != 1 {
		t.Errorf("RolloutStrategyVersion = %d, want 1", f.rolloutStrategyVersion)
	}

	pending := f.Snapshot().PendingStrategyRecords
	if len(pending.Manifest) != 1 || len(pending.Placement) != 1 || len(pending.Rollout) != 1 {
		t.Errorf("pending counts: manifest=%d, placement=%d, rollout=%d; want 1,1,1",
			len(pending.Manifest), len(pending.Placement), len(pending.Rollout))
	}
}

func TestFulfillment_AcquireOrchestrationLock(t *testing.T) {
	f := FulfillmentFromSnapshot(FulfillmentSnapshot{Generation: 5})

	if !f.AcquireOrchestrationLock() {
		t.Error("first AcquireOrchestrationLock returned false, want true")
	}
	if f.activeWorkflowGen == nil || *f.activeWorkflowGen != 5 {
		t.Errorf("ActiveWorkflowGen = %v, want 5", f.activeWorkflowGen)
	}

	if f.AcquireOrchestrationLock() {
		t.Error("second AcquireOrchestrationLock returned true, want false")
	}
}

func TestFulfillment_ReleaseOrchestrationLock(t *testing.T) {
	f := FulfillmentFromSnapshot(FulfillmentSnapshot{Generation: 5})
	f.AcquireOrchestrationLock()

	f.ReleaseOrchestrationLock()

	if f.activeWorkflowGen != nil {
		t.Errorf("ActiveWorkflowGen = %v, want nil", f.activeWorkflowGen)
	}
}

func TestFulfillment_CompleteReconciliation_Converged(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	f := FulfillmentFromSnapshot(FulfillmentSnapshot{Generation: 3})
	f.AcquireOrchestrationLock()

	needsRestart := f.CompleteReconciliation(3, now)

	if needsRestart {
		t.Error("needsRestart = true, want false")
	}
	if f.observedGeneration != 3 {
		t.Errorf("ObservedGeneration = %d, want 3", f.observedGeneration)
	}
	if f.activeWorkflowGen != nil {
		t.Errorf("ActiveWorkflowGen = %v, want nil (lock cleared)", f.activeWorkflowGen)
	}
	if f.updatedAt != now {
		t.Errorf("UpdatedAt = %v, want %v", f.updatedAt, now)
	}
}

func TestFulfillment_CompleteReconciliation_NeedsRestart(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	f := FulfillmentFromSnapshot(FulfillmentSnapshot{Generation: 5})
	f.AcquireOrchestrationLock()

	needsRestart := f.CompleteReconciliation(3, now)

	if !needsRestart {
		t.Error("needsRestart = false, want true")
	}
	if f.observedGeneration != 3 {
		t.Errorf("ObservedGeneration = %d, want 3", f.observedGeneration)
	}
	if f.activeWorkflowGen == nil {
		t.Error("ActiveWorkflowGen = nil, want non-nil (lock kept)")
	}
	if f.updatedAt != now {
		t.Errorf("UpdatedAt = %v, want %v", f.updatedAt, now)
	}
}

func TestFulfillment_Resume_RejectsNonPausedState(t *testing.T) {
	for _, state := range []FulfillmentState{
		FulfillmentStateCreating,
		FulfillmentStateActive,
		FulfillmentStateDeleting,
		FulfillmentStateFailed,
	} {
		t.Run(string(state), func(t *testing.T) {
			f := FulfillmentFromSnapshot(FulfillmentSnapshot{
				ID:         "f1",
				State:      state,
				Generation: 3,
			})
			err := f.Resume(DeliveryAuth{Token: "new-token"}, nil)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("expected ErrInvalidArgument, got: %v", err)
			}
			if f.generation != 3 {
				t.Error("generation should not change on error")
			}
		})
	}
}

func TestFulfillment_Resume_RequiresProvenanceWhenPreviouslyPresent(t *testing.T) {
	// TODO: revisit this behavior; it might be fine to resume without provenance depending on the desired auth
	f := FulfillmentFromSnapshot(FulfillmentSnapshot{
		ID:          "f1",
		State:       FulfillmentStateActive,
		PauseReason: "delivery auth failed",
		Generation:  3,
		Provenance: &Provenance{
			Sig:                Signature{Signer: FederatedIdentity{Subject: "u1", Issuer: "iss"}},
			ExpectedGeneration: 3,
		},
	})
	err := f.Resume(DeliveryAuth{Token: "new-token"}, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument, got: %v", err)
	}
	if f.generation != 3 {
		t.Error("generation should not change on error")
	}
}

func TestFulfillment_Resume_HappyPath_NoProvenance(t *testing.T) {
	f := FulfillmentFromSnapshot(FulfillmentSnapshot{
		ID:          "f1",
		State:       FulfillmentStateActive,
		PauseReason: "delivery auth failed",
		Generation:  3,
		Auth:        DeliveryAuth{Token: "old-token"},
	})

	newAuth := DeliveryAuth{Token: "fresh-token"}
	err := f.Resume(newAuth, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f.state != FulfillmentStateActive {
		t.Errorf("State = %q, want %q", f.state, FulfillmentStateActive)
	}
	if f.auth.Token != "fresh-token" {
		t.Errorf("Auth.Token = %q, want %q", f.auth.Token, "fresh-token")
	}
	if f.generation != 4 {
		t.Errorf("Generation = %d, want 4", f.generation)
	}
	if f.provenance != nil {
		t.Error("Provenance should remain nil")
	}
}

func TestFulfillment_Resume_HappyPath_WithProvenance(t *testing.T) {
	f := FulfillmentFromSnapshot(FulfillmentSnapshot{
		ID:          "f1",
		State:       FulfillmentStateActive,
		PauseReason: "delivery auth failed",
		Generation:  5,
		Auth:        DeliveryAuth{Token: "old-token"},
		Provenance: &Provenance{
			Sig:                Signature{Signer: FederatedIdentity{Subject: "u1", Issuer: "iss"}},
			ExpectedGeneration: 5,
		},
	})

	newAuth := DeliveryAuth{Token: "fresh-token"}
	newProv := &Provenance{
		Sig:                Signature{Signer: FederatedIdentity{Subject: "u1", Issuer: "iss"}},
		ExpectedGeneration: 6,
	}
	err := f.Resume(newAuth, newProv)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f.state != FulfillmentStateActive {
		t.Errorf("State = %q, want %q", f.state, FulfillmentStateActive)
	}
	if f.auth.Token != "fresh-token" {
		t.Errorf("Auth.Token = %q, want %q", f.auth.Token, "fresh-token")
	}
	if f.generation != 6 {
		t.Errorf("Generation = %d, want 6", f.generation)
	}
	if f.provenance != newProv {
		t.Error("Provenance should be updated to new provenance")
	}
}

func TestFulfillment_Resume_AcceptsProvenanceWhenNonePreviously(t *testing.T) {
	f := FulfillmentFromSnapshot(FulfillmentSnapshot{
		ID:          "f1",
		State:       FulfillmentStateActive,
		PauseReason: "delivery auth failed",
		Generation:  2,
		Auth:        DeliveryAuth{Token: "old-token"},
	})

	newProv := &Provenance{
		Sig:                Signature{Signer: FederatedIdentity{Subject: "u1", Issuer: "iss"}},
		ExpectedGeneration: 3,
	}
	err := f.Resume(DeliveryAuth{Token: "fresh-token"}, newProv)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f.provenance != newProv {
		t.Error("Provenance should be set even when previously nil")
	}
	if f.generation != 3 {
		t.Errorf("Generation = %d, want 3", f.generation)
	}
}

func TestFulfillment_TransitionToDeleting_ClearsPauseReason(t *testing.T) {
	tests := []struct {
		name       string
		startState FulfillmentState
	}{
		{"from active (first delete)", FulfillmentStateActive},
		{"from deleting (retry delete)", FulfillmentStateDeleting},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := FulfillmentFromSnapshot(FulfillmentSnapshot{
				ID:          "f1",
				State:       tt.startState,
				PauseReason: "delivery auth failed: broker auth exchange: caller token is empty",
				Generation:  3,
				Auth:        DeliveryAuth{Token: "old-token"},
			})

			f.TransitionToDeleting(DeliveryAuth{Token: "fresh-token"})

			if f.Paused() {
				t.Error("expected Paused() to be false after TransitionToDeleting")
			}
			if f.pauseReason != "" {
				t.Errorf("pauseReason = %q, want empty", f.pauseReason)
			}
			if f.state != FulfillmentStateDeleting {
				t.Errorf("State = %q, want %q", f.state, FulfillmentStateDeleting)
			}
			if f.auth.Token != "fresh-token" {
				t.Errorf("Auth.Token = %q, want %q", f.auth.Token, "fresh-token")
			}
			if f.generation != 4 {
				t.Errorf("Generation = %d, want 4", f.generation)
			}
		})
	}
}

func TestFulfillment_Reconciling(t *testing.T) {
	tests := []struct {
		name        string
		state       FulfillmentState
		pauseReason string
		want        bool
	}{
		{"creating, not paused", FulfillmentStateCreating, "", true},
		{"deleting, not paused", FulfillmentStateDeleting, "", true},
		{"creating, paused", FulfillmentStateCreating, "credential rotation required", false},
		{"deleting, paused", FulfillmentStateDeleting, "credential rotation required", false},
		{"active, not paused", FulfillmentStateActive, "", false},
		{"active, paused", FulfillmentStateActive, "paused", false},
		{"failed, not paused", FulfillmentStateFailed, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := FulfillmentFromSnapshot(FulfillmentSnapshot{
				ID:          "f1",
				State:       tt.state,
				PauseReason: tt.pauseReason,
			})
			if got := f.Reconciling(); got != tt.want {
				t.Errorf("Reconciling() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFulfillment_ApplyReconciliationResult(t *testing.T) {
	f := FulfillmentFromSnapshot(FulfillmentSnapshot{
		ID:         "f1",
		Generation: 3,
		State:      FulfillmentStateCreating,
	})
	result := ReconciliationResult{
		FulfillmentID:   "f1",
		State:           FulfillmentStateActive,
		ResolvedTargets: []TargetID{"t1", "t2"},
		Auth:            DeliveryAuth{},
	}

	f.ApplyReconciliationResult(result)

	if f.state != FulfillmentStateActive {
		t.Errorf("State = %q, want %q", f.state, FulfillmentStateActive)
	}
	if len(f.resolvedTargets) != 2 {
		t.Errorf("ResolvedTargets len = %d, want 2", len(f.resolvedTargets))
	}
	if f.generation != 3 {
		t.Error("Generation changed; should be untouched by ApplyReconciliationResult")
	}
}
