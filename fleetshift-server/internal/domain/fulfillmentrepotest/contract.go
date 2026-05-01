// Package fulfillmentrepotest provides contract tests for
// [domain.FulfillmentRepository] implementations.
package fulfillmentrepotest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Factory creates a fresh [domain.FulfillmentRepository] for each test.
type Factory func(t *testing.T) domain.FulfillmentRepository

// Run exercises the [domain.FulfillmentRepository] contract.
func Run(t *testing.T, factory Factory) {
	fixedTime := time.Date(2026, 3, 2, 12, 0, 0, 0, time.UTC)

	sampleFulfillment := func() domain.Fulfillment {
		return domain.Fulfillment{
			ID: domain.FulfillmentID("f1"),
			ManifestStrategy: domain.ManifestStrategySpec{
				Type:      domain.ManifestStrategyInline,
				Manifests: []domain.Manifest{{Raw: json.RawMessage(`{"kind":"ConfigMap"}`)}},
			},
			ManifestStrategyVersion: 1,
			PlacementStrategy: domain.PlacementStrategySpec{
				Type:    domain.PlacementStrategyStatic,
				Targets: []domain.TargetID{"t1", "t2"},
			},
			PlacementStrategyVersion: 1,
			State:                    domain.FulfillmentStateCreating,
			CreatedAt:                fixedTime,
			UpdatedAt:                fixedTime,
			Generation:               1,
			ObservedGeneration:       0,
		}
	}

	t.Run("CreateAndGet", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()
		f := sampleFulfillment()

		if err := repo.Create(ctx, f); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := repo.Get(ctx, "f1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.ManifestStrategy.Type != domain.ManifestStrategyInline {
			t.Errorf("ManifestStrategy.Type = %q, want %q", got.ManifestStrategy.Type, domain.ManifestStrategyInline)
		}
		if len(got.PlacementStrategy.Targets) != 2 {
			t.Errorf("PlacementStrategy.Targets = %d, want 2", len(got.PlacementStrategy.Targets))
		}
		if got.ManifestStrategyVersion != 1 {
			t.Errorf("ManifestStrategyVersion = %d, want 1", got.ManifestStrategyVersion)
		}
		if got.PlacementStrategyVersion != 1 {
			t.Errorf("PlacementStrategyVersion = %d, want 1", got.PlacementStrategyVersion)
		}
		if got.State != domain.FulfillmentStateCreating {
			t.Errorf("State = %q, want %q", got.State, domain.FulfillmentStateCreating)
		}
		if got.ID != "f1" {
			t.Errorf("ID = %q, want %q", got.ID, "f1")
		}
		if !got.CreatedAt.Equal(fixedTime) {
			t.Errorf("CreatedAt = %v, want %v", got.CreatedAt, fixedTime)
		}
		if !got.UpdatedAt.Equal(fixedTime) {
			t.Errorf("UpdatedAt = %v, want %v", got.UpdatedAt, fixedTime)
		}
	})

	t.Run("CreateAndGet_WithRolloutStrategy", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()
		f := sampleFulfillment()
		f.RolloutStrategy = &domain.RolloutStrategySpec{
			Type:                  domain.RolloutStrategyImmediate,
			VersionConflictPolicy: domain.VersionConflictCompleteAll,
		}
		f.RolloutStrategyVersion = 1

		if err := repo.Create(ctx, f); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := repo.Get(ctx, "f1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.RolloutStrategy == nil {
			t.Fatal("RolloutStrategy is nil after round-trip")
		}
		if got.RolloutStrategy.Type != domain.RolloutStrategyImmediate {
			t.Errorf("RolloutStrategy.Type = %q, want %q", got.RolloutStrategy.Type, domain.RolloutStrategyImmediate)
		}
		if got.RolloutStrategy.VersionConflictPolicy != domain.VersionConflictCompleteAll {
			t.Errorf("RolloutStrategy.VersionConflictPolicy = %q, want %q", got.RolloutStrategy.VersionConflictPolicy, domain.VersionConflictCompleteAll)
		}
		if got.RolloutStrategyVersion != 1 {
			t.Errorf("RolloutStrategyVersion = %d, want 1", got.RolloutStrategyVersion)
		}
	})

	t.Run("CreateAndGet_WithProvenance", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()
		f := sampleFulfillment()
		f.Provenance = &domain.Provenance{
			Sig: domain.Signature{
				Signer: domain.FederatedIdentity{
					Subject: "user-1",
					Issuer:  "https://issuer.example.com",
				},
				ContentHash:    []byte("sha256-hash-bytes"),
				SignatureBytes: []byte("ecdsa-sig-bytes"),
			},
			ValidUntil:         fixedTime.Add(24 * time.Hour),
			ExpectedGeneration: 1,
			OutputConstraints: []domain.OutputConstraint{
				{Name: "cluster-version", Expression: ">= 4.14"},
			},
		}

		if err := repo.Create(ctx, f); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := repo.Get(ctx, "f1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Provenance == nil {
			t.Fatal("Provenance is nil after round-trip")
		}
		if string(got.Provenance.Sig.ContentHash) != "sha256-hash-bytes" {
			t.Errorf("Provenance.Sig.ContentHash = %q, want %q", got.Provenance.Sig.ContentHash, "sha256-hash-bytes")
		}
		if string(got.Provenance.Sig.SignatureBytes) != "ecdsa-sig-bytes" {
			t.Errorf("Provenance.Sig.SignatureBytes = %q, want %q", got.Provenance.Sig.SignatureBytes, "ecdsa-sig-bytes")
		}
		if got.Provenance.Sig.Signer.Subject != "user-1" {
			t.Errorf("Provenance.Sig.Signer.Subject = %q, want %q", got.Provenance.Sig.Signer.Subject, "user-1")
		}
		if !got.Provenance.ValidUntil.Equal(fixedTime.Add(24 * time.Hour)) {
			t.Errorf("Provenance.ValidUntil = %v, want %v", got.Provenance.ValidUntil, fixedTime.Add(24*time.Hour))
		}
		if got.Provenance.ExpectedGeneration != 1 {
			t.Errorf("Provenance.ExpectedGeneration = %d, want 1", got.Provenance.ExpectedGeneration)
		}
		if len(got.Provenance.OutputConstraints) != 1 {
			t.Fatalf("Provenance.OutputConstraints len = %d, want 1", len(got.Provenance.OutputConstraints))
		}
		if got.Provenance.OutputConstraints[0].Name != "cluster-version" {
			t.Errorf("OutputConstraints[0].Name = %q, want %q", got.Provenance.OutputConstraints[0].Name, "cluster-version")
		}
	})

	t.Run("CreateDuplicate", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()
		f := sampleFulfillment()
		_ = repo.Create(ctx, f)
		err := repo.Create(ctx, f)
		if !errors.Is(err, domain.ErrAlreadyExists) {
			t.Fatalf("second Create: got %v, want ErrAlreadyExists", err)
		}
	})

	t.Run("GetNotFound", func(t *testing.T) {
		repo := factory(t)
		_, err := repo.Get(context.Background(), "nonexistent")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("Get: got %v, want ErrNotFound", err)
		}
	})

	t.Run("Update", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()
		f := sampleFulfillment()
		_ = repo.Create(ctx, f)

		laterTime := fixedTime.Add(5 * time.Minute)
		f.State = domain.FulfillmentStateActive
		f.ResolvedTargets = []domain.TargetID{"t1", "t2"}
		f.UpdatedAt = laterTime
		f.Generation = 2
		if err := repo.Update(ctx, f); err != nil {
			t.Fatalf("Update: %v", err)
		}

		got, _ := repo.Get(ctx, "f1")
		if got.State != domain.FulfillmentStateActive {
			t.Errorf("State after Update = %q, want %q", got.State, domain.FulfillmentStateActive)
		}
		if len(got.ResolvedTargets) != 2 {
			t.Errorf("ResolvedTargets = %d, want 2", len(got.ResolvedTargets))
		}
		if !got.CreatedAt.Equal(fixedTime) {
			t.Errorf("CreatedAt changed after Update: got %v, want %v", got.CreatedAt, fixedTime)
		}
		if !got.UpdatedAt.Equal(laterTime) {
			t.Errorf("UpdatedAt = %v, want %v", got.UpdatedAt, laterTime)
		}
	})

	t.Run("UpdateNotFound", func(t *testing.T) {
		repo := factory(t)
		err := repo.Update(context.Background(), domain.Fulfillment{ID: "nonexistent"})
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("Update: got %v, want ErrNotFound", err)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()
		_ = repo.Create(ctx, sampleFulfillment())
		if err := repo.Delete(ctx, "f1"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		_, err := repo.Get(ctx, "f1")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("Get after Delete: got %v, want ErrNotFound", err)
		}
	})

	t.Run("DeleteNotFound", func(t *testing.T) {
		repo := factory(t)
		err := repo.Delete(context.Background(), "nonexistent")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("Delete: got %v, want ErrNotFound", err)
		}
	})

	t.Run("GenerationFields_RoundTrip", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()
		f := sampleFulfillment()
		f.Generation = 3
		f.ObservedGeneration = 2

		if err := repo.Create(ctx, f); err != nil {
			t.Fatalf("Create: %v", err)
		}
		got, err := repo.Get(ctx, "f1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Generation != 3 {
			t.Errorf("Generation = %d, want 3", got.Generation)
		}
		if got.ObservedGeneration != 2 {
			t.Errorf("ObservedGeneration = %d, want 2", got.ObservedGeneration)
		}
	})

	t.Run("Update_PersistsReconciliationFields", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()
		f := sampleFulfillment()
		f.Generation = 1
		f.ObservedGeneration = 0
		_ = repo.Create(ctx, f)

		f.Generation = 5
		f.ObservedGeneration = 3
		if err := repo.Update(ctx, f); err != nil {
			t.Fatalf("Update: %v", err)
		}

		got, _ := repo.Get(ctx, "f1")
		if got.Generation != 5 {
			t.Errorf("Generation = %d, want 5", got.Generation)
		}
		if got.ObservedGeneration != 3 {
			t.Errorf("ObservedGeneration = %d, want 3", got.ObservedGeneration)
		}
	})

	t.Run("ActiveWorkflowGen_RoundTrip", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()
		f := sampleFulfillment()
		gen := domain.Generation(5)
		f.ActiveWorkflowGen = &gen

		if err := repo.Create(ctx, f); err != nil {
			t.Fatalf("Create: %v", err)
		}
		got, err := repo.Get(ctx, "f1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.ActiveWorkflowGen == nil {
			t.Fatal("ActiveWorkflowGen is nil after round-trip, want non-nil")
		}
		if *got.ActiveWorkflowGen != 5 {
			t.Errorf("ActiveWorkflowGen = %d, want 5", *got.ActiveWorkflowGen)
		}
	})

	t.Run("ActiveWorkflowGen_NilByDefault", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()
		f := sampleFulfillment()

		if err := repo.Create(ctx, f); err != nil {
			t.Fatalf("Create: %v", err)
		}
		got, err := repo.Get(ctx, "f1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.ActiveWorkflowGen != nil {
			t.Errorf("ActiveWorkflowGen = %d, want nil", *got.ActiveWorkflowGen)
		}
	})

	t.Run("Update_ActiveWorkflowGen_SetAndClear", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()
		f := sampleFulfillment()
		_ = repo.Create(ctx, f)

		gen := domain.Generation(2)
		f.ActiveWorkflowGen = &gen
		f.UpdatedAt = fixedTime.Add(time.Minute)
		if err := repo.Update(ctx, f); err != nil {
			t.Fatalf("Update (set): %v", err)
		}
		got, _ := repo.Get(ctx, "f1")
		if got.ActiveWorkflowGen == nil || *got.ActiveWorkflowGen != 2 {
			t.Fatalf("after set: ActiveWorkflowGen = %v, want 2", got.ActiveWorkflowGen)
		}

		f.ActiveWorkflowGen = nil
		f.UpdatedAt = fixedTime.Add(2 * time.Minute)
		if err := repo.Update(ctx, f); err != nil {
			t.Fatalf("Update (clear): %v", err)
		}
		got, _ = repo.Get(ctx, "f1")
		if got.ActiveWorkflowGen != nil {
			t.Errorf("after clear: ActiveWorkflowGen = %d, want nil", *got.ActiveWorkflowGen)
		}
	})

	t.Run("Update_AdvanceManifestStrategy_PersistsNewVersion", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()
		f := sampleFulfillment()
		_ = repo.Create(ctx, f)

		advTime := fixedTime.Add(time.Hour)
		f.AdvanceManifestStrategy(domain.ManifestStrategySpec{
			Type: domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{
				{Raw: json.RawMessage(`{"kind":"Secret"}`)},
			},
		}, advTime)
		if err := repo.Update(ctx, f); err != nil {
			t.Fatalf("Update: %v", err)
		}

		got, _ := repo.Get(ctx, "f1")
		if got.ManifestStrategyVersion != 2 {
			t.Errorf("ManifestStrategyVersion = %d, want 2", got.ManifestStrategyVersion)
		}
		if len(got.ManifestStrategy.Manifests) != 1 || string(got.ManifestStrategy.Manifests[0].Raw) != `{"kind":"Secret"}` {
			t.Fatalf("manifest spec not updated: %+v", got.ManifestStrategy.Manifests)
		}
	})

	t.Run("Update_WithRolloutAndProvenance", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()
		f := sampleFulfillment()
		_ = repo.Create(ctx, f)

		f.RolloutStrategy = &domain.RolloutStrategySpec{
			Type:                  domain.RolloutStrategyImmediate,
			VersionConflictPolicy: domain.VersionConflictCompleteAll,
		}
		f.RolloutStrategyVersion = 1
		f.Provenance = &domain.Provenance{
			Sig: domain.Signature{
				Signer: domain.FederatedIdentity{
					Subject: "user-1",
					Issuer:  "https://issuer.example.com",
				},
				ContentHash:    []byte("hash"),
				SignatureBytes: []byte("sig"),
			},
			ValidUntil:         fixedTime.Add(24 * time.Hour),
			ExpectedGeneration: 1,
		}
		f.UpdatedAt = fixedTime.Add(time.Minute)
		if err := repo.Update(ctx, f); err != nil {
			t.Fatalf("Update: %v", err)
		}

		got, err := repo.Get(ctx, "f1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.RolloutStrategy == nil {
			t.Fatal("RolloutStrategy is nil after Update round-trip")
		}
		if got.RolloutStrategy.Type != domain.RolloutStrategyImmediate {
			t.Errorf("RolloutStrategy.Type = %q, want %q", got.RolloutStrategy.Type, domain.RolloutStrategyImmediate)
		}
		if got.Provenance == nil {
			t.Fatal("Provenance is nil after Update round-trip")
		}
		if string(got.Provenance.Sig.ContentHash) != "hash" {
			t.Errorf("Provenance.Sig.ContentHash = %q, want %q", got.Provenance.Sig.ContentHash, "hash")
		}
	})
}
