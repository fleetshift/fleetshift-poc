// Package deploymentrepotest provides contract tests for
// [domain.DeploymentRepository] implementations.
package deploymentrepotest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Factory creates a fresh [domain.DeploymentRepository] for each test.
type Factory func(t *testing.T) domain.DeploymentRepository

// Run exercises the [domain.DeploymentRepository] contract.
func Run(t *testing.T, factory Factory) {
	fixedTime := time.Date(2026, 3, 2, 12, 0, 0, 0, time.UTC)

	sampleDeployment := func() domain.Deployment {
		return domain.Deployment{
			ID:  "d1",
			UID: "uid-abc-123",
			ManifestStrategy: domain.ManifestStrategySpec{
				Type:      domain.ManifestStrategyInline,
				Manifests: []domain.Manifest{{Raw: json.RawMessage(`{"kind":"ConfigMap"}`)}},
			},
			PlacementStrategy: domain.PlacementStrategySpec{
				Type:    domain.PlacementStrategyStatic,
				Targets: []domain.TargetID{"t1", "t2"},
			},
			State:     domain.DeploymentStateCreating,
			CreatedAt: fixedTime,
			UpdatedAt: fixedTime,
			Etag:      "etag-v1",
		}
	}

	t.Run("CreateAndGet", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()
		d := sampleDeployment()

		if err := repo.Create(ctx, d); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := repo.Get(ctx, "d1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.ManifestStrategy.Type != domain.ManifestStrategyInline {
			t.Errorf("ManifestStrategy.Type = %q, want %q", got.ManifestStrategy.Type, domain.ManifestStrategyInline)
		}
		if len(got.PlacementStrategy.Targets) != 2 {
			t.Errorf("PlacementStrategy.Targets = %d, want 2", len(got.PlacementStrategy.Targets))
		}
		if got.State != domain.DeploymentStateCreating {
			t.Errorf("State = %q, want %q", got.State, domain.DeploymentStateCreating)
		}
		if got.UID != "uid-abc-123" {
			t.Errorf("UID = %q, want %q", got.UID, "uid-abc-123")
		}
		if !got.CreatedAt.Equal(fixedTime) {
			t.Errorf("CreatedAt = %v, want %v", got.CreatedAt, fixedTime)
		}
		if !got.UpdatedAt.Equal(fixedTime) {
			t.Errorf("UpdatedAt = %v, want %v", got.UpdatedAt, fixedTime)
		}
		if got.Etag != "etag-v1" {
			t.Errorf("Etag = %q, want %q", got.Etag, "etag-v1")
		}
	})

	t.Run("CreateDuplicate", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()
		d := sampleDeployment()
		_ = repo.Create(ctx, d)
		err := repo.Create(ctx, d)
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
		d := sampleDeployment()
		_ = repo.Create(ctx, d)

		laterTime := fixedTime.Add(5 * time.Minute)
		d.State = domain.DeploymentStateActive
		d.ResolvedTargets = []domain.TargetID{"t1", "t2"}
		d.UpdatedAt = laterTime
		d.Etag = "etag-v2"
		if err := repo.Update(ctx, d); err != nil {
			t.Fatalf("Update: %v", err)
		}

		got, _ := repo.Get(ctx, "d1")
		if got.State != domain.DeploymentStateActive {
			t.Errorf("State after Update = %q, want %q", got.State, domain.DeploymentStateActive)
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
		if got.Etag != "etag-v2" {
			t.Errorf("Etag = %q, want %q", got.Etag, "etag-v2")
		}
	})

	t.Run("UpdateNotFound", func(t *testing.T) {
		repo := factory(t)
		err := repo.Update(context.Background(), domain.Deployment{ID: "nonexistent"})
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("Update: got %v, want ErrNotFound", err)
		}
	})

	t.Run("List", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()
		d1 := sampleDeployment()
		d2 := sampleDeployment()
		d2.ID = "d2"
		_ = repo.Create(ctx, d1)
		_ = repo.Create(ctx, d2)

		got, err := repo.List(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("List: got %d, want 2", len(got))
		}
	})

	t.Run("Delete", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()
		_ = repo.Create(ctx, sampleDeployment())
		if err := repo.Delete(ctx, "d1"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		_, err := repo.Get(ctx, "d1")
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
		d := sampleDeployment()
		d.Generation = 3
		d.ObservedGeneration = 2

		if err := repo.Create(ctx, d); err != nil {
			t.Fatalf("Create: %v", err)
		}
		got, err := repo.Get(ctx, "d1")
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
		d := sampleDeployment()
		d.Generation = 1
		d.ObservedGeneration = 0
		_ = repo.Create(ctx, d)

		d.Generation = 5
		d.ObservedGeneration = 3
		if err := repo.Update(ctx, d); err != nil {
			t.Fatalf("Update: %v", err)
		}

		got, _ := repo.Get(ctx, "d1")
		if got.Generation != 5 {
			t.Errorf("Generation = %d, want 5", got.Generation)
		}
		if got.ObservedGeneration != 3 {
			t.Errorf("ObservedGeneration = %d, want 3", got.ObservedGeneration)
		}
	})
}
