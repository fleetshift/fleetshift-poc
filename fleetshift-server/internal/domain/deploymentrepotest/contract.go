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

// Factory creates a fresh [domain.Tx] for each test so callers can use
// [domain.Tx.Fulfillments] before creating deployments (foreign key)
// and [domain.Tx.Deployments] for the repository under test.
type Factory func(t *testing.T) domain.Tx

// Run exercises the [domain.DeploymentRepository] contract.
func Run(t *testing.T, factory Factory) {
	fixedTime := time.Date(2026, 3, 2, 12, 0, 0, 0, time.UTC)
	ctx := context.Background()

	sampleFulfillment := func(id domain.FulfillmentID) *domain.Fulfillment {
		f := domain.Fulfillment{
			ID:        id,
			State:     domain.FulfillmentStateCreating,
			CreatedAt: fixedTime,
			UpdatedAt: fixedTime,
		}
		f.AdvanceManifestStrategy(domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{Raw: json.RawMessage(`{"kind":"ConfigMap"}`)}},
		}, fixedTime)
		f.AdvancePlacementStrategy(domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"t1", "t2"},
		}, fixedTime)
		return &f
	}

	sampleThinDeployment := func(depID domain.DeploymentID, fid domain.FulfillmentID) domain.Deployment {
		return domain.Deployment{
			ID:            depID,
			UID:           "uid-abc-123",
			FulfillmentID: fid,
			CreatedAt:     fixedTime,
			UpdatedAt:     fixedTime,
			Etag:          "etag-v1",
		}
	}

	t.Run("CreateAndGet", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()

		fid := domain.FulfillmentID("f-create-get")
		if err := tx.Fulfillments().Create(ctx, sampleFulfillment(fid)); err != nil {
			t.Fatalf("Create fulfillment: %v", err)
		}
		d := sampleThinDeployment("d-create-get", fid)
		repo := tx.Deployments()

		if err := repo.Create(ctx, d); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := repo.Get(ctx, "d-create-get")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.FulfillmentID != fid {
			t.Errorf("FulfillmentID = %q, want %q", got.FulfillmentID, fid)
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
		tx := factory(t)
		defer tx.Rollback()

		fid := domain.FulfillmentID("f-dup")
		if err := tx.Fulfillments().Create(ctx, sampleFulfillment(fid)); err != nil {
			t.Fatalf("Create fulfillment: %v", err)
		}
		d := sampleThinDeployment("d-dup", fid)
		repo := tx.Deployments()
		if err := repo.Create(ctx, d); err != nil {
			t.Fatalf("Create: %v", err)
		}
		err := repo.Create(ctx, d)
		if !errors.Is(err, domain.ErrAlreadyExists) {
			t.Fatalf("second Create: got %v, want ErrAlreadyExists", err)
		}
	})

	t.Run("GetNotFound", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()

		_, err := tx.Deployments().Get(ctx, "nonexistent-deployment")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("Get: got %v, want ErrNotFound", err)
		}
	})

	t.Run("GetView", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()

		f := sampleFulfillment("f-view")
		f.AdvanceRolloutStrategy(&domain.RolloutStrategySpec{
			Type:                  domain.RolloutStrategyImmediate,
			VersionConflictPolicy: domain.VersionConflictCompleteAll,
		}, fixedTime)

		if err := tx.Fulfillments().Create(ctx, f); err != nil {
			t.Fatalf("Create fulfillment: %v", err)
		}

		d := sampleThinDeployment("d-view", f.ID)
		repo := tx.Deployments()
		if err := repo.Create(ctx, d); err != nil {
			t.Fatalf("Create deployment: %v", err)
		}

		v, err := repo.GetView(ctx, "d-view")
		if err != nil {
			t.Fatalf("GetView: %v", err)
		}
		if v.Deployment.ID != "d-view" {
			t.Errorf("Deployment.ID = %q, want %q", v.Deployment.ID, "d-view")
		}
		if v.Deployment.FulfillmentID != f.ID {
			t.Errorf("Deployment.FulfillmentID = %q, want %q", v.Deployment.FulfillmentID, f.ID)
		}
		if v.Fulfillment.ID != f.ID {
			t.Errorf("Fulfillment.ID = %q, want %q", v.Fulfillment.ID, f.ID)
		}
		if v.Fulfillment.ManifestStrategy.Type != domain.ManifestStrategyInline {
			t.Errorf("Fulfillment.ManifestStrategy.Type = %q, want %q", v.Fulfillment.ManifestStrategy.Type, domain.ManifestStrategyInline)
		}
		if len(v.Fulfillment.PlacementStrategy.Targets) != 2 {
			t.Errorf("Fulfillment.PlacementStrategy.Targets len = %d, want 2", len(v.Fulfillment.PlacementStrategy.Targets))
		}
		if v.Fulfillment.State != domain.FulfillmentStateCreating {
			t.Errorf("Fulfillment.State = %q, want %q", v.Fulfillment.State, domain.FulfillmentStateCreating)
		}
		if v.Fulfillment.RolloutStrategy == nil {
			t.Fatal("Fulfillment.RolloutStrategy is nil after GetView")
		}
		if v.Fulfillment.RolloutStrategy.Type != domain.RolloutStrategyImmediate {
			t.Errorf("RolloutStrategy.Type = %q, want %q", v.Fulfillment.RolloutStrategy.Type, domain.RolloutStrategyImmediate)
		}
	})

	t.Run("ListView", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()

		for _, pair := range []struct {
			depID string
			fid   domain.FulfillmentID
		}{
			{"d-list-a", "f-list-a"},
			{"d-list-b", "f-list-b"},
		} {
			f := sampleFulfillment(pair.fid)
			if err := tx.Fulfillments().Create(ctx, f); err != nil {
				t.Fatalf("Create fulfillment %q: %v", pair.fid, err)
			}
			d := sampleThinDeployment(domain.DeploymentID(pair.depID), pair.fid)
			if err := tx.Deployments().Create(ctx, d); err != nil {
				t.Fatalf("Create deployment %q: %v", pair.depID, err)
			}
		}

		views, err := tx.Deployments().ListView(ctx)
		if err != nil {
			t.Fatalf("ListView: %v", err)
		}
		if len(views) != 2 {
			t.Fatalf("ListView len = %d, want 2", len(views))
		}
		seen := map[domain.DeploymentID]bool{}
		for _, v := range views {
			seen[v.Deployment.ID] = true
			if v.Deployment.FulfillmentID != v.Fulfillment.ID {
				t.Errorf("deployment %s: FulfillmentID %q != Fulfillment.ID %q", v.Deployment.ID, v.Deployment.FulfillmentID, v.Fulfillment.ID)
			}
		}
		for _, id := range []domain.DeploymentID{"d-list-a", "d-list-b"} {
			if !seen[id] {
				t.Errorf("ListView missing deployment %q", id)
			}
		}
	})

	t.Run("Delete", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()

		fid := domain.FulfillmentID("f-del")
		if err := tx.Fulfillments().Create(ctx, sampleFulfillment(fid)); err != nil {
			t.Fatalf("Create fulfillment: %v", err)
		}
		d := sampleThinDeployment("d-del", fid)
		repo := tx.Deployments()
		if err := repo.Create(ctx, d); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if err := repo.Delete(ctx, "d-del"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		_, err := repo.Get(ctx, "d-del")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("Get after Delete: got %v, want ErrNotFound", err)
		}
	})

	t.Run("DeleteNotFound", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()

		err := tx.Deployments().Delete(ctx, "nonexistent-deployment")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("Delete: got %v, want ErrNotFound", err)
		}
	})
}
