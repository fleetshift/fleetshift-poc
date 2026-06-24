// Package inventoryrepotest provides contract tests for
// [domain.InventoryRepository] implementations.
package inventoryrepotest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Factory creates a fresh [domain.InventoryRepository] for each test.
type Factory func(t *testing.T) domain.InventoryRepository

// Run exercises the [domain.InventoryRepository] contract.
func Run(t *testing.T, factory Factory) {
	now := time.Date(2026, 3, 2, 12, 0, 0, 0, time.UTC)

	t.Run("CreateAndGet", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		did := domain.DeliveryID("d1:t1")
		item := domain.InventoryItemFromSnapshot(domain.InventoryItemSnapshot{
			ID:               "inv-1",
			Type:             "docker.daemon",
			Name:             "local-docker",
			Properties:       json.RawMessage(`{"engine":"docker","version":"24.0.7"}`),
			Labels:           map[string]string{"host": "laptop"},
			SourceDeliveryID: &did,
			CreatedAt:        now,
			UpdatedAt:        now,
		})
		if err := repo.Create(ctx, item); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := repo.Get(ctx, "inv-1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Type() != "docker.daemon" {
			t.Errorf("Type = %q, want %q", got.Type(), "docker.daemon")
		}
		if got.Name() != "local-docker" {
			t.Errorf("Name = %q, want %q", got.Name(), "local-docker")
		}
		if got.Labels()["host"] != "laptop" {
			t.Errorf("Labels[host] = %q, want %q", got.Labels()["host"], "laptop")
		}
		if got.SourceDeliveryID() == nil || *got.SourceDeliveryID() != "d1:t1" {
			t.Errorf("SourceDeliveryID = %v, want d1:t1", got.SourceDeliveryID())
		}

		var props map[string]string
		if err := json.Unmarshal(got.Properties(), &props); err != nil {
			t.Fatalf("unmarshal properties: %v", err)
		}
		if props["engine"] != "docker" {
			t.Errorf("Properties.engine = %q, want docker", props["engine"])
		}
	})

	t.Run("CreateWithNilSourceDelivery", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		item := domain.InventoryItemFromSnapshot(domain.InventoryItemSnapshot{
			ID:        "inv-1",
			Type:      "kubernetes.node",
			Name:      "worker-1",
			CreatedAt: now,
			UpdatedAt: now,
		})
		if err := repo.Create(ctx, item); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := repo.Get(ctx, "inv-1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.SourceDeliveryID() != nil {
			t.Errorf("SourceDeliveryID = %v, want nil", got.SourceDeliveryID())
		}
	})

	t.Run("CreateOrUpdate_Insert", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		item := domain.InventoryItemFromSnapshot(domain.InventoryItemSnapshot{
			ID:         "inv-1",
			Type:       "docker.daemon",
			Name:       "local-docker",
			Properties: json.RawMessage(`{"version":"24"}`),
			CreatedAt:  now,
			UpdatedAt:  now,
		})
		if err := repo.CreateOrUpdate(ctx, item); err != nil {
			t.Fatalf("CreateOrUpdate (insert): %v", err)
		}

		got, err := repo.Get(ctx, "inv-1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Name() != "local-docker" {
			t.Errorf("Name = %q, want %q", got.Name(), "local-docker")
		}
	})

	t.Run("CreateOrUpdate_Update", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		item := domain.InventoryItemFromSnapshot(domain.InventoryItemSnapshot{
			ID: "inv-1", Type: "docker.daemon", Name: "old",
			Properties: json.RawMessage(`{"version":"23"}`),
			CreatedAt:  now, UpdatedAt: now,
		})
		if err := repo.Create(ctx, item); err != nil {
			t.Fatalf("Create: %v", err)
		}

		item = domain.InventoryItemFromSnapshot(domain.InventoryItemSnapshot{
			ID:         item.ID(),
			Type:       item.Type(),
			Name:       "new",
			Properties: json.RawMessage(`{"version":"24"}`),
			Labels:     item.Labels(),
			CreatedAt:  item.CreatedAt(),
			UpdatedAt:  now.Add(time.Minute),
		})
		if err := repo.CreateOrUpdate(ctx, item); err != nil {
			t.Fatalf("CreateOrUpdate (update): %v", err)
		}

		got, err := repo.Get(ctx, "inv-1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Name() != "new" {
			t.Errorf("Name = %q, want new", got.Name())
		}
		if !got.CreatedAt().Equal(now) {
			t.Errorf("CreatedAt changed: got %v, want %v", got.CreatedAt(), now)
		}
	})

	t.Run("CreateDuplicate", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		item := domain.InventoryItemFromSnapshot(domain.InventoryItemSnapshot{ID: "inv-1", Type: "a", Name: "x", CreatedAt: now, UpdatedAt: now})
		_ = repo.Create(ctx, item)
		err := repo.Create(ctx, item)
		if !errors.Is(err, domain.ErrAlreadyExists) {
			t.Fatalf("got %v, want ErrAlreadyExists", err)
		}
	})

	t.Run("GetNotFound", func(t *testing.T) {
		repo := factory(t)
		_, err := repo.Get(context.Background(), "missing")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("got %v, want ErrNotFound", err)
		}
	})

	t.Run("List", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		_ = repo.Create(ctx, domain.InventoryItemFromSnapshot(domain.InventoryItemSnapshot{ID: "a", Type: "x", Name: "1", CreatedAt: now, UpdatedAt: now}))
		_ = repo.Create(ctx, domain.InventoryItemFromSnapshot(domain.InventoryItemSnapshot{ID: "b", Type: "y", Name: "2", CreatedAt: now, UpdatedAt: now}))

		got, err := repo.List(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("List: got %d, want 2", len(got))
		}
	})

	t.Run("ListByType", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		_ = repo.Create(ctx, domain.InventoryItemFromSnapshot(domain.InventoryItemSnapshot{ID: "a", Type: "docker.daemon", Name: "1", CreatedAt: now, UpdatedAt: now}))
		_ = repo.Create(ctx, domain.InventoryItemFromSnapshot(domain.InventoryItemSnapshot{ID: "b", Type: "kubernetes.node", Name: "2", CreatedAt: now, UpdatedAt: now}))
		_ = repo.Create(ctx, domain.InventoryItemFromSnapshot(domain.InventoryItemSnapshot{ID: "c", Type: "docker.daemon", Name: "3", CreatedAt: now, UpdatedAt: now}))

		got, err := repo.ListByType(ctx, "docker.daemon")
		if err != nil {
			t.Fatalf("ListByType: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("ListByType(docker.daemon): got %d, want 2", len(got))
		}
	})

	t.Run("Update", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		item := domain.InventoryItemFromSnapshot(domain.InventoryItemSnapshot{
			ID: "inv-1", Type: "docker.daemon", Name: "old",
			Properties: json.RawMessage(`{"version":"23"}`),
			CreatedAt:  now, UpdatedAt: now,
		})
		_ = repo.Create(ctx, item)

		item = domain.InventoryItemFromSnapshot(domain.InventoryItemSnapshot{
			ID:         item.ID(),
			Type:       item.Type(),
			Name:       "new",
			Properties: json.RawMessage(`{"version":"24"}`),
			Labels:     item.Labels(),
			CreatedAt:  item.CreatedAt(),
			UpdatedAt:  now.Add(time.Minute),
		})
		if err := repo.Update(ctx, item); err != nil {
			t.Fatalf("Update: %v", err)
		}

		got, err := repo.Get(ctx, "inv-1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Name() != "new" {
			t.Errorf("Name = %q, want new", got.Name())
		}
	})

	t.Run("UpdateNotFound", func(t *testing.T) {
		repo := factory(t)
		err := repo.Update(context.Background(), domain.InventoryItemFromSnapshot(domain.InventoryItemSnapshot{ID: "missing", UpdatedAt: now}))
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("got %v, want ErrNotFound", err)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		_ = repo.Create(ctx, domain.InventoryItemFromSnapshot(domain.InventoryItemSnapshot{ID: "inv-1", Type: "a", Name: "x", CreatedAt: now, UpdatedAt: now}))
		if err := repo.Delete(ctx, "inv-1"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		_, err := repo.Get(ctx, "inv-1")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("Get after Delete: got %v, want ErrNotFound", err)
		}
	})

	t.Run("DeleteNotFound", func(t *testing.T) {
		repo := factory(t)
		err := repo.Delete(context.Background(), "missing")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("got %v, want ErrNotFound", err)
		}
	})

	t.Run("DeleteByTarget", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		_ = repo.Create(ctx, domain.InventoryItemFromSnapshot(domain.InventoryItemSnapshot{
			ID: "a", Type: "apps/v1/Deployment", Name: "d1",
			TargetID: "k8s-cluster1", CreatedAt: now, UpdatedAt: now,
		}))
		_ = repo.Create(ctx, domain.InventoryItemFromSnapshot(domain.InventoryItemSnapshot{
			ID: "b", Type: "v1/Pod", Name: "p1",
			TargetID: "k8s-cluster1", CreatedAt: now, UpdatedAt: now,
		}))
		_ = repo.Create(ctx, domain.InventoryItemFromSnapshot(domain.InventoryItemSnapshot{
			ID: "c", Type: "apps/v1/Deployment", Name: "d2",
			TargetID: "k8s-cluster2", CreatedAt: now, UpdatedAt: now,
		}))

		if err := repo.DeleteByTarget(ctx, "k8s-cluster1"); err != nil {
			t.Fatalf("DeleteByTarget: %v", err)
		}

		got, err := repo.List(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("List: got %d, want 1", len(got))
		}
		if got[0].ID() != "c" {
			t.Errorf("remaining item ID = %q, want %q", got[0].ID(), "c")
		}
	})

	t.Run("ReplaceByTargetAndType", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		_ = repo.Create(ctx, domain.InventoryItemFromSnapshot(domain.InventoryItemSnapshot{
			ID: "a", Type: "apps/v1/Deployment", Name: "old-deploy",
			TargetID: "k8s-cluster1", CreatedAt: now, UpdatedAt: now,
		}))
		_ = repo.Create(ctx, domain.InventoryItemFromSnapshot(domain.InventoryItemSnapshot{
			ID: "b", Type: "v1/Pod", Name: "keep-pod",
			TargetID: "k8s-cluster1", CreatedAt: now, UpdatedAt: now,
		}))

		replacements := []domain.InventoryItem{
			domain.InventoryItemFromSnapshot(domain.InventoryItemSnapshot{
				ID: "x", Type: "apps/v1/Deployment", Name: "new-deploy-1",
				TargetID: "k8s-cluster1", CreatedAt: now, UpdatedAt: now,
			}),
			domain.InventoryItemFromSnapshot(domain.InventoryItemSnapshot{
				ID: "y", Type: "apps/v1/Deployment", Name: "new-deploy-2",
				TargetID: "k8s-cluster1", CreatedAt: now, UpdatedAt: now,
			}),
		}

		if err := repo.ReplaceByTargetAndType(ctx, "k8s-cluster1", "apps/v1/Deployment", replacements); err != nil {
			t.Fatalf("ReplaceByTargetAndType: %v", err)
		}

		got, err := repo.List(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("List: got %d, want 3 (2 new deploys + 1 kept pod)", len(got))
		}

		_, err = repo.Get(ctx, "a")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("Get old deploy: got %v, want ErrNotFound", err)
		}
	})

	t.Run("CreateAndGetWithObservation", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		conditions := []domain.InventoryCondition{{
			Type: "Ready", Status: "True", Reason: "AllGood",
		}}
		observed := json.RawMessage(`{"replicas":3,"readyReplicas":3}`)

		item := domain.InventoryItemFromSnapshot(domain.InventoryItemSnapshot{
			ID: "obs-1", Type: "apps/v1/Deployment", Name: "my-deploy",
			TargetID: "k8s-cluster1", Observed: observed,
			Conditions: conditions, ObservedAt: &now,
			CreatedAt: now, UpdatedAt: now,
		})
		if err := repo.Create(ctx, item); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := repo.Get(ctx, "obs-1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.TargetID() != "k8s-cluster1" {
			t.Errorf("TargetID = %q, want %q", got.TargetID(), "k8s-cluster1")
		}
		if got.ObservedAt() == nil || !got.ObservedAt().Equal(now) {
			t.Errorf("ObservedAt = %v, want %v", got.ObservedAt(), now)
		}
		if len(got.Conditions()) != 1 || got.Conditions()[0].Type != "Ready" {
			t.Errorf("Conditions = %+v, want [{Type:Ready ...}]", got.Conditions())
		}

		var obs map[string]any
		if err := json.Unmarshal(got.Observed(), &obs); err != nil {
			t.Fatalf("unmarshal observed: %v", err)
		}
		if obs["replicas"] != float64(3) {
			t.Errorf("observed.replicas = %v, want 3", obs["replicas"])
		}
	})
}
