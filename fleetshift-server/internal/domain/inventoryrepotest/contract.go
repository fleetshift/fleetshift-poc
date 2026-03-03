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
		item := domain.InventoryItem{
			ID:               "inv-1",
			Type:             "docker.daemon",
			Name:             "local-docker",
			Properties:       json.RawMessage(`{"engine":"docker","version":"24.0.7"}`),
			Labels:           map[string]string{"host": "laptop"},
			SourceDeliveryID: &did,
			CreatedAt:        now,
			UpdatedAt:        now,
		}
		if err := repo.Create(ctx, item); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := repo.Get(ctx, "inv-1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Type != "docker.daemon" {
			t.Errorf("Type = %q, want %q", got.Type, "docker.daemon")
		}
		if got.Name != "local-docker" {
			t.Errorf("Name = %q, want %q", got.Name, "local-docker")
		}
		if got.Labels["host"] != "laptop" {
			t.Errorf("Labels[host] = %q, want %q", got.Labels["host"], "laptop")
		}
		if got.SourceDeliveryID == nil || *got.SourceDeliveryID != "d1:t1" {
			t.Errorf("SourceDeliveryID = %v, want d1:t1", got.SourceDeliveryID)
		}

		var props map[string]string
		if err := json.Unmarshal(got.Properties, &props); err != nil {
			t.Fatalf("unmarshal properties: %v", err)
		}
		if props["engine"] != "docker" {
			t.Errorf("Properties.engine = %q, want docker", props["engine"])
		}
	})

	t.Run("CreateWithNilSourceDelivery", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		item := domain.InventoryItem{
			ID:        "inv-1",
			Type:      "kubernetes.node",
			Name:      "worker-1",
			CreatedAt: now,
			UpdatedAt: now,
		}
		if err := repo.Create(ctx, item); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := repo.Get(ctx, "inv-1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.SourceDeliveryID != nil {
			t.Errorf("SourceDeliveryID = %v, want nil", got.SourceDeliveryID)
		}
	})

	t.Run("CreateDuplicate", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		item := domain.InventoryItem{ID: "inv-1", Type: "a", Name: "x", CreatedAt: now, UpdatedAt: now}
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

		_ = repo.Create(ctx, domain.InventoryItem{ID: "a", Type: "x", Name: "1", CreatedAt: now, UpdatedAt: now})
		_ = repo.Create(ctx, domain.InventoryItem{ID: "b", Type: "y", Name: "2", CreatedAt: now, UpdatedAt: now})

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

		_ = repo.Create(ctx, domain.InventoryItem{ID: "a", Type: "docker.daemon", Name: "1", CreatedAt: now, UpdatedAt: now})
		_ = repo.Create(ctx, domain.InventoryItem{ID: "b", Type: "kubernetes.node", Name: "2", CreatedAt: now, UpdatedAt: now})
		_ = repo.Create(ctx, domain.InventoryItem{ID: "c", Type: "docker.daemon", Name: "3", CreatedAt: now, UpdatedAt: now})

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

		item := domain.InventoryItem{
			ID: "inv-1", Type: "docker.daemon", Name: "old",
			Properties: json.RawMessage(`{"version":"23"}`),
			CreatedAt:  now, UpdatedAt: now,
		}
		_ = repo.Create(ctx, item)

		item.Name = "new"
		item.Properties = json.RawMessage(`{"version":"24"}`)
		item.UpdatedAt = now.Add(time.Minute)
		if err := repo.Update(ctx, item); err != nil {
			t.Fatalf("Update: %v", err)
		}

		got, err := repo.Get(ctx, "inv-1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Name != "new" {
			t.Errorf("Name = %q, want new", got.Name)
		}
	})

	t.Run("UpdateNotFound", func(t *testing.T) {
		repo := factory(t)
		err := repo.Update(context.Background(), domain.InventoryItem{ID: "missing", UpdatedAt: now})
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("got %v, want ErrNotFound", err)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		_ = repo.Create(ctx, domain.InventoryItem{ID: "inv-1", Type: "a", Name: "x", CreatedAt: now, UpdatedAt: now})
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
}
