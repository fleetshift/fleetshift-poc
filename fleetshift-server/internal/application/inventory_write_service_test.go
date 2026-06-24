package application_test

import (
	"context"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

func TestInventoryWriteService_ApplyDelta_UpsertAndDelete(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}
	svc := application.NewInventoryWriteService(store)
	ctx := context.Background()

	now := time.Now()
	items := []domain.InventoryItem{
		domain.NewObservedInventoryItem("target-a/uid-1", "apps/v1/Deployment", "nginx", nil, map[string]string{"app": "nginx"}, "target-a", nil, nil, now, now),
		domain.NewObservedInventoryItem("target-a/uid-2", "v1/Service", "nginx-svc", nil, nil, "target-a", nil, nil, now, now),
	}

	if err := svc.ApplyDelta(ctx, "target-a", items, nil, nil, nil); err != nil {
		t.Fatalf("ApplyDelta upsert: %v", err)
	}

	// Verify both exist.
	tx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin read tx: %v", err)
	}
	item1, err := tx.Inventory().Get(ctx, "target-a/uid-1")
	if err != nil {
		t.Fatalf("get uid-1: %v", err)
	}
	if item1.Name() != "nginx" {
		t.Errorf("name = %q, want nginx", item1.Name())
	}
	tx.Rollback()

	// Now delete uid-2.
	if err := svc.ApplyDelta(ctx, "target-a", nil, []domain.InventoryItemID{"target-a/uid-2"}, nil, nil); err != nil {
		t.Fatalf("ApplyDelta delete: %v", err)
	}

	tx2, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin read tx: %v", err)
	}
	defer tx2.Rollback()

	_, err = tx2.Inventory().Get(ctx, "target-a/uid-2")
	if err == nil {
		t.Fatal("expected uid-2 to be deleted")
	}

	// uid-1 should still exist.
	_, err = tx2.Inventory().Get(ctx, "target-a/uid-1")
	if err != nil {
		t.Fatalf("uid-1 should still exist: %v", err)
	}
}

func TestInventoryWriteService_Resync_ReplacesExisting(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}
	svc := application.NewInventoryWriteService(store)
	ctx := context.Background()

	now := time.Now()

	// Pre-insert an item that should be replaced by resync.
	preItem := domain.NewObservedInventoryItem("target-b/old-uid", "apps/v1/Deployment", "old-deploy", nil, nil, "target-b", nil, nil, now, now)
	preTx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin pre-tx: %v", err)
	}
	if err := preTx.Inventory().CreateOrUpdate(ctx, preItem); err != nil {
		t.Fatalf("pre-insert: %v", err)
	}
	if err := preTx.Commit(); err != nil {
		t.Fatalf("pre-commit: %v", err)
	}

	// Resync with 2 new items.
	newItems := []domain.InventoryItem{
		domain.NewObservedInventoryItem("target-b/uid-a", "apps/v1/Deployment", "deploy-a", nil, nil, "target-b", nil, nil, now, now),
		domain.NewObservedInventoryItem("target-b/uid-b", "apps/v1/Deployment", "deploy-b", nil, nil, "target-b", nil, nil, now, now),
	}

	if err := svc.Resync(ctx, "target-b", "apps/v1/Deployment", newItems); err != nil {
		t.Fatalf("Resync: %v", err)
	}

	// Verify: uid-a and uid-b exist, old-uid is gone.
	tx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin read tx: %v", err)
	}
	defer tx.Rollback()

	items, err := tx.Inventory().ListByType(ctx, "apps/v1/Deployment")
	if err != nil {
		t.Fatalf("list by type: %v", err)
	}

	names := make(map[string]bool)
	for _, item := range items {
		names[item.Name()] = true
	}

	if !names["deploy-a"] {
		t.Error("expected deploy-a to exist after resync")
	}
	if !names["deploy-b"] {
		t.Error("expected deploy-b to exist after resync")
	}
	if names["old-deploy"] {
		t.Error("expected old-deploy to be replaced by resync")
	}
}

func TestInventoryWriteService_ApplyDelta_Edges(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}
	svc := application.NewInventoryWriteService(store)
	ctx := context.Background()

	edges := []domain.InventoryEdge{
		{EdgeType: "ownedBy", SourceUID: "pod-1", DestUID: "rs-1", SourceKind: "Pod", DestKind: "ReplicaSet"},
		{EdgeType: "runsOn", SourceUID: "pod-1", DestUID: "node-1", SourceKind: "Pod", DestKind: "Node"},
	}

	if err := svc.ApplyDelta(ctx, "target-a", nil, nil, edges, nil); err != nil {
		t.Fatalf("ApplyDelta edges: %v", err)
	}

	// Delete one edge.
	if err := svc.ApplyDelta(ctx, "target-a", nil, nil, nil, edges[:1]); err != nil {
		t.Fatalf("ApplyDelta edge delete: %v", err)
	}
}
