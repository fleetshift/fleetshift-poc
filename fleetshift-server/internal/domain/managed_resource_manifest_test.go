package domain_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

func TestManagedResourceManifestStrategy_ResolvesIntentFromStore(t *testing.T) {
	store, _ := setupStore(t)
	spec := json.RawMessage(`{"provider":"rosa","version":"4.16.2"}`)
	seedIntent(t, store, "clusters", "prod-us-east-1", spec)

	s := &domain.ManagedResourceManifestStrategy{
		Ref:   domain.IntentRef{ResourceType: "clusters", Name: "prod-us-east-1", Version: 1},
		Store: store,
	}

	got, err := s.Generate(context.Background(), domain.GenerateContext{
		Target: domain.TargetInfo{ID: "addon-target"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 manifest, got %d", len(got))
	}
	if got[0].ResourceType != "clusters" {
		t.Errorf("ResourceType = %q, want %q", got[0].ResourceType, "clusters")
	}
	if string(got[0].Raw) != string(spec) {
		t.Errorf("Raw = %s, want %s", got[0].Raw, spec)
	}
}

func TestManagedResourceManifestStrategy_IntentNotFound(t *testing.T) {
	store, _ := setupStore(t)

	s := &domain.ManagedResourceManifestStrategy{
		Ref:   domain.IntentRef{ResourceType: "clusters", Name: "missing", Version: 99},
		Store: store,
	}

	_, err := s.Generate(context.Background(), domain.GenerateContext{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestManagedResourceManifestStrategy_OnRemovedIsNoop(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	s := &domain.ManagedResourceManifestStrategy{Store: store}
	if err := s.OnRemoved(context.Background(), "t1"); err != nil {
		t.Fatalf("OnRemoved should be a no-op, got error: %v", err)
	}
}

// seedIntent creates a managed resource with a single intent version
// via the aggregate's RecordIntent method.
func seedIntent(t *testing.T, store domain.Store, rt domain.ResourceType, name domain.ResourceName, spec json.RawMessage) {
	t.Helper()
	now := time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)

	tx, err := store.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback()

	f := &domain.Fulfillment{
		ID:        domain.FulfillmentID("f-" + string(name)),
		State:     domain.FulfillmentStateCreating,
		CreatedAt: now,
		UpdatedAt: now,
	}
	f.AdvanceManifestStrategy(domain.ManifestStrategySpec{
		Type:      domain.ManifestStrategyManagedResource,
		IntentRef: domain.IntentRef{ResourceType: rt, Name: name, Version: 1},
	}, now)
	f.AdvancePlacementStrategy(domain.PlacementStrategySpec{
		Type:    domain.PlacementStrategyStatic,
		Targets: []domain.TargetID{"addon-target"},
	}, now)
	f.AdvanceRolloutStrategy(nil, now)
	if err := tx.Fulfillments().Create(context.Background(), f); err != nil {
		t.Fatalf("Create fulfillment: %v", err)
	}

	mr := &domain.ManagedResource{
		ResourceType:  rt,
		Name:          name,
		UID:           "uid-" + string(name),
		FulfillmentID: f.ID,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	mr.RecordIntent(spec, now)
	if err := tx.ManagedResources().CreateInstance(context.Background(), mr); err != nil {
		t.Fatalf("CreateInstance: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}
