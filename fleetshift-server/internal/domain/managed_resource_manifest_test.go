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
	seedIntent(t, store, "test.fleetshift.io/Cluster", "prod-us-east-1", spec)

	s := &domain.ManagedResourceManifestStrategy{
		Ref:   domain.IntentRef{ResourceType: "test.fleetshift.io/Cluster", Name: "prod-us-east-1", Version: 1, ManifestType: "clusters"},
		Store: store,
	}

	got, err := s.Generate(context.Background(), domain.GenerateContext{
		Target: domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "addon-target"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 manifest, got %d", len(got))
	}
	if got[0].ManifestType != "clusters" {
		t.Errorf("ManifestType = %q, want %q", got[0].ManifestType, "clusters")
	}
	if string(got[0].Raw) != string(spec) {
		t.Errorf("Raw = %s, want %s", got[0].Raw, spec)
	}
}

func TestManagedResourceManifestStrategy_IntentNotFound(t *testing.T) {
	store, _ := setupStore(t)

	s := &domain.ManagedResourceManifestStrategy{
		Ref:   domain.IntentRef{ResourceType: "test.fleetshift.io/Cluster", Name: "missing", Version: 99},
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

// seedIntent creates an extension resource with managed state and a
// single intent version via the aggregate's RecordIntent method.
func seedIntent(t *testing.T, store domain.Store, rt domain.ResourceType, name domain.ResourceName, spec json.RawMessage) {
	t.Helper()
	now := time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)

	tx, err := store.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback()

	relation := domain.NewRegisteredSelfTarget("addon-target", "managed-resource")
	ert := domain.NewExtensionResourceType(
		rt, "v1", "resources", now,
		domain.WithManagement(relation, domain.Signature{}),
	)
	if err := tx.ExtensionResources().CreateType(context.Background(), ert); err != nil {
		t.Fatalf("CreateType: %v", err)
	}

	fID := domain.FulfillmentID("f-" + string(name))
	f := domain.FulfillmentFromSnapshot(domain.FulfillmentSnapshot{
		ID:        fID,
		State:     domain.FulfillmentStateCreating,
		CreatedAt: now,
		UpdatedAt: now,
	})
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

	er := domain.NewExtensionResource(
		domain.NewExtensionResourceUID(), rt, name, now,
		domain.WithManagedState(fID),
	)
	if _, err := er.RecordIntent(spec, now); err != nil {
		t.Fatalf("RecordIntent: %v", err)
	}
	if err := tx.ExtensionResources().Create(context.Background(), er); err != nil {
		t.Fatalf("Create extension resource: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}
