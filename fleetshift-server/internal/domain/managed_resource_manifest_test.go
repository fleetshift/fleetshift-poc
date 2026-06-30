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
	erUID := seedIntent(t, store, "test.fleetshift.io/Cluster", "prod-us-east-1", spec)

	s := &domain.ManagedResourceManifestStrategy{
		Ref:   domain.IntentRef{ExtensionResourceUID: erUID, Version: 1, ManifestType: "clusters"},
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
	envelope, err := domain.UnwrapManifestEnvelope(got[0].Raw)
	if err != nil {
		t.Fatalf("UnwrapManifestEnvelope() error = %v", err)
	}
	if envelope.Name != "prod-us-east-1" {
		t.Errorf("envelope.Name = %q, want %q", envelope.Name, "prod-us-east-1")
	}
	if string(envelope.Spec) != string(spec) {
		t.Errorf("envelope.Spec = %s, want %s", envelope.Spec, spec)
	}
}

func TestManagedResourceManifestStrategy_IntentNotFound(t *testing.T) {
	store, _ := setupStore(t)

	s := &domain.ManagedResourceManifestStrategy{
		Ref:   domain.IntentRef{ExtensionResourceUID: domain.NewExtensionResourceUID(), Version: 99},
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
// It returns the extension resource UID for use in IntentRef.
func seedIntent(t *testing.T, store domain.Store, rt domain.ResourceType, name domain.ResourceName, spec json.RawMessage) domain.ExtensionResourceUID {
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

	erUID := domain.NewExtensionResourceUID()
	fID := domain.FulfillmentID("f-" + string(name))
	f := domain.FulfillmentFromSnapshot(domain.FulfillmentSnapshot{
		ID:        fID,
		State:     domain.FulfillmentStateCreating,
		CreatedAt: now,
		UpdatedAt: now,
	})
	f.AdvanceManifestStrategy(domain.ManifestStrategySpec{
		Type:      domain.ManifestStrategyManagedResource,
		IntentRef: domain.IntentRef{ExtensionResourceUID: erUID, Version: 1},
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
		erUID, rt, name, now,
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
	return erUID
}
