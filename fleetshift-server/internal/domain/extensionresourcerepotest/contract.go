// Package extensionresourcerepotest provides contract tests for
// [domain.ExtensionResourceRepository] implementations.
package extensionresourcerepotest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Factory creates a fresh [domain.Tx] for each test. The Tx is needed
// because extension resources reference fulfillments (foreign key in
// managed state).
type Factory func(t *testing.T) domain.Tx

// Run exercises the [domain.ExtensionResourceRepository] contract.
func Run(t *testing.T, factory Factory) {
	t.Run("Types", func(t *testing.T) { runTypeTests(t, factory) })
	t.Run("Instances", func(t *testing.T) { runInstanceTests(t, factory) })
	t.Run("Intents", func(t *testing.T) { runIntentTests(t, factory) })
	t.Run("Views", func(t *testing.T) { runViewTests(t, factory) })
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

var fixedTime = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

func seedFulfillment(t *testing.T, tx domain.Tx, fID domain.FulfillmentID, at time.Time) {
	t.Helper()
	ctx := context.Background()
	f := domain.FulfillmentFromSnapshot(domain.FulfillmentSnapshot{
		ID:        fID,
		State:     domain.FulfillmentStateCreating,
		CreatedAt: at,
		UpdatedAt: at,
	})
	f.AdvanceManifestStrategy(domain.ManifestStrategySpec{
		Type:      domain.ManifestStrategyInline,
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
	}, at)
	f.AdvancePlacementStrategy(domain.PlacementStrategySpec{
		Type:    domain.PlacementStrategyStatic,
		Targets: []domain.TargetID{"t1"},
	}, at)
	if err := tx.Fulfillments().Create(ctx, f); err != nil {
		t.Fatalf("seed fulfillment: %v", err)
	}
}

func sampleType(rt domain.ResourceType) domain.ExtensionResourceType {
	typeName := rt.TypeName()
	if typeName == "" {
		typeName = string(rt)
	}

	return domain.NewExtensionResourceType(
		rt, "v1",
		domain.CollectionID(strings.ToLower(typeName)+"s"),
		fixedTime,
		domain.WithManagement(
			domain.NewRegisteredSelfTarget(
				domain.TargetID("addon-"+typeName),
				domain.ManifestType("api.test."+strings.ToLower(typeName)),
			),
			domain.Signature{
				Signer:         domain.FederatedIdentity{Subject: "addon-svc", Issuer: "https://issuer.test"},
				ContentHash:    []byte("hash"),
				SignatureBytes: []byte("sig"),
			},
		),
	)
}

func seedType(t *testing.T, tx domain.Tx, rt domain.ResourceType) domain.ExtensionResourceType {
	t.Helper()
	def := sampleType(rt)
	if err := tx.ExtensionResources().CreateType(context.Background(), def); err != nil {
		t.Fatalf("seed type %s: %v", rt, err)
	}
	return def
}

// newER constructs an ExtensionResource with managed state and a single
// recorded intent, ready for Create to drain.
func newER(rt domain.ResourceType, name domain.ResourceName, fID domain.FulfillmentID) *domain.ExtensionResource {
	r := domain.NewExtensionResource(
		domain.NewExtensionResourceUID(), rt, name, fixedTime,
		domain.WithManagedState(fID),
	)
	r.RecordIntent(json.RawMessage(`{"provider":"rosa"}`), fixedTime)
	return r
}

// ---------------------------------------------------------------------------
// Type CRUD
// ---------------------------------------------------------------------------

func runTypeTests(t *testing.T, factory Factory) {
	ctx := context.Background()

	t.Run("CreateAndGet", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ExtensionResources()

		def := sampleType("kind.fleetshift.io/Cluster")
		if err := repo.CreateType(ctx, def); err != nil {
			t.Fatalf("CreateType: %v", err)
		}

		got, err := repo.GetType(ctx, "kind.fleetshift.io/Cluster")
		if err != nil {
			t.Fatalf("GetType: %v", err)
		}
		assertEqual(t, "ResourceType", got.ResourceType(), domain.ResourceType("kind.fleetshift.io/Cluster"))
		assertEqual(t, "APIServiceName", got.APIServiceName(), domain.ServiceName("kind.fleetshift.io"))
		assertEqual(t, "APIVersion", got.APIVersion(), domain.APIVersion("v1"))
		assertEqual(t, "CollectionID", got.CollectionID(), domain.CollectionID("clusters"))
		if !got.CreatedAt().Equal(fixedTime) {
			t.Errorf("CreatedAt = %v, want %v", got.CreatedAt(), fixedTime)
		}
		if got.Management() == nil {
			t.Fatal("Management is nil, want non-nil")
		}
		rst, ok := got.Management().Relation().(domain.RegisteredSelfTarget)
		if !ok {
			t.Fatalf("Relation type = %T, want RegisteredSelfTarget", got.Management().Relation())
		}
		assertEqual(t, "AddonTarget", rst.AddonTarget(), domain.TargetID("addon-Cluster"))
		assertEqual(t, "Signature.Signer.Subject", got.Management().Signature().Signer.Subject, "addon-svc")
	})

	t.Run("CreateDuplicate", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ExtensionResources()

		def := sampleType("kind.fleetshift.io/Cluster")
		if err := repo.CreateType(ctx, def); err != nil {
			t.Fatalf("first: %v", err)
		}
		err := repo.CreateType(ctx, def)
		if !errors.Is(err, domain.ErrAlreadyExists) {
			t.Fatalf("second: got %v, want ErrAlreadyExists", err)
		}
	})

	t.Run("GetNotFound", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()

		_, err := tx.ExtensionResources().GetType(ctx, "nonexistent")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("got %v, want ErrNotFound", err)
		}
	})

	t.Run("ListTypes", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ExtensionResources()

		for _, rt := range []domain.ResourceType{"test.fleetshift.io/Alpha", "test.fleetshift.io/Beta"} {
			if err := repo.CreateType(ctx, sampleType(rt)); err != nil {
				t.Fatalf("CreateType %s: %v", rt, err)
			}
		}
		defs, err := repo.ListTypes(ctx)
		if err != nil {
			t.Fatalf("ListTypes: %v", err)
		}
		if len(defs) != 2 {
			t.Fatalf("ListTypes len = %d, want 2", len(defs))
		}
	})

	t.Run("DeleteType", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ExtensionResources()

		rt := domain.ResourceType("test.fleetshift.io/Deletable")
		if err := repo.CreateType(ctx, sampleType(rt)); err != nil {
			t.Fatalf("CreateType: %v", err)
		}
		if err := repo.DeleteType(ctx, rt); err != nil {
			t.Fatalf("DeleteType: %v", err)
		}
		_, err := repo.GetType(ctx, rt)
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("GetType after delete: got %v, want ErrNotFound", err)
		}
	})

	t.Run("DeleteTypeNotFound", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()

		err := tx.ExtensionResources().DeleteType(ctx, "ghost")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("got %v, want ErrNotFound", err)
		}
	})

	t.Run("CreateTypeWithoutManagement", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ExtensionResources()

		def := domain.NewExtensionResourceType(
			"inv.fleetshift.io/Node", "v1", "nodes", fixedTime,
		)
		if err := repo.CreateType(ctx, def); err != nil {
			t.Fatalf("CreateType: %v", err)
		}
		got, err := repo.GetType(ctx, "inv.fleetshift.io/Node")
		if err != nil {
			t.Fatalf("GetType: %v", err)
		}
		if got.Management() != nil {
			t.Error("expected nil Management for inventory-only type")
		}
	})
}

// ---------------------------------------------------------------------------
// Instance CRUD
// ---------------------------------------------------------------------------

func runInstanceTests(t *testing.T, factory Factory) {
	ctx := context.Background()

	t.Run("CreateAndGet", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ExtensionResources()

		seedType(t, tx, "test.fleetshift.io/Cluster")
		fID := domain.FulfillmentID("f-er-create")
		seedFulfillment(t, tx, fID, fixedTime)

		r := newER("test.fleetshift.io/Cluster", "clusters/prod", fID)
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := repo.Get(ctx, "test.fleetshift.io/Cluster", "clusters/prod")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.UID().IsZero() {
			t.Error("UID is zero, want non-zero")
		}
		assertEqual(t, "ResourceType", got.ResourceType(), domain.ResourceType("test.fleetshift.io/Cluster"))
		assertEqual(t, "Name", got.Name(), domain.ResourceName("clusters/prod"))
		if got.Managed() == nil {
			t.Fatal("Managed is nil, want non-nil")
		}
		assertEqual(t, "FulfillmentID", got.Managed().FulfillmentID(), fID)
		assertEqual(t, "CurrentVersion", got.Managed().CurrentVersion(), domain.IntentVersion(1))
	})

	t.Run("GetByUID", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ExtensionResources()

		seedType(t, tx, "test.fleetshift.io/Cluster")
		fID := domain.FulfillmentID("f-er-uid")
		seedFulfillment(t, tx, fID, fixedTime)

		r := newER("test.fleetshift.io/Cluster", "clusters/by-uid", fID)
		uid := r.UID()
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := repo.GetByUID(ctx, uid)
		if err != nil {
			t.Fatalf("GetByUID: %v", err)
		}
		assertEqual(t, "Name", got.Name(), domain.ResourceName("clusters/by-uid"))
	})

	t.Run("GetByUID_NotFound", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()

		_, err := tx.ExtensionResources().GetByUID(ctx, domain.NewExtensionResourceUID())
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("got %v, want ErrNotFound", err)
		}
	})

	t.Run("UniqueResourceTypeName", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ExtensionResources()

		seedType(t, tx, "test.fleetshift.io/Cluster")
		fID := domain.FulfillmentID("f-er-dup")
		seedFulfillment(t, tx, fID, fixedTime)

		r1 := newER("test.fleetshift.io/Cluster", "clusters/dup", fID)
		if err := repo.Create(ctx, r1); err != nil {
			t.Fatalf("first: %v", err)
		}
		r2 := newER("test.fleetshift.io/Cluster", "clusters/dup", fID)
		err := repo.Create(ctx, r2)
		if !errors.Is(err, domain.ErrAlreadyExists) {
			t.Fatalf("second: got %v, want ErrAlreadyExists", err)
		}
	})

	t.Run("ListByResourceType", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ExtensionResources()

		seedType(t, tx, "test.fleetshift.io/Cluster")
		for i, name := range []domain.ResourceName{"clusters/a", "clusters/b"} {
			fID := domain.FulfillmentID(fmt.Sprintf("f-list-%d", i))
			seedFulfillment(t, tx, fID, fixedTime)
			r := newER("test.fleetshift.io/Cluster", name, fID)
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create %s: %v", name, err)
			}
		}

		list, err := repo.ListByResourceType(ctx, "test.fleetshift.io/Cluster")
		if err != nil {
			t.Fatalf("ListByResourceType: %v", err)
		}
		if len(list) != 2 {
			t.Fatalf("len = %d, want 2", len(list))
		}
	})

	t.Run("GetNotFound", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()

		_, err := tx.ExtensionResources().Get(ctx, "test.fleetshift.io/Cluster", "clusters/ghost")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("got %v, want ErrNotFound", err)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ExtensionResources()

		seedType(t, tx, "test.fleetshift.io/Cluster")
		fID := domain.FulfillmentID("f-er-del")
		seedFulfillment(t, tx, fID, fixedTime)

		r := newER("test.fleetshift.io/Cluster", "clusters/del", fID)
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if err := repo.Delete(ctx, "test.fleetshift.io/Cluster", "clusters/del"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		_, err := repo.Get(ctx, "test.fleetshift.io/Cluster", "clusters/del")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("Get after delete: got %v, want ErrNotFound", err)
		}
	})

	t.Run("DeleteNotFound", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()

		err := tx.ExtensionResources().Delete(ctx, "test.fleetshift.io/Cluster", "clusters/ghost")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("got %v, want ErrNotFound", err)
		}
	})

	t.Run("ManagedStateRoundTrip", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ExtensionResources()

		seedType(t, tx, "test.fleetshift.io/Cluster")
		fID := domain.FulfillmentID("f-er-managed")
		seedFulfillment(t, tx, fID, fixedTime)

		r := newER("test.fleetshift.io/Cluster", "clusters/managed", fID)
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := repo.Get(ctx, "test.fleetshift.io/Cluster", "clusters/managed")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Managed() == nil {
			t.Fatal("Managed is nil after round-trip")
		}
		assertEqual(t, "CurrentVersion", got.Managed().CurrentVersion(), domain.IntentVersion(1))
		assertEqual(t, "FulfillmentID", got.Managed().FulfillmentID(), fID)
	})

	t.Run("LabelsRoundTrip", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ExtensionResources()

		seedType(t, tx, "test.fleetshift.io/Cluster")
		fID := domain.FulfillmentID("f-er-labels")
		seedFulfillment(t, tx, fID, fixedTime)

		r := domain.NewExtensionResource(
			domain.NewExtensionResourceUID(),
			"test.fleetshift.io/Cluster", "clusters/labeled", fixedTime,
			domain.WithManagedState(fID),
			domain.WithExtensionLabels(map[string]string{"env": "prod", "tier": "1"}),
		)
		r.RecordIntent(json.RawMessage(`{}`), fixedTime)
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := repo.Get(ctx, "test.fleetshift.io/Cluster", "clusters/labeled")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		assertEqual(t, "Labels[env]", got.Labels()["env"], "prod")
		assertEqual(t, "Labels[tier]", got.Labels()["tier"], "1")
	})
}

// ---------------------------------------------------------------------------
// Intent read/delete
// ---------------------------------------------------------------------------

func runIntentTests(t *testing.T, factory Factory) {
	ctx := context.Background()

	t.Run("DrainedOnCreateAndGet", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ExtensionResources()

		seedType(t, tx, "test.fleetshift.io/Cluster")
		fID := domain.FulfillmentID("f-intent")
		seedFulfillment(t, tx, fID, fixedTime)

		r := newER("test.fleetshift.io/Cluster", "clusters/intent", fID)
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := repo.GetIntent(ctx, "test.fleetshift.io/Cluster", "clusters/intent", 1)
		if err != nil {
			t.Fatalf("GetIntent: %v", err)
		}
		assertEqual(t, "Version", got.Version, domain.IntentVersion(1))
		if string(got.Spec) != `{"provider":"rosa"}` {
			t.Errorf("Spec = %s, want {\"provider\":\"rosa\"}", got.Spec)
		}
	})

	t.Run("GetNotFound", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()

		_, err := tx.ExtensionResources().GetIntent(ctx, "test.fleetshift.io/Cluster", "nope", 99)
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("got %v, want ErrNotFound", err)
		}
	})

	t.Run("DeleteIntents", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ExtensionResources()

		seedType(t, tx, "test.fleetshift.io/Cluster")
		fID := domain.FulfillmentID("f-intent-del")
		seedFulfillment(t, tx, fID, fixedTime)

		r := newER("test.fleetshift.io/Cluster", "clusters/intent-del", fID)
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if err := repo.Delete(ctx, "test.fleetshift.io/Cluster", "clusters/intent-del"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if err := repo.DeleteIntents(ctx, "test.fleetshift.io/Cluster", "clusters/intent-del"); err != nil {
			t.Fatalf("DeleteIntents: %v", err)
		}

		_, err := repo.GetIntent(ctx, "test.fleetshift.io/Cluster", "clusters/intent-del", 1)
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("GetIntent after DeleteIntents: got %v, want ErrNotFound", err)
		}

		// Idempotent
		if err := repo.DeleteIntents(ctx, "test.fleetshift.io/Cluster", "clusters/intent-del"); err != nil {
			t.Fatalf("DeleteIntents second call: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Views (GetView / ListViewsByType)
// ---------------------------------------------------------------------------

func runViewTests(t *testing.T, factory Factory) {
	ctx := context.Background()

	t.Run("GetView", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ExtensionResources()

		seedType(t, tx, "test.fleetshift.io/Cluster")
		fID := domain.FulfillmentID("f-view")
		seedFulfillment(t, tx, fID, fixedTime)

		r := newER("test.fleetshift.io/Cluster", "clusters/view", fID)
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		v, err := repo.GetView(ctx, "test.fleetshift.io/Cluster", "clusters/view")
		if err != nil {
			t.Fatalf("GetView: %v", err)
		}
		assertEqual(t, "Resource.Name", v.Resource.Name(), domain.ResourceName("clusters/view"))
		if v.Intent == nil {
			t.Fatal("Intent is nil, want non-nil")
		}
		if string(v.Intent.Spec) != `{"provider":"rosa"}` {
			t.Errorf("Intent.Spec = %s", v.Intent.Spec)
		}
		if v.Fulfillment == nil {
			t.Fatal("Fulfillment is nil, want non-nil")
		}
		assertEqual(t, "Fulfillment.ID", v.Fulfillment.ID(), fID)
		assertEqual(t, "Fulfillment.State", v.Fulfillment.State(), domain.FulfillmentStateCreating)
	})

	t.Run("GetView_NotFound", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()

		_, err := tx.ExtensionResources().GetView(ctx, "test.fleetshift.io/Cluster", "clusters/ghost")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("got %v, want ErrNotFound", err)
		}
	})

	t.Run("ListViewsByType", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ExtensionResources()

		seedType(t, tx, "test.fleetshift.io/Cluster")
		for i, name := range []domain.ResourceName{"clusters/lv-a", "clusters/lv-b"} {
			fID := domain.FulfillmentID(fmt.Sprintf("f-lv-%d", i))
			seedFulfillment(t, tx, fID, fixedTime)
			r := newER("test.fleetshift.io/Cluster", name, fID)
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create %s: %v", name, err)
			}
		}

		views, err := repo.ListViewsByType(ctx, "test.fleetshift.io/Cluster")
		if err != nil {
			t.Fatalf("ListViewsByType: %v", err)
		}
		if len(views) != 2 {
			t.Fatalf("len = %d, want 2", len(views))
		}
		for _, v := range views {
			if v.Intent == nil {
				t.Errorf("Intent is nil for %s", v.Resource.Name())
			}
			if v.Fulfillment == nil {
				t.Errorf("Fulfillment is nil for %s", v.Resource.Name())
			}
		}
	})
}

func assertEqual[T comparable](t *testing.T, field string, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %v, want %v", field, got, want)
	}
}
