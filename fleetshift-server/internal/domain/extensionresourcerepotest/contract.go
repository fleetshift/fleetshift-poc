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
	t.Run("Inventory", func(t *testing.T) { runInventoryTests(t, factory) })
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

		got, err := repo.Get(ctx, "//test.fleetshift.io/clusters/prod")
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

	t.Run("UniqueServiceNameResourceName", func(t *testing.T) {
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

	// CrossTypeSameNameUnique verifies the new uniqueness constraint:
	// two resources in the same service cannot share the same resource
	// name even if they have different resource types.
	t.Run("CrossTypeSameNameUnique", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ExtensionResources()

		seedType(t, tx, "test.fleetshift.io/Cluster")
		seedType(t, tx, "test.fleetshift.io/Database")

		fID := domain.FulfillmentID("f-er-cross")
		seedFulfillment(t, tx, fID, fixedTime)

		r1 := newER("test.fleetshift.io/Cluster", "resources/shared-name", fID)
		if err := repo.Create(ctx, r1); err != nil {
			t.Fatalf("first (Cluster): %v", err)
		}

		r2 := newER("test.fleetshift.io/Database", "resources/shared-name", fID)
		err := repo.Create(ctx, r2)
		if !errors.Is(err, domain.ErrAlreadyExists) {
			t.Fatalf("second (Database, same name): got %v, want ErrAlreadyExists", err)
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

		_, err := tx.ExtensionResources().Get(ctx, "//test.fleetshift.io/clusters/ghost")
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
		if err := repo.Delete(ctx, "//test.fleetshift.io/clusters/del"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		_, err := repo.Get(ctx, "//test.fleetshift.io/clusters/del")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("Get after delete: got %v, want ErrNotFound", err)
		}
	})

	t.Run("DeleteNotFound", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()

		err := tx.ExtensionResources().Delete(ctx, "//test.fleetshift.io/clusters/ghost")
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

		got, err := repo.Get(ctx, "//test.fleetshift.io/clusters/managed")
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

		got, err := repo.Get(ctx, "//test.fleetshift.io/clusters/labeled")
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

		got, err := repo.GetIntent(ctx, r.UID(), 1)
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

		_, err := tx.ExtensionResources().GetIntent(ctx, domain.NewExtensionResourceUID(), 99)
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("got %v, want ErrNotFound", err)
		}
	})

	// IntentsCascadeOnDelete verifies that ON DELETE CASCADE removes
	// intents when the parent extension resource is deleted.
	t.Run("IntentsCascadeOnDelete", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ExtensionResources()

		seedType(t, tx, "test.fleetshift.io/Cluster")
		fID := domain.FulfillmentID("f-intent-del")
		seedFulfillment(t, tx, fID, fixedTime)

		r := newER("test.fleetshift.io/Cluster", "clusters/intent-del", fID)
		uid := r.UID()
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if err := repo.Delete(ctx, "//test.fleetshift.io/clusters/intent-del"); err != nil {
			t.Fatalf("Delete: %v", err)
		}

		_, err := repo.GetIntent(ctx, uid, 1)
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("GetIntent after Delete: got %v, want ErrNotFound (CASCADE)", err)
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

		v, err := repo.GetView(ctx, "//test.fleetshift.io/clusters/view")
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

		_, err := tx.ExtensionResources().GetView(ctx, "//test.fleetshift.io/clusters/ghost")
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

// ---------------------------------------------------------------------------
// Inventory tests
// ---------------------------------------------------------------------------

func sampleInventoryType(rt domain.ResourceType) domain.ExtensionResourceType {
	typeName := rt.TypeName()
	if typeName == "" {
		typeName = string(rt)
	}
	return domain.NewExtensionResourceType(rt, "v1",
		domain.CollectionID(strings.ToLower(typeName)+"s"),
		fixedTime, domain.WithInventory())
}

func newInventoryER(rt domain.ResourceType, name domain.ResourceName) *domain.ExtensionResource {
	return domain.NewExtensionResource(
		domain.NewExtensionResourceUID(), rt, name, fixedTime)
}

func runInventoryTests(t *testing.T, factory Factory) {
	ctx := context.Background()

	t.Run("TypeCRUD", func(t *testing.T) {
		t.Run("CreateTypeWithInventoryMetadata", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			def := sampleInventoryType("inv.fleetshift.io/Node")
			if err := repo.CreateType(ctx, def); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			got, err := repo.GetType(ctx, "inv.fleetshift.io/Node")
			if err != nil {
				t.Fatalf("GetType: %v", err)
			}
			if got.Inventory() == nil {
				t.Fatal("Inventory is nil, want non-nil after round-trip")
			}
			if got.Management() != nil {
				t.Error("expected nil Management for inventory-only type")
			}
		})

		t.Run("CreateTypeManagedPlusInventory", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			def := domain.NewExtensionResourceType(
				"combo.fleetshift.io/Widget", "v1", "widgets", fixedTime,
				domain.WithManagement(
					domain.NewRegisteredSelfTarget("target-widget", "api.test.widget"),
					domain.Signature{
						Signer:         domain.FederatedIdentity{Subject: "addon-svc", Issuer: "https://issuer.test"},
						ContentHash:    []byte("hash"),
						SignatureBytes: []byte("sig"),
					},
				),
				domain.WithInventory(),
			)
			if err := repo.CreateType(ctx, def); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			got, err := repo.GetType(ctx, "combo.fleetshift.io/Widget")
			if err != nil {
				t.Fatalf("GetType: %v", err)
			}
			if got.Management() == nil {
				t.Fatal("Management is nil after round-trip")
			}
			if got.Inventory() == nil {
				t.Fatal("Inventory is nil after round-trip")
			}
		})
	})

	t.Run("Instances", func(t *testing.T) {
		t.Run("CreateInventoryOnlyResource", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			r := newInventoryER("inv.fleetshift.io/Node", "nodes/n1")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			got, err := repo.Get(ctx, "//inv.fleetshift.io/nodes/n1")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if got.Managed() != nil {
				t.Error("expected nil Managed for inventory-only resource")
			}
			assertEqual(t, "Name", got.Name(), domain.ResourceName("nodes/n1"))
		})

		t.Run("CreateManagedPlusInventoryResource", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			rt := domain.ResourceType("combo.fleetshift.io/Gadget")
			def := domain.NewExtensionResourceType(
				rt, "v1", "gadgets", fixedTime,
				domain.WithManagement(
					domain.NewRegisteredSelfTarget("target-gadget", "api.test.gadget"),
					domain.Signature{
						Signer:         domain.FederatedIdentity{Subject: "addon-svc", Issuer: "https://issuer.test"},
						ContentHash:    []byte("hash"),
						SignatureBytes: []byte("sig"),
					},
				),
				domain.WithInventory(),
			)
			if err := repo.CreateType(ctx, def); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			fID := domain.FulfillmentID("f-combo")
			seedFulfillment(t, tx, fID, fixedTime)

			r := newER(rt, "gadgets/g1", fID)
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			got, err := repo.Get(ctx, rt.FullName("gadgets/g1"))
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if got.Managed() == nil {
				t.Fatal("Managed is nil, want non-nil")
			}
			assertEqual(t, "FulfillmentID", got.Managed().FulfillmentID(), fID)
		})
	})

	t.Run("Upsert", func(t *testing.T) {
		t.Run("UpsertInventoryCreatesLatestState", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/upsert1")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			now := fixedTime.Add(time.Minute)
			invRes := domain.InventoryResourceFromSnapshot(domain.InventoryResourceSnapshot{
				Labels:      map[string]string{"zone": "us-east-1"},
				Observation: json.RawMessage(`{"cpu":4}`),
				Conditions: []domain.ConditionSnapshot{
					{Type: "Ready", Status: domain.ConditionTrue, Reason: "AllGood", Message: "ok", LastTransitionTime: now},
				},
				ObservedAt: now,
				UpdatedAt:  now,
			})

			err := repo.UpsertInventory(ctx, []domain.InventoryUpdate{{
				ExtensionResourceUID: r.UID(),
				Inventory:            *invRes,
			}})
			if err != nil {
				t.Fatalf("UpsertInventory: %v", err)
			}

			view, err := repo.GetView(ctx, "//inv.fleetshift.io/nodes/upsert1")
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			if view.Inventory == nil {
				t.Fatal("Inventory is nil after upsert")
			}
			assertEqual(t, "Labels[zone]", view.Inventory.Labels()["zone"], "us-east-1")
			assertEqual(t, "Observation", string(view.Inventory.Observation()), `{"cpu":4}`)
			if len(view.Inventory.Conditions()) != 1 {
				t.Fatalf("Conditions len = %d, want 1", len(view.Inventory.Conditions()))
			}
			assertEqual(t, "Condition.Type", view.Inventory.Conditions()[0].Type(), domain.ConditionType("Ready"))
		})

		t.Run("UpsertInventoryUpdatesExisting", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/upsert2")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			now := fixedTime.Add(time.Minute)
			inv1 := domain.InventoryResourceFromSnapshot(domain.InventoryResourceSnapshot{
				Observation: json.RawMessage(`{"cpu":2}`),
				ObservedAt:  now,
				UpdatedAt:   now,
			})
			if err := repo.UpsertInventory(ctx, []domain.InventoryUpdate{{
				ExtensionResourceUID: r.UID(),
				Inventory:            *inv1,
			}}); err != nil {
				t.Fatalf("first UpsertInventory: %v", err)
			}

			later := now.Add(time.Minute)
			inv2 := domain.InventoryResourceFromSnapshot(domain.InventoryResourceSnapshot{
				Observation: json.RawMessage(`{"cpu":8}`),
				ObservedAt:  later,
				UpdatedAt:   later,
			})
			if err := repo.UpsertInventory(ctx, []domain.InventoryUpdate{{
				ExtensionResourceUID: r.UID(),
				Inventory:            *inv2,
			}}); err != nil {
				t.Fatalf("second UpsertInventory: %v", err)
			}

			view, err := repo.GetView(ctx, "//inv.fleetshift.io/nodes/upsert2")
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			if view.Inventory == nil {
				t.Fatal("Inventory is nil after second upsert")
			}
			assertEqual(t, "Observation", string(view.Inventory.Observation()), `{"cpu":8}`)
		})

		t.Run("UpsertInventoryBatch", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r1 := newInventoryER("inv.fleetshift.io/Node", "nodes/batch1")
			r2 := newInventoryER("inv.fleetshift.io/Node", "nodes/batch2")
			for _, r := range []*domain.ExtensionResource{r1, r2} {
				if err := repo.Create(ctx, r); err != nil {
					t.Fatalf("Create %s: %v", r.Name(), err)
				}
			}

			now := fixedTime.Add(time.Minute)
			mkInv := func(obs string) domain.InventoryResource {
				return *domain.InventoryResourceFromSnapshot(domain.InventoryResourceSnapshot{
					Observation: json.RawMessage(obs),
					ObservedAt:  now,
					UpdatedAt:   now,
				})
			}
			err := repo.UpsertInventory(ctx, []domain.InventoryUpdate{
				{ExtensionResourceUID: r1.UID(), Inventory: mkInv(`{"n":1}`)},
				{ExtensionResourceUID: r2.UID(), Inventory: mkInv(`{"n":2}`)},
			})
			if err != nil {
				t.Fatalf("UpsertInventory batch: %v", err)
			}

			for _, tc := range []struct {
				name domain.ResourceName
				want string
			}{
				{"nodes/batch1", `{"n":1}`},
				{"nodes/batch2", `{"n":2}`},
			} {
				view, err := repo.GetView(ctx, domain.NewFullResourceName("inv.fleetshift.io", tc.name))
				if err != nil {
					t.Fatalf("GetView %s: %v", tc.name, err)
				}
				if view.Inventory == nil {
					t.Fatalf("Inventory for %s is nil", tc.name)
				}
				assertEqual(t, fmt.Sprintf("%s Observation", tc.name), string(view.Inventory.Observation()), tc.want)
			}
		})
	})

	t.Run("Views", func(t *testing.T) {
		t.Run("GetViewInventoryOnly", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/view1")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			now := fixedTime.Add(time.Minute)
			inv := domain.InventoryResourceFromSnapshot(domain.InventoryResourceSnapshot{
				Observation: json.RawMessage(`{"ready":true}`),
				ObservedAt:  now,
				UpdatedAt:   now,
			})
			if err := repo.UpsertInventory(ctx, []domain.InventoryUpdate{{
				ExtensionResourceUID: r.UID(),
				Inventory:            *inv,
			}}); err != nil {
				t.Fatalf("UpsertInventory: %v", err)
			}

			view, err := repo.GetView(ctx, "//inv.fleetshift.io/nodes/view1")
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			assertEqual(t, "Resource.Name", view.Resource.Name(), domain.ResourceName("nodes/view1"))
			if view.Intent != nil {
				t.Error("expected nil Intent for inventory-only resource")
			}
			if view.Fulfillment != nil {
				t.Error("expected nil Fulfillment for inventory-only resource")
			}
			if view.Inventory == nil {
				t.Fatal("Inventory is nil, want non-nil")
			}
			assertEqual(t, "Observation", string(view.Inventory.Observation()), `{"ready":true}`)
		})

		t.Run("ListViewsByTypeIncludesInventoryOnly", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			for _, name := range []domain.ResourceName{"nodes/lv1", "nodes/lv2"} {
				r := newInventoryER("inv.fleetshift.io/Node", name)
				if err := repo.Create(ctx, r); err != nil {
					t.Fatalf("Create %s: %v", name, err)
				}
			}

			views, err := repo.ListViewsByType(ctx, "inv.fleetshift.io/Node")
			if err != nil {
				t.Fatalf("ListViewsByType: %v", err)
			}
			if len(views) != 2 {
				t.Fatalf("len = %d, want 2", len(views))
			}
			for _, v := range views {
				if v.Intent != nil {
					t.Errorf("Intent is non-nil for inventory-only %s", v.Resource.Name())
				}
				if v.Fulfillment != nil {
					t.Errorf("Fulfillment is non-nil for inventory-only %s", v.Resource.Name())
				}
			}
		})

		t.Run("GetViewManagedPlusInventory", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			rt := domain.ResourceType("combo.fleetshift.io/Thing")
			def := domain.NewExtensionResourceType(
				rt, "v1", "things", fixedTime,
				domain.WithManagement(
					domain.NewRegisteredSelfTarget("target-thing", "api.test.thing"),
					domain.Signature{
						Signer:         domain.FederatedIdentity{Subject: "addon-svc", Issuer: "https://issuer.test"},
						ContentHash:    []byte("hash"),
						SignatureBytes: []byte("sig"),
					},
				),
				domain.WithInventory(),
			)
			if err := repo.CreateType(ctx, def); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			fID := domain.FulfillmentID("f-combo-view")
			seedFulfillment(t, tx, fID, fixedTime)

			r := newER(rt, "things/t1", fID)
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			now := fixedTime.Add(time.Minute)
			inv := domain.InventoryResourceFromSnapshot(domain.InventoryResourceSnapshot{
				Observation: json.RawMessage(`{"version":"2.0"}`),
				ObservedAt:  now,
				UpdatedAt:   now,
			})
			if err := repo.UpsertInventory(ctx, []domain.InventoryUpdate{{
				ExtensionResourceUID: r.UID(),
				Inventory:            *inv,
			}}); err != nil {
				t.Fatalf("UpsertInventory: %v", err)
			}

			view, err := repo.GetView(ctx, rt.FullName("things/t1"))
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			if view.Intent == nil {
				t.Fatal("Intent is nil for managed+inventory resource")
			}
			if view.Fulfillment == nil {
				t.Fatal("Fulfillment is nil for managed+inventory resource")
			}
			if view.Inventory == nil {
				t.Fatal("Inventory is nil for managed+inventory resource")
			}
			assertEqual(t, "Observation", string(view.Inventory.Observation()), `{"version":"2.0"}`)
		})

		t.Run("GetViewManagedStillRequiresIntentAndFulfillment", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			seedType(t, tx, "test.fleetshift.io/Cluster")
			fID := domain.FulfillmentID("f-managed-view")
			seedFulfillment(t, tx, fID, fixedTime)

			r := newER("test.fleetshift.io/Cluster", "clusters/managed-v", fID)
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			view, err := repo.GetView(ctx, "//test.fleetshift.io/clusters/managed-v")
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			if view.Intent == nil {
				t.Fatal("Intent is nil for managed resource")
			}
			if view.Fulfillment == nil {
				t.Fatal("Fulfillment is nil for managed resource")
			}
			if view.Inventory != nil {
				t.Error("expected nil Inventory for managed-only resource")
			}
		})
	})

	t.Run("History", func(t *testing.T) {
		t.Run("AppendAndListObservations", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/obs1")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			t1 := fixedTime.Add(time.Minute)
			t2 := fixedTime.Add(2 * time.Minute)
			obs := []domain.Observation{
				domain.NewObservation("obs-1", r.UID(), json.RawMessage(`{"v":1}`), t1, t1),
				domain.NewObservation("obs-2", r.UID(), json.RawMessage(`{"v":2}`), t2, t2),
			}
			if err := repo.AppendObservations(ctx, obs); err != nil {
				t.Fatalf("AppendObservations: %v", err)
			}

			got, err := repo.ListObservations(ctx, r.UID(), 10)
			if err != nil {
				t.Fatalf("ListObservations: %v", err)
			}
			if len(got) != 2 {
				t.Fatalf("len = %d, want 2", len(got))
			}
			// Most recent first (ordered by observed_at DESC)
			assertEqual(t, "got[0].ID", got[0].ID(), domain.ObservationID("obs-2"))
			assertEqual(t, "got[1].ID", got[1].ID(), domain.ObservationID("obs-1"))
			assertEqual(t, "got[0].Observation", string(got[0].Observation()), `{"v":2}`)
		})

		t.Run("RecordAndListConditionTransitions", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/ct1")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			t1 := fixedTime.Add(time.Minute)
			t2 := fixedTime.Add(2 * time.Minute)
			reports := []domain.ConditionReport{
				domain.NewConditionReport(r.UID(), "Ready", domain.ConditionFalse, "Starting", "not yet", t1, t1),
				domain.NewConditionReport(r.UID(), "Ready", domain.ConditionTrue, "AllGood", "ok", t2, t2),
			}
			if err := repo.RecordConditions(ctx, reports); err != nil {
				t.Fatalf("RecordConditions: %v", err)
			}

			got, err := repo.ListConditionTransitions(ctx, r.UID(), nil, 10)
			if err != nil {
				t.Fatalf("ListConditionTransitions: %v", err)
			}
			if len(got) != 2 {
				t.Fatalf("len = %d, want 2", len(got))
			}
			// Most recent first (ordered by observed_at DESC)
			assertEqual(t, "got[0].Status", got[0].Status(), domain.ConditionTrue)
			assertEqual(t, "got[1].Status", got[1].Status(), domain.ConditionFalse)
			// Auto-generated IDs should be non-empty and distinct
			if got[0].ID() == "" {
				t.Error("got[0].ID() is empty, want auto-generated")
			}
			if got[1].ID() == "" {
				t.Error("got[1].ID() is empty, want auto-generated")
			}
			if got[0].ID() == got[1].ID() {
				t.Errorf("got[0].ID() == got[1].ID() (%s); want distinct", got[0].ID())
			}
			// createdAt is set by the repo, not the reporter
			if got[0].CreatedAt().IsZero() {
				t.Error("got[0].CreatedAt() is zero, want repo-generated timestamp")
			}
		})

		t.Run("RecordConditionsDeduplicates", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/ct-dedup")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			t1 := fixedTime.Add(time.Minute)
			t2 := fixedTime.Add(2 * time.Minute)
			t3 := fixedTime.Add(3 * time.Minute)

			// First report: Ready=True
			if err := repo.RecordConditions(ctx, []domain.ConditionReport{
				domain.NewConditionReport(r.UID(), "Ready", domain.ConditionTrue, "AllGood", "ok", t1, t1),
			}); err != nil {
				t.Fatalf("first record: %v", err)
			}

			// Duplicate: same (type, status, reason, message) -- should be skipped
			if err := repo.RecordConditions(ctx, []domain.ConditionReport{
				domain.NewConditionReport(r.UID(), "Ready", domain.ConditionTrue, "AllGood", "ok", t2, t2),
			}); err != nil {
				t.Fatalf("duplicate record: %v", err)
			}

			got, err := repo.ListConditionTransitions(ctx, r.UID(), nil, 10)
			if err != nil {
				t.Fatalf("ListConditionTransitions: %v", err)
			}
			if len(got) != 1 {
				t.Fatalf("len = %d, want 1 (duplicate should be skipped)", len(got))
			}
			assertEqual(t, "got[0].Status", got[0].Status(), domain.ConditionTrue)

			// Genuine transition: different status
			if err := repo.RecordConditions(ctx, []domain.ConditionReport{
				domain.NewConditionReport(r.UID(), "Ready", domain.ConditionFalse, "Failed", "oops", t3, t3),
			}); err != nil {
				t.Fatalf("transition record: %v", err)
			}

			got, err = repo.ListConditionTransitions(ctx, r.UID(), nil, 10)
			if err != nil {
				t.Fatalf("ListConditionTransitions after transition: %v", err)
			}
			if len(got) != 2 {
				t.Fatalf("len = %d, want 2", len(got))
			}
			assertEqual(t, "got[0].Status", got[0].Status(), domain.ConditionFalse)
			assertEqual(t, "got[1].Status", got[1].Status(), domain.ConditionTrue)
		})

		t.Run("RecordConditionsReturnToPastState", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/ct-bounce")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			t1 := fixedTime.Add(time.Minute)
			t2 := fixedTime.Add(2 * time.Minute)
			t3 := fixedTime.Add(3 * time.Minute)

			// c1: Ready=True
			if err := repo.RecordConditions(ctx, []domain.ConditionReport{
				domain.NewConditionReport(r.UID(), "Ready", domain.ConditionTrue, "AllGood", "ok", t1, t1),
			}); err != nil {
				t.Fatalf("c1: %v", err)
			}

			// c2: Ready=False (genuine transition)
			if err := repo.RecordConditions(ctx, []domain.ConditionReport{
				domain.NewConditionReport(r.UID(), "Ready", domain.ConditionFalse, "Degraded", "something broke", t2, t2),
			}); err != nil {
				t.Fatalf("c2: %v", err)
			}

			// c1 again: Ready=True -- looks like c1 but the latest is c2,
			// so this is a genuine third transition and must NOT be dropped.
			if err := repo.RecordConditions(ctx, []domain.ConditionReport{
				domain.NewConditionReport(r.UID(), "Ready", domain.ConditionTrue, "AllGood", "ok", t3, t3),
			}); err != nil {
				t.Fatalf("c1 again: %v", err)
			}

			got, err := repo.ListConditionTransitions(ctx, r.UID(), nil, 10)
			if err != nil {
				t.Fatalf("ListConditionTransitions: %v", err)
			}
			if len(got) != 3 {
				t.Fatalf("len = %d, want 3 (return to past state is a genuine transition)", len(got))
			}
			// Most recent first
			assertEqual(t, "got[0].Status", got[0].Status(), domain.ConditionTrue)
			assertEqual(t, "got[0].Reason", got[0].Reason(), "AllGood")
			assertEqual(t, "got[1].Status", got[1].Status(), domain.ConditionFalse)
			assertEqual(t, "got[1].Reason", got[1].Reason(), "Degraded")
			assertEqual(t, "got[2].Status", got[2].Status(), domain.ConditionTrue)
			assertEqual(t, "got[2].Reason", got[2].Reason(), "AllGood")
		})

		t.Run("ListConditionTransitionsFilterByType", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/ct-filter")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			t1 := fixedTime.Add(time.Minute)
			reports := []domain.ConditionReport{
				domain.NewConditionReport(r.UID(), "Ready", domain.ConditionTrue, "ok", "", t1, t1),
				domain.NewConditionReport(r.UID(), "Provisioned", domain.ConditionTrue, "done", "", t1, t1),
			}
			if err := repo.RecordConditions(ctx, reports); err != nil {
				t.Fatalf("RecordConditions: %v", err)
			}

			readyType := domain.ConditionType("Ready")
			got, err := repo.ListConditionTransitions(ctx, r.UID(), &readyType, 10)
			if err != nil {
				t.Fatalf("ListConditionTransitions: %v", err)
			}
			if len(got) != 1 {
				t.Fatalf("len = %d, want 1", len(got))
			}
			assertEqual(t, "got[0].ConditionType", got[0].ConditionType(), domain.ConditionType("Ready"))
		})

		t.Run("UpsertInventoryProducesTransitions", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/upsert-trans")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			t1 := fixedTime.Add(time.Minute)
			inv := domain.InventoryResourceFromSnapshot(domain.InventoryResourceSnapshot{
				Observation: json.RawMessage(`{"cpu":4}`),
				Conditions: []domain.ConditionSnapshot{
					{Type: "Ready", Status: domain.ConditionTrue, Reason: "AllGood", Message: "ok", LastTransitionTime: t1},
				},
				ObservedAt: t1,
				UpdatedAt:  t1,
			})
			if err := repo.UpsertInventory(ctx, []domain.InventoryUpdate{{
				ExtensionResourceUID: r.UID(),
				Inventory:            *inv,
			}}); err != nil {
				t.Fatalf("UpsertInventory: %v", err)
			}

			// The condition from UpsertInventory should appear as a transition
			got, err := repo.ListConditionTransitions(ctx, r.UID(), nil, 10)
			if err != nil {
				t.Fatalf("ListConditionTransitions: %v", err)
			}
			if len(got) != 1 {
				t.Fatalf("len = %d, want 1", len(got))
			}
			assertEqual(t, "got[0].Status", got[0].Status(), domain.ConditionTrue)
			assertEqual(t, "got[0].Reason", got[0].Reason(), "AllGood")

			// A second upsert with the same condition should NOT produce another transition
			t2 := fixedTime.Add(2 * time.Minute)
			inv2 := domain.InventoryResourceFromSnapshot(domain.InventoryResourceSnapshot{
				Observation: json.RawMessage(`{"cpu":8}`),
				Conditions: []domain.ConditionSnapshot{
					{Type: "Ready", Status: domain.ConditionTrue, Reason: "AllGood", Message: "ok", LastTransitionTime: t1},
				},
				ObservedAt: t2,
				UpdatedAt:  t2,
			})
			if err := repo.UpsertInventory(ctx, []domain.InventoryUpdate{{
				ExtensionResourceUID: r.UID(),
				Inventory:            *inv2,
			}}); err != nil {
				t.Fatalf("second UpsertInventory: %v", err)
			}

			got, err = repo.ListConditionTransitions(ctx, r.UID(), nil, 10)
			if err != nil {
				t.Fatalf("ListConditionTransitions after dedup: %v", err)
			}
			if len(got) != 1 {
				t.Fatalf("len = %d, want 1 (duplicate should be deduplicated)", len(got))
			}
		})

		t.Run("UpsertInventoryGenuineTransition", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/upsert-genuine")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			t1 := fixedTime.Add(time.Minute)
			t2 := fixedTime.Add(2 * time.Minute)

			// First upsert: Ready=True
			inv1 := domain.InventoryResourceFromSnapshot(domain.InventoryResourceSnapshot{
				Observation: json.RawMessage(`{"cpu":4}`),
				Conditions: []domain.ConditionSnapshot{
					{Type: "Ready", Status: domain.ConditionTrue, Reason: "AllGood", Message: "ok", LastTransitionTime: t1},
				},
				ObservedAt: t1,
				UpdatedAt:  t1,
			})
			if err := repo.UpsertInventory(ctx, []domain.InventoryUpdate{{
				ExtensionResourceUID: r.UID(),
				Inventory:            *inv1,
			}}); err != nil {
				t.Fatalf("first UpsertInventory: %v", err)
			}

			// Second upsert: Ready=False (genuine transition via upsert)
			inv2 := domain.InventoryResourceFromSnapshot(domain.InventoryResourceSnapshot{
				Observation: json.RawMessage(`{"cpu":4}`),
				Conditions: []domain.ConditionSnapshot{
					{Type: "Ready", Status: domain.ConditionFalse, Reason: "Degraded", Message: "broke", LastTransitionTime: t2},
				},
				ObservedAt: t2,
				UpdatedAt:  t2,
			})
			if err := repo.UpsertInventory(ctx, []domain.InventoryUpdate{{
				ExtensionResourceUID: r.UID(),
				Inventory:            *inv2,
			}}); err != nil {
				t.Fatalf("second UpsertInventory: %v", err)
			}

			got, err := repo.ListConditionTransitions(ctx, r.UID(), nil, 10)
			if err != nil {
				t.Fatalf("ListConditionTransitions: %v", err)
			}
			if len(got) != 2 {
				t.Fatalf("len = %d, want 2 (two genuine upsert transitions)", len(got))
			}
			assertEqual(t, "got[0].Status", got[0].Status(), domain.ConditionFalse)
			assertEqual(t, "got[1].Status", got[1].Status(), domain.ConditionTrue)

			// Latest state should reflect the second upsert
			view, err := repo.GetView(ctx, "//inv.fleetshift.io/nodes/upsert-genuine")
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			if len(view.Inventory.Conditions()) != 1 {
				t.Fatalf("Conditions len = %d, want 1", len(view.Inventory.Conditions()))
			}
			assertEqual(t, "latest Status", view.Inventory.Conditions()[0].Status(), domain.ConditionFalse)
		})

		t.Run("ReportUpsertReport", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/report-upsert-report")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			t1 := fixedTime.Add(time.Minute)
			t2 := fixedTime.Add(2 * time.Minute)
			t3 := fixedTime.Add(3 * time.Minute)

			// Step 1: RecordConditions sets Ready=True
			if err := repo.RecordConditions(ctx, []domain.ConditionReport{
				domain.NewConditionReport(r.UID(), "Ready", domain.ConditionTrue, "AllGood", "ok", t1, t1),
			}); err != nil {
				t.Fatalf("RecordConditions: %v", err)
			}

			// Step 2: UpsertInventory transitions Ready=False
			inv := domain.InventoryResourceFromSnapshot(domain.InventoryResourceSnapshot{
				Observation: json.RawMessage(`{"cpu":4}`),
				Conditions: []domain.ConditionSnapshot{
					{Type: "Ready", Status: domain.ConditionFalse, Reason: "Degraded", Message: "broke", LastTransitionTime: t2},
				},
				ObservedAt: t2,
				UpdatedAt:  t2,
			})
			if err := repo.UpsertInventory(ctx, []domain.InventoryUpdate{{
				ExtensionResourceUID: r.UID(),
				Inventory:            *inv,
			}}); err != nil {
				t.Fatalf("UpsertInventory: %v", err)
			}

			// Step 3: RecordConditions transitions Ready=True again
			if err := repo.RecordConditions(ctx, []domain.ConditionReport{
				domain.NewConditionReport(r.UID(), "Ready", domain.ConditionTrue, "Recovered", "back", t3, t3),
			}); err != nil {
				t.Fatalf("RecordConditions again: %v", err)
			}

			got, err := repo.ListConditionTransitions(ctx, r.UID(), nil, 10)
			if err != nil {
				t.Fatalf("ListConditionTransitions: %v", err)
			}
			if len(got) != 3 {
				t.Fatalf("len = %d, want 3 (report -> upsert -> report)", len(got))
			}
			assertEqual(t, "got[0].Status", got[0].Status(), domain.ConditionTrue)
			assertEqual(t, "got[0].Reason", got[0].Reason(), "Recovered")
			assertEqual(t, "got[1].Status", got[1].Status(), domain.ConditionFalse)
			assertEqual(t, "got[2].Status", got[2].Status(), domain.ConditionTrue)
			assertEqual(t, "got[2].Reason", got[2].Reason(), "AllGood")

			// Latest state should reflect the final report
			view, err := repo.GetView(ctx, "//inv.fleetshift.io/nodes/report-upsert-report")
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			if len(view.Inventory.Conditions()) != 1 {
				t.Fatalf("Conditions len = %d, want 1", len(view.Inventory.Conditions()))
			}
			assertEqual(t, "latest Status", view.Inventory.Conditions()[0].Status(), domain.ConditionTrue)
			assertEqual(t, "latest Reason", view.Inventory.Conditions()[0].Reason(), "Recovered")
		})

		t.Run("RecordConditionsUpdatesLatestState", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/record-latest")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			t1 := fixedTime.Add(time.Minute)
			if err := repo.RecordConditions(ctx, []domain.ConditionReport{
				domain.NewConditionReport(r.UID(), "Ready", domain.ConditionTrue, "AllGood", "ok", t1, t1),
			}); err != nil {
				t.Fatalf("RecordConditions: %v", err)
			}

			// The condition should be visible via GetView (latest state)
			view, err := repo.GetView(ctx, "//inv.fleetshift.io/nodes/record-latest")
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			if view.Inventory == nil {
				t.Fatal("Inventory is nil; RecordConditions should create a minimal inventory row")
			}
			if len(view.Inventory.Conditions()) != 1 {
				t.Fatalf("Conditions len = %d, want 1", len(view.Inventory.Conditions()))
			}
			assertEqual(t, "Condition.Type", view.Inventory.Conditions()[0].Type(), domain.ConditionType("Ready"))
			assertEqual(t, "Condition.Status", view.Inventory.Conditions()[0].Status(), domain.ConditionTrue)
		})

		t.Run("RecordConditionsMultipleUIDsGetInventoryRows", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r1 := newInventoryER("inv.fleetshift.io/Node", "nodes/multi-a")
			r2 := newInventoryER("inv.fleetshift.io/Node", "nodes/multi-b")
			if err := repo.Create(ctx, r1); err != nil {
				t.Fatalf("Create r1: %v", err)
			}
			if err := repo.Create(ctx, r2); err != nil {
				t.Fatalf("Create r2: %v", err)
			}

			t1 := fixedTime.Add(time.Minute)
			// Single batch with reports for two different UIDs.
			if err := repo.RecordConditions(ctx, []domain.ConditionReport{
				domain.NewConditionReport(r1.UID(), "Ready", domain.ConditionTrue, "ok", "", t1, t1),
				domain.NewConditionReport(r2.UID(), "Ready", domain.ConditionTrue, "ok", "", t1, t1),
			}); err != nil {
				t.Fatalf("RecordConditions: %v", err)
			}

			// Both UIDs must have an inventory row.
			for _, tc := range []struct {
				name string
				rn   domain.ResourceName
			}{
				{"r1", "nodes/multi-a"},
				{"r2", "nodes/multi-b"},
			} {
				view, err := repo.GetView(ctx, domain.NewFullResourceName("inv.fleetshift.io", tc.rn))
				if err != nil {
					t.Fatalf("GetView %s: %v", tc.name, err)
				}
				if view.Inventory == nil {
					t.Fatalf("%s: Inventory is nil; RecordConditions should create inventory rows for all UIDs in the batch", tc.name)
				}
				if len(view.Inventory.Conditions()) != 1 {
					t.Fatalf("%s: Conditions len = %d, want 1", tc.name, len(view.Inventory.Conditions()))
				}
			}
		})

		// CrossPathConsistency exercises a sequence that alternates
		// between UpsertInventory and RecordConditions, mixing genuine
		// transitions with duplicates in both directions.
		//
		//   Step  Path    Condition         Transition?
		//   1     upsert  Ready=True        yes (first)
		//   2     report  Ready=True        no  (dedup: report sees upsert state)
		//   3     report  Ready=False       yes (genuine via report)
		//   4     upsert  Ready=False       no  (dedup: upsert sees report state)
		//   5     upsert  Ready=True        yes (genuine via upsert)
		//
		// Result: 3 transitions, latest state Ready=True.
		t.Run("CrossPathConsistency", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/cross-path")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			t1 := fixedTime.Add(1 * time.Minute)
			t2 := fixedTime.Add(2 * time.Minute)
			t3 := fixedTime.Add(3 * time.Minute)
			t4 := fixedTime.Add(4 * time.Minute)
			t5 := fixedTime.Add(5 * time.Minute)

			upsertWith := func(step string, status domain.ConditionStatus, reason, msg string, ts time.Time) {
				t.Helper()
				inv := domain.InventoryResourceFromSnapshot(domain.InventoryResourceSnapshot{
					Observation: json.RawMessage(`{"cpu":4}`),
					Conditions: []domain.ConditionSnapshot{
						{Type: "Ready", Status: status, Reason: reason, Message: msg, LastTransitionTime: ts},
					},
					ObservedAt: ts,
					UpdatedAt:  ts,
				})
				if err := repo.UpsertInventory(ctx, []domain.InventoryUpdate{{
					ExtensionResourceUID: r.UID(),
					Inventory:            *inv,
				}}); err != nil {
					t.Fatalf("%s UpsertInventory: %v", step, err)
				}
			}

			reportWith := func(step string, status domain.ConditionStatus, reason, msg string, ts time.Time) {
				t.Helper()
				if err := repo.RecordConditions(ctx, []domain.ConditionReport{
					domain.NewConditionReport(r.UID(), "Ready", status, reason, msg, ts, ts),
				}); err != nil {
					t.Fatalf("%s RecordConditions: %v", step, err)
				}
			}

			assertTransitionCount := func(step string, want int) {
				t.Helper()
				got, err := repo.ListConditionTransitions(ctx, r.UID(), nil, 10)
				if err != nil {
					t.Fatalf("%s ListConditionTransitions: %v", step, err)
				}
				if len(got) != want {
					t.Fatalf("%s transitions = %d, want %d", step, len(got), want)
				}
			}

			// Step 1: upsert Ready=True → genuine (first)
			upsertWith("step1", domain.ConditionTrue, "AllGood", "ok", t1)
			assertTransitionCount("step1", 1)

			// Step 2: report Ready=True → dedup (same state set by upsert)
			reportWith("step2", domain.ConditionTrue, "AllGood", "ok", t2)
			assertTransitionCount("step2", 1)

			// Step 3: report Ready=False → genuine transition via report
			reportWith("step3", domain.ConditionFalse, "Degraded", "broke", t3)
			assertTransitionCount("step3", 2)

			// Step 4: upsert Ready=False → dedup (same state set by report)
			upsertWith("step4", domain.ConditionFalse, "Degraded", "broke", t4)
			assertTransitionCount("step4", 2)

			// Step 5: upsert Ready=True → genuine transition via upsert
			upsertWith("step5", domain.ConditionTrue, "Recovered", "back", t5)
			assertTransitionCount("step5", 3)

			// Verify full transition history (most recent first)
			got, err := repo.ListConditionTransitions(ctx, r.UID(), nil, 10)
			if err != nil {
				t.Fatalf("final ListConditionTransitions: %v", err)
			}
			assertEqual(t, "got[0].Status", got[0].Status(), domain.ConditionTrue)
			assertEqual(t, "got[0].Reason", got[0].Reason(), "Recovered")
			assertEqual(t, "got[1].Status", got[1].Status(), domain.ConditionFalse)
			assertEqual(t, "got[1].Reason", got[1].Reason(), "Degraded")
			assertEqual(t, "got[2].Status", got[2].Status(), domain.ConditionTrue)
			assertEqual(t, "got[2].Reason", got[2].Reason(), "AllGood")

			// Latest state should reflect the final upsert
			view, err := repo.GetView(ctx, "//inv.fleetshift.io/nodes/cross-path")
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			if len(view.Inventory.Conditions()) != 1 {
				t.Fatalf("Conditions len = %d, want 1", len(view.Inventory.Conditions()))
			}
			assertEqual(t, "latest Status", view.Inventory.Conditions()[0].Status(), domain.ConditionTrue)
			assertEqual(t, "latest Reason", view.Inventory.Conditions()[0].Reason(), "Recovered")
		})
	})
}

func assertEqual[T comparable](t *testing.T, field string, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %v, want %v", field, got, want)
	}
}
