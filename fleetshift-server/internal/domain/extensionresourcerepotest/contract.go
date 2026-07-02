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

// wallClockDistantPast is a sentinel ReceivedAt value used to prove
// inventory writes use the caller-supplied ReceivedAt for timestamps
// rather than computing them from time.Now() internally: it is far
// enough in the past that it can never collide with a real wall-clock
// read.
var wallClockDistantPast = time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)

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

	// InventoryRoundTrip verifies that Get, GetByUID, and
	// ListByResourceType all hydrate ExtensionResource.Inventory after
	// ReplaceInventory has written inventory state.
	t.Run("InventoryRoundTrip", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ExtensionResources()

		rt := domain.ResourceType("inv.fleetshift.io/Node")
		if err := repo.CreateType(ctx, sampleInventoryType(rt)); err != nil {
			t.Fatalf("CreateType: %v", err)
		}
		r := newInventoryER(rt, "nodes/inv-rt")
		if err := repo.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}

		now := fixedTime.Add(time.Minute)
		obs := json.RawMessage(`{"cpu":4}`)
		if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
			ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
			Labels:      map[string]string{"zone": "us-east-1"},
			Observation: &obs,
			Conditions:  []domain.Condition{mustCondition(t, "Ready", domain.ConditionTrue, "AllGood", "ok", now)},
			ObservedAt:  now,
			ReceivedAt:  now,
		}}); err != nil {
			t.Fatalf("ReplaceInventory: %v", err)
		}

		assertInventory := func(label string, got *domain.ExtensionResource) {
			t.Helper()
			if got.Inventory() == nil {
				t.Fatalf("%s: Inventory is nil after round-trip", label)
			}
			assertEqual(t, label+" Labels[zone]", got.Inventory().Labels()["zone"], "us-east-1")
			assertObservation(t, label+" Observation", got.Inventory().Observation(), `{"cpu":4}`)
			if !got.Inventory().ObservedAt().Equal(now) {
				t.Errorf("%s: ObservedAt = %v, want %v", label, got.Inventory().ObservedAt(), now)
			}
			if len(got.Inventory().Conditions()) != 1 {
				t.Fatalf("%s: Conditions len = %d, want 1", label, len(got.Inventory().Conditions()))
			}
			assertEqual(t, label+" Condition.Type", got.Inventory().Conditions()[0].Type(), domain.ConditionType("Ready"))
			assertEqual(t, label+" Condition.Status", got.Inventory().Conditions()[0].Status(), domain.ConditionTrue)
		}

		byName, err := repo.Get(ctx, rt.FullName("nodes/inv-rt"))
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		assertInventory("Get", byName)

		byUID, err := repo.GetByUID(ctx, r.UID())
		if err != nil {
			t.Fatalf("GetByUID: %v", err)
		}
		assertInventory("GetByUID", byUID)

		list, err := repo.ListByResourceType(ctx, rt)
		if err != nil {
			t.Fatalf("ListByResourceType: %v", err)
		}
		if len(list) != 1 {
			t.Fatalf("ListByResourceType len = %d, want 1", len(list))
		}
		assertInventory("ListByResourceType", list[0])
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

	t.Run("Replace", func(t *testing.T) {
		t.Run("CreatesLatestStateObservationHistoryAndTransitions", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/replace1")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			now := fixedTime.Add(time.Minute)
			obs := json.RawMessage(`{"cpu":4}`)
			_, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Labels:      map[string]string{"zone": "us-east-1"},
				Observation: &obs,
				Conditions:  []domain.Condition{mustCondition(t, "Ready", domain.ConditionTrue, "AllGood", "ok", now)},
				ObservedAt:  now,
				ReceivedAt:  now,
			}})
			if err != nil {
				t.Fatalf("ReplaceInventory: %v", err)
			}

			view, err := repo.GetView(ctx, "//inv.fleetshift.io/nodes/replace1")
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			if view.Resource.Inventory() == nil {
				t.Fatal("Inventory is nil after replace")
			}
			assertEqual(t, "Labels[zone]", view.Resource.Inventory().Labels()["zone"], "us-east-1")
			assertObservation(t, "Observation", view.Resource.Inventory().Observation(), `{"cpu":4}`)
			if len(view.Resource.Inventory().Conditions()) != 1 {
				t.Fatalf("Conditions len = %d, want 1", len(view.Resource.Inventory().Conditions()))
			}
			assertEqual(t, "Condition.Type", view.Resource.Inventory().Conditions()[0].Type(), domain.ConditionType("Ready"))

			obsHistory, err := repo.ListObservations(ctx, r.UID(), 10)
			if err != nil {
				t.Fatalf("ListObservations: %v", err)
			}
			if len(obsHistory) != 1 {
				t.Fatalf("observation history len = %d, want 1", len(obsHistory))
			}
			assertEqual(t, "history Observation", string(obsHistory[0].Observation()), `{"cpu":4}`)

			transitions, err := repo.ListConditionTransitions(ctx, r.UID(), nil, 10)
			if err != nil {
				t.Fatalf("ListConditionTransitions: %v", err)
			}
			if len(transitions) != 1 {
				t.Fatalf("transitions len = %d, want 1 (initial condition)", len(transitions))
			}
			assertEqual(t, "transition Status", transitions[0].Status(), domain.ConditionTrue)
		})

		t.Run("UpdatesExisting", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/replace2")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			now := fixedTime.Add(time.Minute)
			obs1 := json.RawMessage(`{"cpu":2}`)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: &obs1,
				ObservedAt:  now,
				ReceivedAt:  now,
			}}); err != nil {
				t.Fatalf("first ReplaceInventory: %v", err)
			}

			later := now.Add(time.Minute)
			obs2 := json.RawMessage(`{"cpu":8}`)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: &obs2,
				ObservedAt:  later,
				ReceivedAt:  later,
			}}); err != nil {
				t.Fatalf("second ReplaceInventory: %v", err)
			}

			view, err := repo.GetView(ctx, "//inv.fleetshift.io/nodes/replace2")
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			if view.Resource.Inventory() == nil {
				t.Fatal("Inventory is nil after second replace")
			}
			assertObservation(t, "Observation", view.Resource.Inventory().Observation(), `{"cpu":8}`)
		})

		t.Run("Batch", func(t *testing.T) {
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
			obs1 := json.RawMessage(`{"n":1}`)
			obs2 := json.RawMessage(`{"n":2}`)
			_, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{
				{ResourceType: r1.ResourceType(), Name: r1.Name(), CandidateUID: domain.NewExtensionResourceUID(), Observation: &obs1, ObservedAt: now, ReceivedAt: now},
				{ResourceType: r2.ResourceType(), Name: r2.Name(), CandidateUID: domain.NewExtensionResourceUID(), Observation: &obs2, ObservedAt: now, ReceivedAt: now},
			})
			if err != nil {
				t.Fatalf("ReplaceInventory batch: %v", err)
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
				if view.Resource.Inventory() == nil {
					t.Fatalf("Inventory for %s is nil", tc.name)
				}
				assertObservation(t, fmt.Sprintf("%s Observation", tc.name), view.Resource.Inventory().Observation(), tc.want)
			}
		})

		t.Run("SameConditionDoesNotDuplicateTransition", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/replace-dedup")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			t1 := fixedTime.Add(time.Minute)
			t2 := fixedTime.Add(2 * time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Conditions: []domain.Condition{mustCondition(t, "Ready", domain.ConditionTrue, "AllGood", "ok", t1)},
				ObservedAt: t1,
				ReceivedAt: t1,
			}}); err != nil {
				t.Fatalf("first ReplaceInventory: %v", err)
			}
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Conditions: []domain.Condition{mustCondition(t, "Ready", domain.ConditionTrue, "AllGood", "ok", t2)},
				ObservedAt: t2,
				ReceivedAt: t2,
			}}); err != nil {
				t.Fatalf("second ReplaceInventory: %v", err)
			}

			got, err := repo.ListConditionTransitions(ctx, r.UID(), nil, 10)
			if err != nil {
				t.Fatalf("ListConditionTransitions: %v", err)
			}
			if len(got) != 1 {
				t.Fatalf("len = %d, want 1 (repeated condition should not duplicate)", len(got))
			}
		})

		t.Run("ChangedConditionRecordsTransition", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/replace-genuine")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			t1 := fixedTime.Add(time.Minute)
			t2 := fixedTime.Add(2 * time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Conditions: []domain.Condition{mustCondition(t, "Ready", domain.ConditionTrue, "AllGood", "ok", t1)},
				ObservedAt: t1,
				ReceivedAt: t1,
			}}); err != nil {
				t.Fatalf("first ReplaceInventory: %v", err)
			}
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Conditions: []domain.Condition{mustCondition(t, "Ready", domain.ConditionFalse, "Degraded", "broke", t2)},
				ObservedAt: t2,
				ReceivedAt: t2,
			}}); err != nil {
				t.Fatalf("second ReplaceInventory: %v", err)
			}

			got, err := repo.ListConditionTransitions(ctx, r.UID(), nil, 10)
			if err != nil {
				t.Fatalf("ListConditionTransitions: %v", err)
			}
			if len(got) != 2 {
				t.Fatalf("len = %d, want 2 (changed condition is a genuine transition)", len(got))
			}
			assertEqual(t, "got[0].Status", got[0].Status(), domain.ConditionFalse)
			assertEqual(t, "got[1].Status", got[1].Status(), domain.ConditionTrue)

			view, err := repo.GetView(ctx, "//inv.fleetshift.io/nodes/replace-genuine")
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			if len(view.Resource.Inventory().Conditions()) != 1 {
				t.Fatalf("Conditions len = %d, want 1", len(view.Resource.Inventory().Conditions()))
			}
			assertEqual(t, "latest Status", view.Resource.Inventory().Conditions()[0].Status(), domain.ConditionFalse)
		})

		t.Run("ReturnToPastStateRecordsGenuineTransition", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/replace-bounce")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			t1 := fixedTime.Add(time.Minute)
			t2 := fixedTime.Add(2 * time.Minute)
			t3 := fixedTime.Add(3 * time.Minute)

			replaceWith := func(step string, status domain.ConditionStatus, reason, msg string, ts time.Time) {
				t.Helper()
				if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
					ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
					Conditions: []domain.Condition{mustCondition(t, "Ready", status, reason, msg, ts)},
					ObservedAt: ts,
					ReceivedAt: ts,
				}}); err != nil {
					t.Fatalf("%s ReplaceInventory: %v", step, err)
				}
			}

			replaceWith("c1", domain.ConditionTrue, "AllGood", "ok", t1)
			replaceWith("c2", domain.ConditionFalse, "Degraded", "broke", t2)
			// c1 again: looks like c1 but the latest is c2, so this is a
			// genuine third transition and must not be dropped.
			replaceWith("c3", domain.ConditionTrue, "AllGood", "ok", t3)

			got, err := repo.ListConditionTransitions(ctx, r.UID(), nil, 10)
			if err != nil {
				t.Fatalf("ListConditionTransitions: %v", err)
			}
			if len(got) != 3 {
				t.Fatalf("len = %d, want 3 (return to past state is a genuine transition)", len(got))
			}
			assertEqual(t, "got[0].Status", got[0].Status(), domain.ConditionTrue)
			assertEqual(t, "got[1].Status", got[1].Status(), domain.ConditionFalse)
			assertEqual(t, "got[2].Status", got[2].Status(), domain.ConditionTrue)
		})

		t.Run("RemovesConditionsAbsentFromReplacement", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/replace-remove-cond")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			t1 := fixedTime.Add(time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Conditions: []domain.Condition{
					mustCondition(t, "Ready", domain.ConditionTrue, "AllGood", "ok", t1),
					mustCondition(t, "Provisioned", domain.ConditionTrue, "Done", "done", t1),
				},
				ObservedAt: t1,
				ReceivedAt: t1,
			}}); err != nil {
				t.Fatalf("first ReplaceInventory: %v", err)
			}

			t2 := fixedTime.Add(2 * time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Conditions: []domain.Condition{mustCondition(t, "Ready", domain.ConditionTrue, "AllGood", "ok", t1)},
				ObservedAt: t2,
				ReceivedAt: t2,
			}}); err != nil {
				t.Fatalf("second ReplaceInventory: %v", err)
			}

			view, err := repo.GetView(ctx, "//inv.fleetshift.io/nodes/replace-remove-cond")
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			if len(view.Resource.Inventory().Conditions()) != 1 {
				t.Fatalf("Conditions len = %d, want 1 (Provisioned should be removed)", len(view.Resource.Inventory().Conditions()))
			}
			assertEqual(t, "remaining Condition.Type", view.Resource.Inventory().Conditions()[0].Type(), domain.ConditionType("Ready"))
		})

		t.Run("RemovesLabelsAbsentFromReplacement", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/replace-remove-label")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			t1 := fixedTime.Add(time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Labels:     map[string]string{"zone": "us-east-1", "team": "platform"},
				ObservedAt: t1,
				ReceivedAt: t1,
			}}); err != nil {
				t.Fatalf("first ReplaceInventory: %v", err)
			}

			t2 := fixedTime.Add(2 * time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Labels:     map[string]string{"zone": "us-east-1"},
				ObservedAt: t2,
				ReceivedAt: t2,
			}}); err != nil {
				t.Fatalf("second ReplaceInventory: %v", err)
			}

			view, err := repo.GetView(ctx, "//inv.fleetshift.io/nodes/replace-remove-label")
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			labels := view.Resource.Inventory().Labels()
			if len(labels) != 1 {
				t.Fatalf("Labels len = %d, want 1 (team should be removed): %v", len(labels), labels)
			}
			assertEqual(t, "Labels[zone]", labels["zone"], "us-east-1")
			if _, ok := labels["team"]; ok {
				t.Errorf("Labels[team] still present, want removed")
			}
		})

		t.Run("NilObservationLeavesLatestUnchanged", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/replace-nil-obs")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			t1 := fixedTime.Add(time.Minute)
			obs := json.RawMessage(`{"cpu":4}`)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: &obs,
				ObservedAt:  t1,
				ReceivedAt:  t1,
			}}); err != nil {
				t.Fatalf("first ReplaceInventory: %v", err)
			}

			t2 := fixedTime.Add(2 * time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: nil,
				ObservedAt:  t2,
				ReceivedAt:  t2,
			}}); err != nil {
				t.Fatalf("second ReplaceInventory (nil observation): %v", err)
			}

			view, err := repo.GetView(ctx, "//inv.fleetshift.io/nodes/replace-nil-obs")
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			assertObservation(t, "Observation", view.Resource.Inventory().Observation(), `{"cpu":4}`)

			// A nil observation must not append history.
			hist, err := repo.ListObservations(ctx, r.UID(), 10)
			if err != nil {
				t.Fatalf("ListObservations: %v", err)
			}
			if len(hist) != 1 {
				t.Fatalf("history len = %d, want 1 (only the first non-nil observation)", len(hist))
			}
		})

		t.Run("NullLiteralObservationLeavesLatestUnchanged", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/replace-null-literal-obs")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			t1 := fixedTime.Add(time.Minute)
			obs := json.RawMessage(`{"cpu":4}`)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: &obs,
				ObservedAt:  t1,
				ReceivedAt:  t1,
			}}); err != nil {
				t.Fatalf("first ReplaceInventory: %v", err)
			}

			// A non-nil pointer to the JSON literal null must behave
			// identically to a nil pointer: untouched, no history.
			nullLiteral := json.RawMessage(`null`)
			t2 := fixedTime.Add(2 * time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: &nullLiteral,
				ObservedAt:  t2,
				ReceivedAt:  t2,
			}}); err != nil {
				t.Fatalf("second ReplaceInventory (null literal observation): %v", err)
			}

			view, err := repo.GetView(ctx, "//inv.fleetshift.io/nodes/replace-null-literal-obs")
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			assertObservation(t, "Observation", view.Resource.Inventory().Observation(), `{"cpu":4}`)

			hist, err := repo.ListObservations(ctx, r.UID(), 10)
			if err != nil {
				t.Fatalf("ListObservations: %v", err)
			}
			if len(hist) != 1 {
				t.Fatalf("history len = %d, want 1 (null literal appends no history)", len(hist))
			}
		})

		t.Run("DeduplicatesIdenticalObservationHistory", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/replace-obs-dedup")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			t1 := fixedTime.Add(time.Minute)
			obs := json.RawMessage(`{"cpu":4}`)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: &obs,
				ObservedAt:  t1,
				ReceivedAt:  t1,
			}}); err != nil {
				t.Fatalf("first ReplaceInventory: %v", err)
			}

			// Byte-identical observation on a resync must not append a
			// second history row.
			t2 := fixedTime.Add(2 * time.Minute)
			sameObs := json.RawMessage(`{"cpu":4}`)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: &sameObs,
				ObservedAt:  t2,
				ReceivedAt:  t2,
			}}); err != nil {
				t.Fatalf("second ReplaceInventory (same observation): %v", err)
			}

			hist, err := repo.ListObservations(ctx, r.UID(), 10)
			if err != nil {
				t.Fatalf("ListObservations: %v", err)
			}
			if len(hist) != 1 {
				t.Fatalf("history len = %d, want 1 (identical observation must dedup)", len(hist))
			}

			// A genuinely different value appends a second row.
			t3 := fixedTime.Add(3 * time.Minute)
			diffObs := json.RawMessage(`{"cpu":8}`)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: &diffObs,
				ObservedAt:  t3,
				ReceivedAt:  t3,
			}}); err != nil {
				t.Fatalf("third ReplaceInventory (different observation): %v", err)
			}

			hist, err = repo.ListObservations(ctx, r.UID(), 10)
			if err != nil {
				t.Fatalf("ListObservations: %v", err)
			}
			if len(hist) != 2 {
				t.Fatalf("history len = %d, want 2 (different observation appends a row)", len(hist))
			}
			assertEqual(t, "hist[0].Observation", string(hist[0].Observation()), `{"cpu":8}`)
		})

		t.Run("RejectsUnregisteredResourceType", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			// No CreateType call for this type: ReplaceInventory
			// resolves-or-creates the extension_resources row itself
			// (there's no "unknown UID" to reject anymore), but that
			// insert's FK to extension_resource_types still rejects a
			// resource type that was never registered.
			now := fixedTime.Add(time.Minute)
			_, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Unregistered",
				Name:         "nodes/unregistered",
				CandidateUID: domain.NewExtensionResourceUID(),
				ObservedAt:   now,
				ReceivedAt:   now,
			}})
			if err == nil {
				t.Fatal("expected error for unregistered resource type, got nil")
			}
		})

		t.Run("UsesReceivedAtNotWallClock", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/replace-receivedat")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			observedAt := fixedTime.Add(time.Minute)
			receivedAt := wallClockDistantPast
			obs := json.RawMessage(`{"cpu":4}`)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: &obs,
				Conditions:  []domain.Condition{mustCondition(t, "Ready", domain.ConditionTrue, "AllGood", "ok", observedAt)},
				ObservedAt:  observedAt,
				ReceivedAt:  receivedAt,
			}}); err != nil {
				t.Fatalf("ReplaceInventory: %v", err)
			}

			view, err := repo.GetView(ctx, "//inv.fleetshift.io/nodes/replace-receivedat")
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			if !view.Resource.Inventory().UpdatedAt().Equal(receivedAt) {
				t.Errorf("Inventory.UpdatedAt = %v, want %v (ReceivedAt, not wall clock)", view.Resource.Inventory().UpdatedAt(), receivedAt)
			}

			hist, err := repo.ListObservations(ctx, r.UID(), 10)
			if err != nil {
				t.Fatalf("ListObservations: %v", err)
			}
			if len(hist) != 1 {
				t.Fatalf("history len = %d, want 1", len(hist))
			}
			if !hist[0].CreatedAt().Equal(receivedAt) {
				t.Errorf("observation CreatedAt = %v, want %v (ReceivedAt, not wall clock)", hist[0].CreatedAt(), receivedAt)
			}

			transitions, err := repo.ListConditionTransitions(ctx, r.UID(), nil, 10)
			if err != nil {
				t.Fatalf("ListConditionTransitions: %v", err)
			}
			if len(transitions) != 1 {
				t.Fatalf("transitions len = %d, want 1", len(transitions))
			}
			if !transitions[0].CreatedAt().Equal(receivedAt) {
				t.Errorf("transition CreatedAt = %v, want %v (ReceivedAt, not wall clock)", transitions[0].CreatedAt(), receivedAt)
			}
		})
	})

	t.Run("Delta", func(t *testing.T) {
		t.Run("SetsAndDeletesLabelsWithoutTouchingUnrelated", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/delta-labels")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			t1 := fixedTime.Add(time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Labels:     map[string]string{"zone": "us-east-1", "tier": "1"},
				ObservedAt: t1,
				ReceivedAt: t1,
			}}); err != nil {
				t.Fatalf("seed ReplaceInventory: %v", err)
			}

			t2 := fixedTime.Add(2 * time.Minute)
			if _, err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				SetLabels:    map[string]string{"zone": "us-west-2"},
				DeleteLabels: []string{"tier"},
				ObservedAt:   t2,
				ReceivedAt:   t2,
			}}); err != nil {
				t.Fatalf("ApplyInventoryDeltas: %v", err)
			}

			view, err := repo.GetView(ctx, "//inv.fleetshift.io/nodes/delta-labels")
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			labels := view.Resource.Inventory().Labels()
			assertEqual(t, "Labels[zone]", labels["zone"], "us-west-2")
			if _, ok := labels["tier"]; ok {
				t.Errorf("Labels[tier] = %q, want deleted", labels["tier"])
			}
		})

		t.Run("ObservationUnchanged", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/delta-obs-unchanged")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			t1 := fixedTime.Add(time.Minute)
			obs := json.RawMessage(`{"cpu":4}`)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: &obs,
				ObservedAt:  t1,
				ReceivedAt:  t1,
			}}); err != nil {
				t.Fatalf("seed ReplaceInventory: %v", err)
			}

			t2 := fixedTime.Add(2 * time.Minute)
			if _, err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				SetLabels:   map[string]string{"zone": "us-east-1"},
				Observation: nil,
				ObservedAt:  t2,
				ReceivedAt:  t2,
			}}); err != nil {
				t.Fatalf("ApplyInventoryDeltas: %v", err)
			}

			view, err := repo.GetView(ctx, "//inv.fleetshift.io/nodes/delta-obs-unchanged")
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			assertObservation(t, "Observation", view.Resource.Inventory().Observation(), `{"cpu":4}`)

			hist, err := repo.ListObservations(ctx, r.UID(), 10)
			if err != nil {
				t.Fatalf("ListObservations: %v", err)
			}
			if len(hist) != 1 {
				t.Fatalf("history len = %d, want 1 (unchanged appends no history)", len(hist))
			}
		})

		t.Run("ObservationReplace", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/delta-obs-replace")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			t1 := fixedTime.Add(time.Minute)
			obs1 := json.RawMessage(`{"cpu":4}`)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: &obs1,
				ObservedAt:  t1,
				ReceivedAt:  t1,
			}}); err != nil {
				t.Fatalf("seed ReplaceInventory: %v", err)
			}

			t2 := fixedTime.Add(2 * time.Minute)
			obs2 := json.RawMessage(`{"cpu":8}`)
			if _, err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: &obs2,
				ObservedAt:  t2,
				ReceivedAt:  t2,
			}}); err != nil {
				t.Fatalf("ApplyInventoryDeltas: %v", err)
			}

			view, err := repo.GetView(ctx, "//inv.fleetshift.io/nodes/delta-obs-replace")
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			assertObservation(t, "Observation", view.Resource.Inventory().Observation(), `{"cpu":8}`)

			hist, err := repo.ListObservations(ctx, r.UID(), 10)
			if err != nil {
				t.Fatalf("ListObservations: %v", err)
			}
			if len(hist) != 2 {
				t.Fatalf("history len = %d, want 2 (replace appends history)", len(hist))
			}
			assertEqual(t, "hist[0].Observation", string(hist[0].Observation()), `{"cpu":8}`)
		})

		t.Run("NullLiteralObservationLeavesLatestUnchanged", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/delta-obs-null-literal")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			t1 := fixedTime.Add(time.Minute)
			obs := json.RawMessage(`{"cpu":4}`)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: &obs,
				ObservedAt:  t1,
				ReceivedAt:  t1,
			}}); err != nil {
				t.Fatalf("seed ReplaceInventory: %v", err)
			}

			// A non-nil pointer to the JSON literal null must behave
			// identically to a nil pointer: untouched, no history.
			nullLiteral := json.RawMessage(`null`)
			t2 := fixedTime.Add(2 * time.Minute)
			if _, err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: &nullLiteral,
				ObservedAt:  t2,
				ReceivedAt:  t2,
			}}); err != nil {
				t.Fatalf("ApplyInventoryDeltas: %v", err)
			}

			view, err := repo.GetView(ctx, "//inv.fleetshift.io/nodes/delta-obs-null-literal")
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			assertObservation(t, "Observation", view.Resource.Inventory().Observation(), `{"cpu":4}`)

			hist, err := repo.ListObservations(ctx, r.UID(), 10)
			if err != nil {
				t.Fatalf("ListObservations: %v", err)
			}
			if len(hist) != 1 {
				t.Fatalf("history len = %d, want 1 (null literal appends no history)", len(hist))
			}
		})

		t.Run("DeduplicatesIdenticalObservationHistory", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/delta-obs-dedup")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			t1 := fixedTime.Add(time.Minute)
			obs := json.RawMessage(`{"cpu":4}`)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: &obs,
				ObservedAt:  t1,
				ReceivedAt:  t1,
			}}); err != nil {
				t.Fatalf("seed ReplaceInventory: %v", err)
			}

			// Byte-identical observation via a delta must not append a
			// second history row.
			t2 := fixedTime.Add(2 * time.Minute)
			sameObs := json.RawMessage(`{"cpu":4}`)
			if _, err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: &sameObs,
				ObservedAt:  t2,
				ReceivedAt:  t2,
			}}); err != nil {
				t.Fatalf("ApplyInventoryDeltas (same observation): %v", err)
			}

			hist, err := repo.ListObservations(ctx, r.UID(), 10)
			if err != nil {
				t.Fatalf("ListObservations: %v", err)
			}
			if len(hist) != 1 {
				t.Fatalf("history len = %d, want 1 (identical observation must dedup)", len(hist))
			}
		})

		t.Run("UpsertsAndDeletesConditionsLeavingOmittedUntouched", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/delta-conditions")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			t1 := fixedTime.Add(time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Conditions: []domain.Condition{
					mustCondition(t, "Ready", domain.ConditionTrue, "AllGood", "ok", t1),
					mustCondition(t, "Provisioned", domain.ConditionTrue, "Done", "done", t1),
					mustCondition(t, "Healthy", domain.ConditionTrue, "Nominal", "nominal", t1),
				},
				ObservedAt: t1,
				ReceivedAt: t1,
			}}); err != nil {
				t.Fatalf("seed ReplaceInventory: %v", err)
			}

			t2 := fixedTime.Add(2 * time.Minute)
			if _, err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				UpsertConditions: []domain.Condition{mustCondition(t, "Ready", domain.ConditionFalse, "Degraded", "broke", t2)},
				DeleteConditions: []domain.ConditionType{"Provisioned"},
				ObservedAt:       t2,
				ReceivedAt:       t2,
			}}); err != nil {
				t.Fatalf("ApplyInventoryDeltas: %v", err)
			}

			view, err := repo.GetView(ctx, "//inv.fleetshift.io/nodes/delta-conditions")
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			byType := make(map[domain.ConditionType]domain.Condition)
			for _, c := range view.Resource.Inventory().Conditions() {
				byType[c.Type()] = c
			}
			if len(byType) != 2 {
				t.Fatalf("Conditions len = %d, want 2 (Provisioned deleted)", len(byType))
			}
			ready, ok := byType["Ready"]
			if !ok {
				t.Fatal("Ready condition missing")
			}
			assertEqual(t, "Ready.Status", ready.Status(), domain.ConditionFalse)
			healthy, ok := byType["Healthy"]
			if !ok {
				t.Fatal("Healthy condition missing, should be untouched by the delta")
			}
			assertEqual(t, "Healthy.Status", healthy.Status(), domain.ConditionTrue)
			if _, ok := byType["Provisioned"]; ok {
				t.Error("Provisioned condition should have been deleted")
			}
		})

		t.Run("HeartbeatWithNoFieldChanges", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/delta-heartbeat")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			t1 := fixedTime.Add(time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Labels:     map[string]string{"zone": "us-east-1"},
				ObservedAt: t1,
				ReceivedAt: t1,
			}}); err != nil {
				t.Fatalf("seed ReplaceInventory: %v", err)
			}

			t2 := fixedTime.Add(2 * time.Minute)
			if _, err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				ObservedAt: t2,
				ReceivedAt: t2,
			}}); err != nil {
				t.Fatalf("heartbeat ApplyInventoryDeltas: %v", err)
			}

			view, err := repo.GetView(ctx, "//inv.fleetshift.io/nodes/delta-heartbeat")
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			assertEqual(t, "Labels[zone] unchanged", view.Resource.Inventory().Labels()["zone"], "us-east-1")
			if !view.Resource.Inventory().ObservedAt().Equal(t2) {
				t.Errorf("ObservedAt = %v, want %v (heartbeat still bumps freshness)", view.Resource.Inventory().ObservedAt(), t2)
			}
			if !view.Resource.Inventory().UpdatedAt().Equal(t2) {
				t.Errorf("UpdatedAt = %v, want %v (heartbeat still bumps freshness)", view.Resource.Inventory().UpdatedAt(), t2)
			}
		})

		t.Run("RejectsUnregisteredResourceType", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			// See the Replace-side RejectsUnregisteredResourceType for
			// why this is the delta-side equivalent of "unknown UID".
			now := fixedTime.Add(time.Minute)
			_, err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType: "inv.fleetshift.io/Unregistered",
				Name:         "nodes/delta-unregistered",
				CandidateUID: domain.NewExtensionResourceUID(),
				ObservedAt:   now,
				ReceivedAt:   now,
			}})
			if err == nil {
				t.Fatal("expected error for unregistered resource type, got nil")
			}
		})

		// RejectsLabelInBothSetAndDelete/RejectsConditionInBothUpsertAndDelete
		// guard an invariant [InventoryReportService]'s
		// validateDeltaReport also checks before identity resolution
		// even begins -- but that's an application-layer courtesy,
		// not a substitute for the repository defending its own
		// contract against a delta built any other way. The two
		// backends' applyInventoryDeltasCoreCTEs/ApplyInventoryDeltas
		// implementations have no defined ordering between a field's
		// set/upsert and its delete when both target the same
		// key/type in one delta (Postgres's sibling writable CTEs
		// touching the same table have no guaranteed execution order;
		// SQLite's sequential statements would happen to let the
		// delete win, but only incidentally) -- so without this
		// check, the very same contradictory delta could silently
		// resolve differently per backend.
		t.Run("RejectsLabelInBothSetAndDelete", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/delta-label-contradiction")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}
			t1 := fixedTime.Add(time.Minute)
			if _, err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				SetLabels:  map[string]string{"zone": "us-east-1"},
				ObservedAt: t1, ReceivedAt: t1,
			}}); err != nil {
				t.Fatalf("seed ApplyInventoryDeltas: %v", err)
			}

			t2 := fixedTime.Add(2 * time.Minute)
			_, err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				SetLabels:    map[string]string{"zone": "us-west-2"},
				DeleteLabels: []string{"zone"},
				ObservedAt:   t2, ReceivedAt: t2,
			}})
			if !errors.Is(err, domain.ErrInvalidArgument) {
				t.Fatalf("ApplyInventoryDeltas err = %v, want ErrInvalidArgument", err)
			}

			view, err := repo.GetView(ctx, "//inv.fleetshift.io/nodes/delta-label-contradiction")
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			if got := view.Resource.Inventory().Labels()["zone"]; got != "us-east-1" {
				t.Errorf("Labels[zone] = %q, want unchanged %q (rejected before any write)", got, "us-east-1")
			}
		})

		t.Run("RejectsConditionInBothUpsertAndDelete", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/delta-condition-contradiction")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}
			t1 := fixedTime.Add(time.Minute)
			if _, err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				UpsertConditions: []domain.Condition{mustCondition(t, "Ready", domain.ConditionTrue, "AllGood", "ok", t1)},
				ObservedAt:       t1, ReceivedAt: t1,
			}}); err != nil {
				t.Fatalf("seed ApplyInventoryDeltas: %v", err)
			}

			t2 := fixedTime.Add(2 * time.Minute)
			_, err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				UpsertConditions: []domain.Condition{mustCondition(t, "Ready", domain.ConditionFalse, "Degraded", "broke", t2)},
				DeleteConditions: []domain.ConditionType{"Ready"},
				ObservedAt:       t2, ReceivedAt: t2,
			}})
			if !errors.Is(err, domain.ErrInvalidArgument) {
				t.Fatalf("ApplyInventoryDeltas err = %v, want ErrInvalidArgument", err)
			}

			view, err := repo.GetView(ctx, "//inv.fleetshift.io/nodes/delta-condition-contradiction")
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			conds := view.Resource.Inventory().Conditions()
			if len(conds) != 1 || conds[0].Status() != domain.ConditionTrue {
				t.Errorf("Conditions = %+v, want unchanged [Ready=True] (rejected before any write)", conds)
			}
		})

		t.Run("UsesReceivedAtNotWallClock", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/delta-receivedat")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			observedAt := fixedTime.Add(time.Minute)
			receivedAt := wallClockDistantPast
			obs := json.RawMessage(`{"cpu":4}`)
			if _, err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation:      &obs,
				UpsertConditions: []domain.Condition{mustCondition(t, "Ready", domain.ConditionTrue, "AllGood", "ok", observedAt)},
				ObservedAt:       observedAt,
				ReceivedAt:       receivedAt,
			}}); err != nil {
				t.Fatalf("ApplyInventoryDeltas: %v", err)
			}

			view, err := repo.GetView(ctx, "//inv.fleetshift.io/nodes/delta-receivedat")
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			if !view.Resource.Inventory().UpdatedAt().Equal(receivedAt) {
				t.Errorf("Inventory.UpdatedAt = %v, want %v (ReceivedAt, not wall clock)", view.Resource.Inventory().UpdatedAt(), receivedAt)
			}

			hist, err := repo.ListObservations(ctx, r.UID(), 10)
			if err != nil {
				t.Fatalf("ListObservations: %v", err)
			}
			if len(hist) != 1 {
				t.Fatalf("history len = %d, want 1", len(hist))
			}
			if !hist[0].CreatedAt().Equal(receivedAt) {
				t.Errorf("observation CreatedAt = %v, want %v (ReceivedAt, not wall clock)", hist[0].CreatedAt(), receivedAt)
			}

			transitions, err := repo.ListConditionTransitions(ctx, r.UID(), nil, 10)
			if err != nil {
				t.Fatalf("ListConditionTransitions: %v", err)
			}
			if len(transitions) != 1 {
				t.Fatalf("transitions len = %d, want 1", len(transitions))
			}
			if !transitions[0].CreatedAt().Equal(receivedAt) {
				t.Errorf("transition CreatedAt = %v, want %v (ReceivedAt, not wall clock)", transitions[0].CreatedAt(), receivedAt)
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
			obs := json.RawMessage(`{"ready":true}`)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: &obs,
				ObservedAt:  now,
				ReceivedAt:  now,
			}}); err != nil {
				t.Fatalf("ReplaceInventory: %v", err)
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
			if view.Resource.Inventory() == nil {
				t.Fatal("Inventory is nil, want non-nil")
			}
			assertObservation(t, "Observation", view.Resource.Inventory().Observation(), `{"ready":true}`)
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
			obs := json.RawMessage(`{"version":"2.0"}`)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: &obs,
				ObservedAt:  now,
				ReceivedAt:  now,
			}}); err != nil {
				t.Fatalf("ReplaceInventory: %v", err)
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
			if view.Resource.Inventory() == nil {
				t.Fatal("Inventory is nil for managed+inventory resource")
			}
			assertObservation(t, "Observation", view.Resource.Inventory().Observation(), `{"version":"2.0"}`)
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
			if view.Resource.Inventory() != nil {
				t.Error("expected nil Inventory for managed-only resource")
			}
		})
	})

	t.Run("History", func(t *testing.T) {
		t.Run("ListObservationsOrdersByObservedAtDescWithDistinctIDs", func(t *testing.T) {
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
			obs1 := json.RawMessage(`{"v":1}`)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: &obs1,
				ObservedAt:  t1,
				ReceivedAt:  t1,
			}}); err != nil {
				t.Fatalf("first ReplaceInventory: %v", err)
			}
			obs2 := json.RawMessage(`{"v":2}`)
			if _, err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: &obs2,
				ObservedAt:  t2,
				ReceivedAt:  t2,
			}}); err != nil {
				t.Fatalf("ApplyInventoryDeltas: %v", err)
			}

			got, err := repo.ListObservations(ctx, r.UID(), 10)
			if err != nil {
				t.Fatalf("ListObservations: %v", err)
			}
			if len(got) != 2 {
				t.Fatalf("len = %d, want 2", len(got))
			}
			// Most recent first (ordered by observed_at DESC)
			assertEqual(t, "got[0].Observation", string(got[0].Observation()), `{"v":2}`)
			assertEqual(t, "got[1].Observation", string(got[1].Observation()), `{"v":1}`)
			if got[0].ID() == "" || got[1].ID() == "" {
				t.Error("observation IDs should be auto-generated and non-empty")
			}
			if got[0].ID() == got[1].ID() {
				t.Errorf("got[0].ID() == got[1].ID() (%s); want distinct", got[0].ID())
			}
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
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Conditions: []domain.Condition{
					mustCondition(t, "Ready", domain.ConditionTrue, "ok", "", t1),
					mustCondition(t, "Provisioned", domain.ConditionTrue, "done", "", t1),
				},
				ObservedAt: t1,
				ReceivedAt: t1,
			}}); err != nil {
				t.Fatalf("ReplaceInventory: %v", err)
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

		t.Run("MultipleUIDsInOneDeltaBatchGetDistinctConditions", func(t *testing.T) {
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

			// Distinct payloads per UID so a batch bug that copies the
			// first delta to every row would be caught.
			t1 := fixedTime.Add(time.Minute)
			t2 := fixedTime.Add(2 * time.Minute)
			if _, err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{
				{
					ResourceType: r1.ResourceType(), Name: r1.Name(), CandidateUID: domain.NewExtensionResourceUID(),
					UpsertConditions: []domain.Condition{mustCondition(t, "Ready", domain.ConditionTrue, "AllGood", "node is healthy", t1)},
					ObservedAt:       t1,
					ReceivedAt:       t1,
				},
				{
					ResourceType: r2.ResourceType(), Name: r2.Name(), CandidateUID: domain.NewExtensionResourceUID(),
					UpsertConditions: []domain.Condition{mustCondition(t, "Ready", domain.ConditionFalse, "Degraded", "disk pressure", t2)},
					ObservedAt:       t2,
					ReceivedAt:       t2,
				},
			}); err != nil {
				t.Fatalf("ApplyInventoryDeltas: %v", err)
			}

			// Verify r1 condition via GetView.
			v1, err := repo.GetView(ctx, domain.NewFullResourceName("inv.fleetshift.io", "nodes/multi-a"))
			if err != nil {
				t.Fatalf("GetView r1: %v", err)
			}
			if v1.Resource.Inventory() == nil {
				t.Fatal("r1: Inventory is nil; ApplyInventoryDeltas should touch inventory rows for all UIDs in the batch")
			}
			if len(v1.Resource.Inventory().Conditions()) != 1 {
				t.Fatalf("r1: Conditions len = %d, want 1", len(v1.Resource.Inventory().Conditions()))
			}
			assertEqual(t, "r1.Condition.Status", v1.Resource.Inventory().Conditions()[0].Status(), domain.ConditionTrue)
			assertEqual(t, "r1.Condition.Reason", v1.Resource.Inventory().Conditions()[0].Reason(), "AllGood")
			assertEqual(t, "r1.Condition.Message", v1.Resource.Inventory().Conditions()[0].Message(), "node is healthy")
			assertEqual(t, "r1.Condition.LastTransitionTime", v1.Resource.Inventory().Conditions()[0].LastTransitionTime(), t1)

			// Verify r2 condition via GetView.
			v2, err := repo.GetView(ctx, domain.NewFullResourceName("inv.fleetshift.io", "nodes/multi-b"))
			if err != nil {
				t.Fatalf("GetView r2: %v", err)
			}
			if v2.Resource.Inventory() == nil {
				t.Fatal("r2: Inventory is nil; ApplyInventoryDeltas should touch inventory rows for all UIDs in the batch")
			}
			if len(v2.Resource.Inventory().Conditions()) != 1 {
				t.Fatalf("r2: Conditions len = %d, want 1", len(v2.Resource.Inventory().Conditions()))
			}
			assertEqual(t, "r2.Condition.Status", v2.Resource.Inventory().Conditions()[0].Status(), domain.ConditionFalse)
			assertEqual(t, "r2.Condition.Reason", v2.Resource.Inventory().Conditions()[0].Reason(), "Degraded")
			assertEqual(t, "r2.Condition.Message", v2.Resource.Inventory().Conditions()[0].Message(), "disk pressure")
			assertEqual(t, "r2.Condition.LastTransitionTime", v2.Resource.Inventory().Conditions()[0].LastTransitionTime(), t2)

			// Verify per-UID transitions were recorded independently.
			tr1, err := repo.ListConditionTransitions(ctx, r1.UID(), nil, 10)
			if err != nil {
				t.Fatalf("ListConditionTransitions r1: %v", err)
			}
			if len(tr1) != 1 {
				t.Fatalf("r1 transitions = %d, want 1", len(tr1))
			}
			assertEqual(t, "r1.Transition.Status", tr1[0].Status(), domain.ConditionTrue)
			assertEqual(t, "r1.Transition.Reason", tr1[0].Reason(), "AllGood")

			tr2, err := repo.ListConditionTransitions(ctx, r2.UID(), nil, 10)
			if err != nil {
				t.Fatalf("ListConditionTransitions r2: %v", err)
			}
			if len(tr2) != 1 {
				t.Fatalf("r2 transitions = %d, want 1", len(tr2))
			}
			assertEqual(t, "r2.Transition.Status", tr2[0].Status(), domain.ConditionFalse)
			assertEqual(t, "r2.Transition.Reason", tr2[0].Reason(), "Degraded")
		})

		// CrossPathConsistencyAcrossReplaceAndDelta exercises a sequence
		// that alternates between ReplaceInventory and
		// ApplyInventoryDeltas, mixing genuine transitions with
		// duplicates in both directions, to prove both commands feed the
		// same transition-detection path.
		//
		//   Step  Path     Condition         Transition?
		//   1     replace  Ready=True        yes (first)
		//   2     delta    Ready=True        no  (dedup: delta sees replace state)
		//   3     delta    Ready=False       yes (genuine via delta)
		//   4     replace  Ready=False       no  (dedup: replace sees delta state)
		//   5     replace  Ready=True        yes (genuine via replace)
		//
		// Result: 3 transitions, latest state Ready=True.
		t.Run("CrossPathConsistencyAcrossReplaceAndDelta", func(t *testing.T) {
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

			replaceWith := func(step string, status domain.ConditionStatus, reason, msg string, ts time.Time) {
				t.Helper()
				if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
					ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
					Conditions: []domain.Condition{mustCondition(t, "Ready", status, reason, msg, ts)},
					ObservedAt: ts,
					ReceivedAt: ts,
				}}); err != nil {
					t.Fatalf("%s ReplaceInventory: %v", step, err)
				}
			}

			deltaWith := func(step string, status domain.ConditionStatus, reason, msg string, ts time.Time) {
				t.Helper()
				if _, err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
					ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
					UpsertConditions: []domain.Condition{mustCondition(t, "Ready", status, reason, msg, ts)},
					ObservedAt:       ts,
					ReceivedAt:       ts,
				}}); err != nil {
					t.Fatalf("%s ApplyInventoryDeltas: %v", step, err)
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

			// Step 1: replace Ready=True → genuine (first)
			replaceWith("step1", domain.ConditionTrue, "AllGood", "ok", t1)
			assertTransitionCount("step1", 1)

			// Step 2: delta Ready=True → dedup (same state set by replace)
			deltaWith("step2", domain.ConditionTrue, "AllGood", "ok", t2)
			assertTransitionCount("step2", 1)

			// Step 3: delta Ready=False → genuine transition via delta
			deltaWith("step3", domain.ConditionFalse, "Degraded", "broke", t3)
			assertTransitionCount("step3", 2)

			// Step 4: replace Ready=False → dedup (same state set by delta)
			replaceWith("step4", domain.ConditionFalse, "Degraded", "broke", t4)
			assertTransitionCount("step4", 2)

			// Step 5: replace Ready=True → genuine transition via replace
			replaceWith("step5", domain.ConditionTrue, "Recovered", "back", t5)
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

			// Latest state should reflect the final replace
			view, err := repo.GetView(ctx, "//inv.fleetshift.io/nodes/cross-path")
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			if len(view.Resource.Inventory().Conditions()) != 1 {
				t.Fatalf("Conditions len = %d, want 1", len(view.Resource.Inventory().Conditions()))
			}
			assertEqual(t, "latest Status", view.Resource.Inventory().Conditions()[0].Status(), domain.ConditionTrue)
			assertEqual(t, "latest Reason", view.Resource.Inventory().Conditions()[0].Reason(), "Recovered")
		})
	})

	// NaturalKeyResolution exercises ReplaceInventory/ApplyInventoryDeltas'
	// own resolve-or-create of the extension_resources row by natural
	// key (ResourceType, Name) -- the behavior that replaced the old
	// UpsertBatch/ClaimOrGetIdentity round trip the application layer
	// used to need before writing inventory at all.
	t.Run("NaturalKeyResolution", func(t *testing.T) {
		t.Run("CreatesRowLazilyWhenNoneExists", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			// No repo.Create call at all: the extension_resources row
			// does not exist yet, so ReplaceInventory must create it
			// using the supplied CandidateUID.
			candidateUID := domain.NewExtensionResourceUID()
			now := fixedTime.Add(time.Minute)
			obs := json.RawMessage(`{"cpu":4}`)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         "nodes/lazy-create",
				CandidateUID: candidateUID,
				Observation:  &obs,
				ObservedAt:   now,
				ReceivedAt:   now,
			}}); err != nil {
				t.Fatalf("ReplaceInventory: %v", err)
			}

			got, err := repo.GetByUID(ctx, candidateUID)
			if err != nil {
				t.Fatalf("GetByUID(candidateUID): %v", err)
			}
			assertEqual(t, "Name", got.Name(), domain.ResourceName("nodes/lazy-create"))
			if got.Inventory() == nil {
				t.Fatal("Inventory is nil after lazy create")
			}
			assertObservation(t, "Observation", got.Inventory().Observation(), `{"cpu":4}`)
		})

		t.Run("PreservesExistingRowIgnoringCandidateUID", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/already-exists")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			// A different CandidateUID for the same natural key must be
			// discarded in favor of the row that already exists.
			staleCandidateUID := domain.NewExtensionResourceUID()
			now := fixedTime.Add(time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(),
				Name:         r.Name(),
				CandidateUID: staleCandidateUID,
				ObservedAt:   now,
				ReceivedAt:   now,
			}}); err != nil {
				t.Fatalf("ReplaceInventory: %v", err)
			}

			got, err := repo.Get(ctx, r.ResourceType().FullName(r.Name()))
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if got.UID() != r.UID() {
				t.Errorf("UID = %s, want original UID %s (unchanged)", got.UID(), r.UID())
			}
			if got.UID() == staleCandidateUID {
				t.Error("UID must not become the discarded CandidateUID")
			}
			if _, err := repo.GetByUID(ctx, staleCandidateUID); !errors.Is(err, domain.ErrNotFound) {
				t.Errorf("GetByUID(staleCandidateUID): got %v, want ErrNotFound (never created)", err)
			}
		})

		t.Run("BatchMixesNewAndExistingRows", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			existing := newInventoryER("inv.fleetshift.io/Node", "nodes/mix-existing")
			if err := repo.Create(ctx, existing); err != nil {
				t.Fatalf("Create: %v", err)
			}

			newCandidateUID := domain.NewExtensionResourceUID()
			now := fixedTime.Add(time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{
				{
					ResourceType: existing.ResourceType(),
					Name:         existing.Name(),
					CandidateUID: domain.NewExtensionResourceUID(),
					ObservedAt:   now,
					ReceivedAt:   now,
				},
				{
					ResourceType: "inv.fleetshift.io/Node",
					Name:         "nodes/mix-new",
					CandidateUID: newCandidateUID,
					ObservedAt:   now,
					ReceivedAt:   now,
				},
			}); err != nil {
				t.Fatalf("ReplaceInventory batch: %v", err)
			}

			gotExisting, err := repo.GetByUID(ctx, existing.UID())
			if err != nil {
				t.Fatalf("GetByUID(existing): %v", err)
			}
			assertEqual(t, "existing.Name", gotExisting.Name(), domain.ResourceName("nodes/mix-existing"))

			gotNew, err := repo.GetByUID(ctx, newCandidateUID)
			if err != nil {
				t.Fatalf("GetByUID(new): %v", err)
			}
			assertEqual(t, "new.Name", gotNew.Name(), domain.ResourceName("nodes/mix-new"))
		})
	})

	// Aliases exercises the alias fold-in shared by ReplaceInventory
	// and ApplyInventoryDeltas: aliases supplied alongside a report are
	// upserted in the same statement as the inventory write, and any
	// [domain.AliasConflict] is reported back rather than raising a
	// hard SQL error or silently overwriting the conflicting state.
	t.Run("Aliases", func(t *testing.T) {
		t.Run("ReplaceInventoryFoldsAliasesIntoResourceAliases", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			alias, _ := domain.NewAlias("gcp", "instance_id", "alias-replace-1")
			now := fixedTime.Add(time.Minute)
			conflicts, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         "nodes/alias-replace",
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{alias},
				ObservedAt:   now,
				ReceivedAt:   now,
			}})
			if err != nil {
				t.Fatalf("ReplaceInventory: %v", err)
			}
			if len(conflicts) != 0 {
				t.Fatalf("conflicts = %+v, want none", conflicts)
			}

			resolved, err := tx.ResourceIdentities().ResolveAlias(ctx, alias)
			if err != nil {
				t.Fatalf("ResolveAlias: %v", err)
			}
			assertEqual(t, "resolved name", resolved, domain.ResourceName("nodes/alias-replace"))
		})

		t.Run("ApplyInventoryDeltasFoldsAliasesIntoResourceAliases", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			alias, _ := domain.NewAlias("gcp", "instance_id", "alias-delta-1")
			now := fixedTime.Add(time.Minute)
			conflicts, err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         "nodes/alias-delta",
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{alias},
				ObservedAt:   now,
				ReceivedAt:   now,
			}})
			if err != nil {
				t.Fatalf("ApplyInventoryDeltas: %v", err)
			}
			if len(conflicts) != 0 {
				t.Fatalf("conflicts = %+v, want none", conflicts)
			}

			resolved, err := tx.ResourceIdentities().ResolveAlias(ctx, alias)
			if err != nil {
				t.Fatalf("ResolveAlias: %v", err)
			}
			assertEqual(t, "resolved name", resolved, domain.ResourceName("nodes/alias-delta"))
		})

		t.Run("ReportsConflictWhenValueClaimedByAnotherResourceAcrossCalls", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			alias, _ := domain.NewAlias("gcp", "instance_id", "alias-contested")
			t1 := fixedTime.Add(time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         "nodes/alias-owner-a",
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{alias},
				ObservedAt:   t1,
				ReceivedAt:   t1,
			}}); err != nil {
				t.Fatalf("seed ReplaceInventory (A): %v", err)
			}

			t2 := fixedTime.Add(2 * time.Minute)
			conflicts, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         "nodes/alias-owner-b",
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{alias},
				ObservedAt:   t2,
				ReceivedAt:   t2,
			}})
			if err != nil {
				t.Fatalf("ReplaceInventory (B): %v", err)
			}
			if len(conflicts) != 1 {
				t.Fatalf("conflicts len = %d, want 1: %+v", len(conflicts), conflicts)
			}
			assertEqual(t, "conflict.Kind", conflicts[0].Kind, domain.AliasConflictValueClaimedByOther)
			assertEqual(t, "conflict.TargetName", conflicts[0].TargetName, domain.ResourceName("nodes/alias-owner-b"))
			assertEqual(t, "conflict.ActualName", conflicts[0].ActualName, domain.ResourceName("nodes/alias-owner-a"))

			// The alias must still resolve to its real owner (A) --
			// B's conflicting claim must never overwrite it.
			resolved, err := tx.ResourceIdentities().ResolveAlias(ctx, alias)
			if err != nil {
				t.Fatalf("ResolveAlias: %v", err)
			}
			assertEqual(t, "resolved name", resolved, domain.ResourceName("nodes/alias-owner-a"))
		})

		// SameExtensionResourceReplacingOwnAliasValueSucceeds covers
		// the "per-extension-resource replace" half of the alias
		// contract: the very same extension resource (same natural
		// key -- ResourceType+Name, see [domain.InventoryReplacement]'s
		// doc) reporting a *different* value for a (namespace, key) it
		// already owns is not a conflict at all -- it's a legitimate
		// update to its own contribution, exactly like changing a
		// label value on a later report. This test used to assert the
		// opposite (a permanent conflict) under the older
		// "additive and immutable" alias contract; see
		// AliasConflictResourceHasDifferentValue's doc and
		// ReplacingOwnAliasWhileSiblingStillHoldsOldValueConflicts
		// below for when a value change *does* still conflict (a
		// sibling extension resource for the same name still holding
		// the old value).
		t.Run("SameExtensionResourceReplacingOwnAliasValueSucceeds", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			name := domain.ResourceName("nodes/alias-value-flip")
			firstValue, _ := domain.NewAlias("gcp", "instance_id", "alias-value-v1")
			t1 := fixedTime.Add(time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{firstValue},
				ObservedAt:   t1,
				ReceivedAt:   t1,
			}}); err != nil {
				t.Fatalf("seed ReplaceInventory: %v", err)
			}

			secondValue, _ := domain.NewAlias("gcp", "instance_id", "alias-value-v2")
			t2 := fixedTime.Add(2 * time.Minute)
			conflicts, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{secondValue},
				ObservedAt:   t2,
				ReceivedAt:   t2,
			}})
			if err != nil {
				t.Fatalf("ReplaceInventory (second value): %v", err)
			}
			if len(conflicts) != 0 {
				t.Fatalf("conflicts = %+v, want none", conflicts)
			}

			// The new value must resolve, and the old one must have
			// been retracted -- freed up for any other resource to
			// claim, not left dangling forever.
			resolvedSecond, err := tx.ResourceIdentities().ResolveAlias(ctx, secondValue)
			if err != nil {
				t.Fatalf("ResolveAlias(secondValue): %v", err)
			}
			assertEqual(t, "resolved name", resolvedSecond, name)
			if _, err := tx.ResourceIdentities().ResolveAlias(ctx, firstValue); !errors.Is(err, domain.ErrNotFound) {
				t.Errorf("ResolveAlias(firstValue): got %v, want ErrNotFound (retracted on replace)", err)
			}
		})

		// The three tests below round out single-contributor replace
		// coverage: SameExtensionResourceReplacingOwnAliasValueSucceeds
		// above only exercises a 1-alias resource changing its one
		// value, and AliasRetractedWhenSoleContributingExtensionResourceStopsReportingIt
		// (further down) only exercises going from 1 alias to 0. A
		// resource that carries *several* aliases at once can replace
		// them unevenly -- keep some, drop some, add new ones, or
		// swap the whole set -- in a single report, mirroring
		// RemovesLabelsAbsentFromReplacement's "keep some, drop some"
		// shape for Labels.

		t.Run("PartialReplaceKeepsUnchangedAliasesAndRetractsDroppedOnesWithinSameExtensionResource", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			name := domain.ResourceName("nodes/alias-partial-replace")
			keep, _ := domain.NewAlias("gcp", "instance_id", "alias-partial-keep")
			drop, _ := domain.NewAlias("gcp", "zone", "alias-partial-drop")

			t1 := fixedTime.Add(time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{keep, drop},
				ObservedAt:   t1,
				ReceivedAt:   t1,
			}}); err != nil {
				t.Fatalf("first ReplaceInventory: %v", err)
			}

			// Second report carries only "keep" -- "drop" is simply
			// absent, not reasserted with a different value.
			t2 := fixedTime.Add(2 * time.Minute)
			conflicts, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{keep},
				ObservedAt:   t2,
				ReceivedAt:   t2,
			}})
			if err != nil {
				t.Fatalf("second ReplaceInventory: %v", err)
			}
			if len(conflicts) != 0 {
				t.Fatalf("conflicts = %+v, want none", conflicts)
			}

			resolvedKeep, err := tx.ResourceIdentities().ResolveAlias(ctx, keep)
			if err != nil {
				t.Fatalf("ResolveAlias(keep): %v", err)
			}
			assertEqual(t, "resolved keep", resolvedKeep, name)
			if _, err := tx.ResourceIdentities().ResolveAlias(ctx, drop); !errors.Is(err, domain.ErrNotFound) {
				t.Errorf("ResolveAlias(drop): got %v, want ErrNotFound (retracted, not resubmitted)", err)
			}
		})

		t.Run("ReplaceGrowsAliasSetWithNewKeyFromSameExtensionResource", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			name := domain.ResourceName("nodes/alias-grow")
			original, _ := domain.NewAlias("gcp", "instance_id", "alias-grow-original")
			grown, _ := domain.NewAlias("gcp", "zone", "alias-grow-new")

			t1 := fixedTime.Add(time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{original},
				ObservedAt:   t1,
				ReceivedAt:   t1,
			}}); err != nil {
				t.Fatalf("first ReplaceInventory: %v", err)
			}

			t2 := fixedTime.Add(2 * time.Minute)
			conflicts, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{original, grown},
				ObservedAt:   t2,
				ReceivedAt:   t2,
			}})
			if err != nil {
				t.Fatalf("second ReplaceInventory: %v", err)
			}
			if len(conflicts) != 0 {
				t.Fatalf("conflicts = %+v, want none", conflicts)
			}

			for _, a := range []domain.Alias{original, grown} {
				resolved, err := tx.ResourceIdentities().ResolveAlias(ctx, a)
				if err != nil {
					t.Fatalf("ResolveAlias(%+v): %v", a, err)
				}
				assertEqual(t, fmt.Sprintf("resolved(%+v)", a), resolved, name)
			}
		})

		t.Run("ReplaceFullySwapsAliasSetWithNoOverlapWithinSameExtensionResource", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			name := domain.ResourceName("nodes/alias-full-swap")
			oldA, _ := domain.NewAlias("gcp", "instance_id", "alias-swap-old-a")
			oldB, _ := domain.NewAlias("gcp", "zone", "alias-swap-old-b")
			newC, _ := domain.NewAlias("gcp", "instance_id", "alias-swap-new-c")
			newD, _ := domain.NewAlias("gcp", "zone", "alias-swap-new-d")

			t1 := fixedTime.Add(time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{oldA, oldB},
				ObservedAt:   t1,
				ReceivedAt:   t1,
			}}); err != nil {
				t.Fatalf("first ReplaceInventory: %v", err)
			}

			// Second report shares no aliases at all with the first --
			// every key keeps existing, but every value moves.
			t2 := fixedTime.Add(2 * time.Minute)
			conflicts, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{newC, newD},
				ObservedAt:   t2,
				ReceivedAt:   t2,
			}})
			if err != nil {
				t.Fatalf("second ReplaceInventory: %v", err)
			}
			if len(conflicts) != 0 {
				t.Fatalf("conflicts = %+v, want none", conflicts)
			}

			for _, a := range []domain.Alias{newC, newD} {
				resolved, err := tx.ResourceIdentities().ResolveAlias(ctx, a)
				if err != nil {
					t.Fatalf("ResolveAlias(%+v): %v", a, err)
				}
				assertEqual(t, fmt.Sprintf("resolved(%+v)", a), resolved, name)
			}
			for _, a := range []domain.Alias{oldA, oldB} {
				if _, err := tx.ResourceIdentities().ResolveAlias(ctx, a); !errors.Is(err, domain.ErrNotFound) {
					t.Errorf("ResolveAlias(%+v): got %v, want ErrNotFound (retracted on swap)", a, err)
				}
			}
		})

		t.Run("ReportsIntraBatchContradictionWithoutSQLError", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			// Two reports in the *same* call claim the same alias value
			// for two different resources -- a contradiction that only
			// exists within this batch, with no pre-existing row on
			// either side for a unique index to catch.
			alias, _ := domain.NewAlias("gcp", "instance_id", "alias-intra-batch")
			now := fixedTime.Add(time.Minute)
			conflicts, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{
				{
					ResourceType: "inv.fleetshift.io/Node",
					Name:         "nodes/alias-intra-a",
					CandidateUID: domain.NewExtensionResourceUID(),
					Aliases:      []domain.Alias{alias},
					ObservedAt:   now,
					ReceivedAt:   now,
				},
				{
					ResourceType: "inv.fleetshift.io/Node",
					Name:         "nodes/alias-intra-b",
					CandidateUID: domain.NewExtensionResourceUID(),
					Aliases:      []domain.Alias{alias},
					ObservedAt:   now,
					ReceivedAt:   now,
				},
			})
			if err != nil {
				t.Fatalf("ReplaceInventory: %v", err)
			}
			if len(conflicts) != 1 {
				t.Fatalf("conflicts len = %d, want 1: %+v", len(conflicts), conflicts)
			}
			assertEqual(t, "conflict.Kind", conflicts[0].Kind, domain.AliasConflictValueClaimedByOther)

			// Exactly one of the two resources won the alias; the other
			// must not have gotten it either (no silent partial write).
			resolved, err := tx.ResourceIdentities().ResolveAlias(ctx, alias)
			if err != nil {
				t.Fatalf("ResolveAlias: %v", err)
			}
			if resolved != conflicts[0].ActualName {
				t.Errorf("resolved name = %q, want the winning resource %q", resolved, conflicts[0].ActualName)
			}
		})

		// ReportsNoConflictWhenSameResourceRepeatsSameAliasAcrossCalls
		// covers the overwhelmingly common steady-state case: a
		// resource re-reporting an alias it already owns, unchanged,
		// on every poll. This must never surface as a conflict --
		// only a genuine contradiction against a *different* target
		// or value should.
		t.Run("ReportsNoConflictWhenSameResourceRepeatsSameAliasAcrossCalls", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			alias, _ := domain.NewAlias("gcp", "instance_id", "alias-steady-state")
			name := domain.ResourceName("nodes/alias-steady-state")
			for i, at := range []time.Time{fixedTime.Add(time.Minute), fixedTime.Add(2 * time.Minute), fixedTime.Add(3 * time.Minute)} {
				conflicts, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
					ResourceType: "inv.fleetshift.io/Node",
					Name:         name,
					CandidateUID: domain.NewExtensionResourceUID(),
					Aliases:      []domain.Alias{alias},
					ObservedAt:   at,
					ReceivedAt:   at,
				}})
				if err != nil {
					t.Fatalf("ReplaceInventory (poll %d): %v", i, err)
				}
				if len(conflicts) != 0 {
					t.Fatalf("ReplaceInventory (poll %d): conflicts = %+v, want none", i, conflicts)
				}
			}

			resolved, err := tx.ResourceIdentities().ResolveAlias(ctx, alias)
			if err != nil {
				t.Fatalf("ResolveAlias: %v", err)
			}
			assertEqual(t, "resolved name", resolved, name)
		})

		// ReportsConflictsOnlyForActualConflictsInMixedBatch exercises
		// a single chunk carrying, at once: an idempotent repeat of an
		// alias its own resource already owns (must succeed silently),
		// a brand-new alias for an unrelated resource (must succeed),
		// and a genuine value-claimed-by-another conflict (must be
		// reported) -- proving the safe and conflicting candidates
		// within one round trip don't interfere with each other.
		t.Run("ReportsConflictsOnlyForActualConflictsInMixedBatch", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			ownedAlias, _ := domain.NewAlias("gcp", "instance_id", "alias-mixed-owned")
			t1 := fixedTime.Add(time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         "nodes/alias-mixed-owner",
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{ownedAlias},
				ObservedAt:   t1,
				ReceivedAt:   t1,
			}}); err != nil {
				t.Fatalf("seed ReplaceInventory: %v", err)
			}

			newAlias, _ := domain.NewAlias("gcp", "instance_id", "alias-mixed-new")
			t2 := fixedTime.Add(2 * time.Minute)
			conflicts, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{
				{
					// Idempotent repeat of an alias this exact resource already owns.
					ResourceType: "inv.fleetshift.io/Node",
					Name:         "nodes/alias-mixed-owner",
					CandidateUID: domain.NewExtensionResourceUID(),
					Aliases:      []domain.Alias{ownedAlias},
					ObservedAt:   t2,
					ReceivedAt:   t2,
				},
				{
					// Brand-new alias for an unrelated resource.
					ResourceType: "inv.fleetshift.io/Node",
					Name:         "nodes/alias-mixed-fresh",
					CandidateUID: domain.NewExtensionResourceUID(),
					Aliases:      []domain.Alias{newAlias},
					ObservedAt:   t2,
					ReceivedAt:   t2,
				},
				{
					// Genuine conflict: claims an alias value already owned by another resource.
					ResourceType: "inv.fleetshift.io/Node",
					Name:         "nodes/alias-mixed-challenger",
					CandidateUID: domain.NewExtensionResourceUID(),
					Aliases:      []domain.Alias{ownedAlias},
					ObservedAt:   t2,
					ReceivedAt:   t2,
				},
			})
			if err != nil {
				t.Fatalf("ReplaceInventory (mixed batch): %v", err)
			}
			if len(conflicts) != 1 {
				t.Fatalf("conflicts len = %d, want 1: %+v", len(conflicts), conflicts)
			}
			assertEqual(t, "conflict.Kind", conflicts[0].Kind, domain.AliasConflictValueClaimedByOther)
			assertEqual(t, "conflict.TargetName", conflicts[0].TargetName, domain.ResourceName("nodes/alias-mixed-challenger"))
			assertEqual(t, "conflict.ActualName", conflicts[0].ActualName, domain.ResourceName("nodes/alias-mixed-owner"))

			resolvedOwned, err := tx.ResourceIdentities().ResolveAlias(ctx, ownedAlias)
			if err != nil {
				t.Fatalf("ResolveAlias(ownedAlias): %v", err)
			}
			assertEqual(t, "resolved owned alias", resolvedOwned, domain.ResourceName("nodes/alias-mixed-owner"))

			resolvedNew, err := tx.ResourceIdentities().ResolveAlias(ctx, newAlias)
			if err != nil {
				t.Fatalf("ResolveAlias(newAlias): %v", err)
			}
			assertEqual(t, "resolved new alias", resolvedNew, domain.ResourceName("nodes/alias-mixed-fresh"))
		})

		// The tests above all report exactly one alias per report.
		// The ones below exercise a single report carrying *multiple*
		// aliases at once -- e.g. a cloud resource identified by both
		// an instance ID and a zone -- since every alias in a report
		// is flattened into its own independent candidate row before
		// checkAliasBatchConsistency/SQL ever sees it (see
		// aliasCandidateInput's doc comment), and that per-alias
		// independence is exactly what could silently break under
		// future optimization without being caught by any
		// single-alias test.

		t.Run("ResolvesMultipleDistinctAliasesFromSingleReport", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			instanceID, _ := domain.NewAlias("gcp", "instance_id", "multi-instance-1")
			zone, _ := domain.NewAlias("gcp", "zone", "multi-zone-1")
			name := domain.ResourceName("nodes/alias-multi-distinct")
			t1 := fixedTime.Add(time.Minute)
			conflicts, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{instanceID, zone},
				ObservedAt:   t1,
				ReceivedAt:   t1,
			}})
			if err != nil {
				t.Fatalf("ReplaceInventory: %v", err)
			}
			if len(conflicts) != 0 {
				t.Fatalf("conflicts = %+v, want none", conflicts)
			}

			for _, a := range []domain.Alias{instanceID, zone} {
				resolved, err := tx.ResourceIdentities().ResolveAlias(ctx, a)
				if err != nil {
					t.Fatalf("ResolveAlias(%+v): %v", a, err)
				}
				assertEqual(t, fmt.Sprintf("resolved(%+v)", a), resolved, name)
			}

			// Steady-state repeat: re-reporting both aliases unchanged
			// on a later call must not conflict either, the same as
			// the single-alias case above.
			t2 := fixedTime.Add(2 * time.Minute)
			conflicts, err = repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{instanceID, zone},
				ObservedAt:   t2,
				ReceivedAt:   t2,
			}})
			if err != nil {
				t.Fatalf("second ReplaceInventory: %v", err)
			}
			if len(conflicts) != 0 {
				t.Fatalf("second ReplaceInventory: conflicts = %+v, want none", conflicts)
			}
		})

		// PartialConflictInMultiAliasReportStillAppliesTheRest proves
		// aliases within one report are resolved independently of
		// each other *and* of the report's own inventory write: one
		// bad alias must not sink the good alias or the observation
		// riding along in the very same report.
		t.Run("PartialConflictInMultiAliasReportStillAppliesTheRest", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			contested, _ := domain.NewAlias("gcp", "instance_id", "multi-partial-contested")
			t1 := fixedTime.Add(time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         "nodes/alias-multi-partial-owner",
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{contested},
				ObservedAt:   t1,
				ReceivedAt:   t1,
			}}); err != nil {
				t.Fatalf("seed ReplaceInventory: %v", err)
			}

			safe, _ := domain.NewAlias("gcp", "zone", "multi-partial-zone")
			obs := json.RawMessage(`{"cpu":4}`)
			t2 := fixedTime.Add(2 * time.Minute)
			conflicts, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         "nodes/alias-multi-partial-challenger",
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{contested, safe},
				Observation:  &obs,
				ObservedAt:   t2,
				ReceivedAt:   t2,
			}})
			if err != nil {
				t.Fatalf("ReplaceInventory: %v", err)
			}
			if len(conflicts) != 1 {
				t.Fatalf("conflicts len = %d, want 1: %+v", len(conflicts), conflicts)
			}
			assertEqual(t, "conflict.Alias", conflicts[0].Alias, contested)
			assertEqual(t, "conflict.Kind", conflicts[0].Kind, domain.AliasConflictValueClaimedByOther)
			assertEqual(t, "conflict.TargetName", conflicts[0].TargetName, domain.ResourceName("nodes/alias-multi-partial-challenger"))
			assertEqual(t, "conflict.ActualName", conflicts[0].ActualName, domain.ResourceName("nodes/alias-multi-partial-owner"))

			// The contested alias must still belong to its original owner.
			resolvedContested, err := tx.ResourceIdentities().ResolveAlias(ctx, contested)
			if err != nil {
				t.Fatalf("ResolveAlias(contested): %v", err)
			}
			assertEqual(t, "resolved contested", resolvedContested, domain.ResourceName("nodes/alias-multi-partial-owner"))

			// But the safe alias, from the very same losing report,
			// must have been written to the challenger.
			resolvedSafe, err := tx.ResourceIdentities().ResolveAlias(ctx, safe)
			if err != nil {
				t.Fatalf("ResolveAlias(safe): %v", err)
			}
			assertEqual(t, "resolved safe", resolvedSafe, domain.ResourceName("nodes/alias-multi-partial-challenger"))

			// And the challenger's own inventory write (unrelated to
			// aliases entirely) must have gone through too.
			view, err := repo.GetView(ctx, "//inv.fleetshift.io/nodes/alias-multi-partial-challenger")
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			assertObservation(t, "challenger Observation", view.Resource.Inventory().Observation(), `{"cpu":4}`)
		})

		// IntraReportSameKeyDifferentValuesConflictsAndKeepsFirst
		// covers checkAliasBatchConsistency's other invariant reached
		// *within* a single report instead of across two reports:
		// one report asserting two different values for the same
		// (namespace, key) is self-contradictory, exactly like
		// ReportsConflictWhenResourceHasDifferentValueForSameKeyAcrossCalls
		// but discovered in one call instead of two.
		t.Run("IntraReportSameKeyDifferentValuesConflictsAndKeepsFirst", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			zoneA, _ := domain.NewAlias("gcp", "zone", "multi-samekey-a")
			zoneB, _ := domain.NewAlias("gcp", "zone", "multi-samekey-b")
			name := domain.ResourceName("nodes/alias-multi-samekey")
			now := fixedTime.Add(time.Minute)
			conflicts, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{zoneA, zoneB},
				ObservedAt:   now,
				ReceivedAt:   now,
			}})
			if err != nil {
				t.Fatalf("ReplaceInventory: %v", err)
			}
			if len(conflicts) != 1 {
				t.Fatalf("conflicts len = %d, want 1: %+v", len(conflicts), conflicts)
			}
			assertEqual(t, "conflict.Alias", conflicts[0].Alias, zoneB)
			assertEqual(t, "conflict.Kind", conflicts[0].Kind, domain.AliasConflictResourceHasDifferentValue)
			assertEqual(t, "conflict.TargetName", conflicts[0].TargetName, name)
			assertEqual(t, "conflict.ActualValue", conflicts[0].ActualValue, zoneA.Value)

			// zoneA (listed first in the report) must have won; zoneB
			// must never have been written at all.
			resolvedA, err := tx.ResourceIdentities().ResolveAlias(ctx, zoneA)
			if err != nil {
				t.Fatalf("ResolveAlias(zoneA): %v", err)
			}
			assertEqual(t, "resolved zoneA", resolvedA, name)
			if _, err := tx.ResourceIdentities().ResolveAlias(ctx, zoneB); !errors.Is(err, domain.ErrNotFound) {
				t.Errorf("ResolveAlias(zoneB): got %v, want ErrNotFound (never written)", err)
			}
		})

		// DuplicateAliasWithinReportIsNotAConflict covers the one
		// case checkAliasBatchConsistency deliberately lets through
		// unflagged: the exact same (namespace, key, value) listed
		// twice for the same target in one report is redundant, not
		// contradictory, and must not surface as a conflict or a hard
		// SQL error from either backend's underlying multi-row insert.
		t.Run("DuplicateAliasWithinReportIsNotAConflict", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			alias, _ := domain.NewAlias("gcp", "instance_id", "multi-exact-duplicate")
			name := domain.ResourceName("nodes/alias-multi-duplicate")
			now := fixedTime.Add(time.Minute)
			conflicts, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{alias, alias},
				ObservedAt:   now,
				ReceivedAt:   now,
			}})
			if err != nil {
				t.Fatalf("ReplaceInventory: %v", err)
			}
			if len(conflicts) != 0 {
				t.Fatalf("conflicts = %+v, want none", conflicts)
			}

			resolved, err := tx.ResourceIdentities().ResolveAlias(ctx, alias)
			if err != nil {
				t.Fatalf("ResolveAlias: %v", err)
			}
			assertEqual(t, "resolved name", resolved, name)
		})

		// ApplyInventoryDeltasResolvesMultipleAliasesFromSingleReport
		// is ResolvesMultipleDistinctAliasesFromSingleReport's Delta
		// counterpart: the two entry points assemble aliasCandidates
		// independently (see ApplyInventoryDeltas/ReplaceInventory in
		// extension_resource_repo.go), so multi-alias support on one
		// path is never a guarantee it also works on the other.
		t.Run("ApplyInventoryDeltasResolvesMultipleAliasesFromSingleReport", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			instanceID, _ := domain.NewAlias("gcp", "instance_id", "multi-delta-instance-1")
			zone, _ := domain.NewAlias("gcp", "zone", "multi-delta-zone-1")
			name := domain.ResourceName("nodes/alias-multi-delta")
			now := fixedTime.Add(time.Minute)
			conflicts, err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{instanceID, zone},
				ObservedAt:   now,
				ReceivedAt:   now,
			}})
			if err != nil {
				t.Fatalf("ApplyInventoryDeltas: %v", err)
			}
			if len(conflicts) != 0 {
				t.Fatalf("conflicts = %+v, want none", conflicts)
			}

			for _, a := range []domain.Alias{instanceID, zone} {
				resolved, err := tx.ResourceIdentities().ResolveAlias(ctx, a)
				if err != nil {
					t.Fatalf("ResolveAlias(%+v): %v", a, err)
				}
				assertEqual(t, fmt.Sprintf("resolved(%+v)", a), resolved, name)
			}
		})

		// ---------------------------------------------------------------
		// Multi-contributor aliases: many different extension resources
		// (distinguished by ResourceType, i.e. different addons/services
		// -- see [domain.ResourceType]'s doc) can represent the very same
		// platform Name at once, e.g. a cluster represented by both a
		// cloud-provider addon and a Kubernetes addon. Every test above
		// this point uses a single ResourceType per Name, so it only ever
		// exercises one contributor (repeated calls with the same
		// ResourceType+Name resolve to the same extension_resources row
		// by natural key -- see [domain.InventoryReplacement]'s doc --
		// which is "the same contributor reporting again", not "a
		// different contributor"). The tests below use two distinct
		// ResourceTypes targeting one Name to exercise genuine
		// multi-contributor scenarios: aliases are additive across
		// contributors, replaced independently per contributor
		// (whether that contributor stops asserting an alias via a
		// later report or via outright deletion of its extension
		// resource), and still guarded by the same two conflict
		// kinds, now scoped across contributors rather than within
		// one.
		//
		// As of this writing, neither backend implements per-contributor
		// alias tracking yet -- aliases are still immutable-once-set at
		// the storage layer with no notion of which extension resource
		// contributed which row -- so every test in this group is
		// expected to fail red until that lands. They exist to pin the
		// target contract down before its implementation; see
		// docs/design/architecture/resource_identity_and_api.md's
		// "Aliases" section for the same contract in prose.

		t.Run("DifferentExtensionResourcesForSameNameContributeDistinctAliases", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			rtCloud := domain.ResourceType("gcp.fleetshift.io/Instance")
			rtK8s := domain.ResourceType("kind.fleetshift.io/Node")
			if err := repo.CreateType(ctx, sampleInventoryType(rtCloud)); err != nil {
				t.Fatalf("CreateType(cloud): %v", err)
			}
			if err := repo.CreateType(ctx, sampleInventoryType(rtK8s)); err != nil {
				t.Fatalf("CreateType(k8s): %v", err)
			}

			name := domain.ResourceName("clusters/multi-contributor-distinct")
			instanceID, _ := domain.NewAlias("gcp", "instance_id", "mc-distinct-instance")
			nodeName, _ := domain.NewAlias("k8s", "node_name", "mc-distinct-node")

			now := fixedTime.Add(time.Minute)
			conflicts, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{
				{
					ResourceType: rtCloud,
					Name:         name,
					CandidateUID: domain.NewExtensionResourceUID(),
					Aliases:      []domain.Alias{instanceID},
					ObservedAt:   now,
					ReceivedAt:   now,
				},
				{
					ResourceType: rtK8s,
					Name:         name,
					CandidateUID: domain.NewExtensionResourceUID(),
					Aliases:      []domain.Alias{nodeName},
					ObservedAt:   now,
					ReceivedAt:   now,
				},
			})
			if err != nil {
				t.Fatalf("ReplaceInventory: %v", err)
			}
			if len(conflicts) != 0 {
				t.Fatalf("conflicts = %+v, want none", conflicts)
			}

			for _, a := range []domain.Alias{instanceID, nodeName} {
				resolved, err := tx.ResourceIdentities().ResolveAlias(ctx, a)
				if err != nil {
					t.Fatalf("ResolveAlias(%+v): %v", a, err)
				}
				assertEqual(t, fmt.Sprintf("resolved(%+v)", a), resolved, name)
			}
		})

		t.Run("DifferentExtensionResourcesForSameNameCanShareTheSameAliasValue", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			rtCloud := domain.ResourceType("gcp.fleetshift.io/Instance")
			rtK8s := domain.ResourceType("kind.fleetshift.io/Node")
			if err := repo.CreateType(ctx, sampleInventoryType(rtCloud)); err != nil {
				t.Fatalf("CreateType(cloud): %v", err)
			}
			if err := repo.CreateType(ctx, sampleInventoryType(rtK8s)); err != nil {
				t.Fatalf("CreateType(k8s): %v", err)
			}

			name := domain.ResourceName("clusters/multi-contributor-shared-value")
			shared, _ := domain.NewAlias("gcp", "zone", "mc-shared-value")

			t1 := fixedTime.Add(time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: rtCloud,
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{shared},
				ObservedAt:   t1,
				ReceivedAt:   t1,
			}}); err != nil {
				t.Fatalf("seed cloud ReplaceInventory: %v", err)
			}

			t2 := fixedTime.Add(2 * time.Minute)
			conflicts, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: rtK8s,
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{shared},
				ObservedAt:   t2,
				ReceivedAt:   t2,
			}})
			if err != nil {
				t.Fatalf("ReplaceInventory (k8s): %v", err)
			}
			if len(conflicts) != 0 {
				t.Fatalf("conflicts = %+v, want none", conflicts)
			}

			resolved, err := tx.ResourceIdentities().ResolveAlias(ctx, shared)
			if err != nil {
				t.Fatalf("ResolveAlias: %v", err)
			}
			assertEqual(t, "resolved name", resolved, name)
		})

		t.Run("DifferentExtensionResourcesForSameNameWithDifferentValuesConflicts", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			rtCloud := domain.ResourceType("gcp.fleetshift.io/Instance")
			rtK8s := domain.ResourceType("kind.fleetshift.io/Node")
			if err := repo.CreateType(ctx, sampleInventoryType(rtCloud)); err != nil {
				t.Fatalf("CreateType(cloud): %v", err)
			}
			if err := repo.CreateType(ctx, sampleInventoryType(rtK8s)); err != nil {
				t.Fatalf("CreateType(k8s): %v", err)
			}

			name := domain.ResourceName("clusters/multi-contributor-value-conflict")
			cloudZone, _ := domain.NewAlias("gcp", "zone", "mc-value-conflict-cloud")
			k8sZone, _ := domain.NewAlias("gcp", "zone", "mc-value-conflict-k8s")

			t1 := fixedTime.Add(time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: rtCloud,
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{cloudZone},
				ObservedAt:   t1,
				ReceivedAt:   t1,
			}}); err != nil {
				t.Fatalf("seed cloud ReplaceInventory: %v", err)
			}

			t2 := fixedTime.Add(2 * time.Minute)
			conflicts, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: rtK8s,
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{k8sZone},
				ObservedAt:   t2,
				ReceivedAt:   t2,
			}})
			if err != nil {
				t.Fatalf("ReplaceInventory (k8s): %v", err)
			}
			if len(conflicts) != 1 {
				t.Fatalf("conflicts len = %d, want 1: %+v", len(conflicts), conflicts)
			}
			assertEqual(t, "conflict.Kind", conflicts[0].Kind, domain.AliasConflictResourceHasDifferentValue)
			assertEqual(t, "conflict.TargetName", conflicts[0].TargetName, name)
			assertEqual(t, "conflict.ActualValue", conflicts[0].ActualValue, cloudZone.Value)

			resolved, err := tx.ResourceIdentities().ResolveAlias(ctx, cloudZone)
			if err != nil {
				t.Fatalf("ResolveAlias(cloudZone): %v", err)
			}
			assertEqual(t, "resolved cloudZone", resolved, name)
			if _, err := tx.ResourceIdentities().ResolveAlias(ctx, k8sZone); !errors.Is(err, domain.ErrNotFound) {
				t.Errorf("ResolveAlias(k8sZone): got %v, want ErrNotFound (never written)", err)
			}
		})

		t.Run("AliasRetractedWhenSoleContributingExtensionResourceStopsReportingIt", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			rt := domain.ResourceType("gcp.fleetshift.io/Instance")
			if err := repo.CreateType(ctx, sampleInventoryType(rt)); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			name := domain.ResourceName("clusters/multi-contributor-retract-sole")
			alias, _ := domain.NewAlias("gcp", "instance_id", "mc-retract-sole")

			t1 := fixedTime.Add(time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: rt,
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{alias},
				ObservedAt:   t1,
				ReceivedAt:   t1,
			}}); err != nil {
				t.Fatalf("seed ReplaceInventory: %v", err)
			}
			if _, err := tx.ResourceIdentities().ResolveAlias(ctx, alias); err != nil {
				t.Fatalf("ResolveAlias before retraction: %v", err)
			}

			// Same extension resource (same natural key) reports
			// again, this time contributing no aliases at all -- its
			// prior contribution must be retracted since nothing else
			// asserts it.
			t2 := fixedTime.Add(2 * time.Minute)
			conflicts, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: rt,
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      nil,
				ObservedAt:   t2,
				ReceivedAt:   t2,
			}})
			if err != nil {
				t.Fatalf("ReplaceInventory (no aliases): %v", err)
			}
			if len(conflicts) != 0 {
				t.Fatalf("conflicts = %+v, want none", conflicts)
			}

			if _, err := tx.ResourceIdentities().ResolveAlias(ctx, alias); !errors.Is(err, domain.ErrNotFound) {
				t.Errorf("ResolveAlias after retraction: got %v, want ErrNotFound", err)
			}
		})

		// AliasSurvivesWhenOneOfMultipleContributingExtensionResourcesStopsReportingIt
		// is the crux of the whole multi-contributor design: retraction
		// must be reference-counted per alias, not per report. If it
		// were tracked per-Name instead, this test would incorrectly
		// retract the alias entirely as soon as its first contributor
		// stopped reporting it.
		t.Run("AliasSurvivesWhenOneOfMultipleContributingExtensionResourcesStopsReportingIt", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			rtCloud := domain.ResourceType("gcp.fleetshift.io/Instance")
			rtK8s := domain.ResourceType("kind.fleetshift.io/Node")
			if err := repo.CreateType(ctx, sampleInventoryType(rtCloud)); err != nil {
				t.Fatalf("CreateType(cloud): %v", err)
			}
			if err := repo.CreateType(ctx, sampleInventoryType(rtK8s)); err != nil {
				t.Fatalf("CreateType(k8s): %v", err)
			}

			name := domain.ResourceName("clusters/multi-contributor-survives")
			shared, _ := domain.NewAlias("gcp", "instance_id", "mc-survives-shared")

			t1 := fixedTime.Add(time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{
				{
					ResourceType: rtCloud,
					Name:         name,
					CandidateUID: domain.NewExtensionResourceUID(),
					Aliases:      []domain.Alias{shared},
					ObservedAt:   t1,
					ReceivedAt:   t1,
				},
				{
					ResourceType: rtK8s,
					Name:         name,
					CandidateUID: domain.NewExtensionResourceUID(),
					Aliases:      []domain.Alias{shared},
					ObservedAt:   t1,
					ReceivedAt:   t1,
				},
			}); err != nil {
				t.Fatalf("seed ReplaceInventory: %v", err)
			}

			// The cloud contributor stops reporting the alias; the
			// Kubernetes contributor's identical contribution still
			// stands, so the alias must remain resolvable.
			t2 := fixedTime.Add(2 * time.Minute)
			conflicts, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: rtCloud,
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      nil,
				ObservedAt:   t2,
				ReceivedAt:   t2,
			}})
			if err != nil {
				t.Fatalf("ReplaceInventory (cloud drops alias): %v", err)
			}
			if len(conflicts) != 0 {
				t.Fatalf("conflicts = %+v, want none", conflicts)
			}

			resolved, err := tx.ResourceIdentities().ResolveAlias(ctx, shared)
			if err != nil {
				t.Fatalf("ResolveAlias after cloud drops alias: %v", err)
			}
			assertEqual(t, "resolved name", resolved, name)
		})

		t.Run("AliasFullyRetractedOnceAllContributingExtensionResourcesStopReportingIt", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			rtCloud := domain.ResourceType("gcp.fleetshift.io/Instance")
			rtK8s := domain.ResourceType("kind.fleetshift.io/Node")
			if err := repo.CreateType(ctx, sampleInventoryType(rtCloud)); err != nil {
				t.Fatalf("CreateType(cloud): %v", err)
			}
			if err := repo.CreateType(ctx, sampleInventoryType(rtK8s)); err != nil {
				t.Fatalf("CreateType(k8s): %v", err)
			}

			name := domain.ResourceName("clusters/multi-contributor-fully-retracted")
			shared, _ := domain.NewAlias("gcp", "instance_id", "mc-fully-retracted-shared")

			t1 := fixedTime.Add(time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{
				{
					ResourceType: rtCloud,
					Name:         name,
					CandidateUID: domain.NewExtensionResourceUID(),
					Aliases:      []domain.Alias{shared},
					ObservedAt:   t1,
					ReceivedAt:   t1,
				},
				{
					ResourceType: rtK8s,
					Name:         name,
					CandidateUID: domain.NewExtensionResourceUID(),
					Aliases:      []domain.Alias{shared},
					ObservedAt:   t1,
					ReceivedAt:   t1,
				},
			}); err != nil {
				t.Fatalf("seed ReplaceInventory: %v", err)
			}

			t2 := fixedTime.Add(2 * time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{
				{
					ResourceType: rtCloud,
					Name:         name,
					CandidateUID: domain.NewExtensionResourceUID(),
					Aliases:      nil,
					ObservedAt:   t2,
					ReceivedAt:   t2,
				},
				{
					ResourceType: rtK8s,
					Name:         name,
					CandidateUID: domain.NewExtensionResourceUID(),
					Aliases:      nil,
					ObservedAt:   t2,
					ReceivedAt:   t2,
				},
			}); err != nil {
				t.Fatalf("ReplaceInventory (both drop alias): %v", err)
			}

			if _, err := tx.ResourceIdentities().ResolveAlias(ctx, shared); !errors.Is(err, domain.ErrNotFound) {
				t.Errorf("ResolveAlias after both drop alias: got %v, want ErrNotFound", err)
			}
		})

		// The three tests below are Delete's counterpart to the three
		// ReplaceInventory-based retraction tests above: an extension
		// resource can stop contributing an alias either by reporting
		// again without it, or by ceasing to exist entirely. Deletion
		// is not just "the same trigger, different API" -- Delete is
		// a completely separate code path from ReplaceInventory's
		// alias fold-in, and today it doesn't touch resource_aliases
		// at all (there's no foreign key from extension_resources to
		// resource_aliases, unlike representations, which do vanish
		// on delete because they're derived by joining on the
		// extension_resources row directly). So retraction working
		// for the omission case is never a guarantee it also works
		// for the deletion case.

		t.Run("AliasRetractedWhenSoleContributingExtensionResourceIsDeleted", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			rt := domain.ResourceType("gcp.fleetshift.io/Instance")
			if err := repo.CreateType(ctx, sampleInventoryType(rt)); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			name := domain.ResourceName("clusters/multi-contributor-delete-sole")
			alias, _ := domain.NewAlias("gcp", "instance_id", "mc-delete-sole")

			now := fixedTime.Add(time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: rt,
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{alias},
				ObservedAt:   now,
				ReceivedAt:   now,
			}}); err != nil {
				t.Fatalf("seed ReplaceInventory: %v", err)
			}
			if _, err := tx.ResourceIdentities().ResolveAlias(ctx, alias); err != nil {
				t.Fatalf("ResolveAlias before delete: %v", err)
			}

			if err := repo.Delete(ctx, rt.FullName(name)); err != nil {
				t.Fatalf("Delete: %v", err)
			}

			if _, err := tx.ResourceIdentities().ResolveAlias(ctx, alias); !errors.Is(err, domain.ErrNotFound) {
				t.Errorf("ResolveAlias after delete: got %v, want ErrNotFound", err)
			}
		})

		// AliasSurvivesWhenOneOfMultipleContributingExtensionResourcesIsDeleted
		// is this group's version of the crux scenario: deleting one
		// of two contributors must not take down an alias the other
		// contributor is still asserting.
		t.Run("AliasSurvivesWhenOneOfMultipleContributingExtensionResourcesIsDeleted", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			rtCloud := domain.ResourceType("gcp.fleetshift.io/Instance")
			rtK8s := domain.ResourceType("kind.fleetshift.io/Node")
			if err := repo.CreateType(ctx, sampleInventoryType(rtCloud)); err != nil {
				t.Fatalf("CreateType(cloud): %v", err)
			}
			if err := repo.CreateType(ctx, sampleInventoryType(rtK8s)); err != nil {
				t.Fatalf("CreateType(k8s): %v", err)
			}

			name := domain.ResourceName("clusters/multi-contributor-delete-survives")
			shared, _ := domain.NewAlias("gcp", "instance_id", "mc-delete-survives-shared")

			now := fixedTime.Add(time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{
				{
					ResourceType: rtCloud,
					Name:         name,
					CandidateUID: domain.NewExtensionResourceUID(),
					Aliases:      []domain.Alias{shared},
					ObservedAt:   now,
					ReceivedAt:   now,
				},
				{
					ResourceType: rtK8s,
					Name:         name,
					CandidateUID: domain.NewExtensionResourceUID(),
					Aliases:      []domain.Alias{shared},
					ObservedAt:   now,
					ReceivedAt:   now,
				},
			}); err != nil {
				t.Fatalf("seed ReplaceInventory: %v", err)
			}

			// Delete the cloud representation entirely; the
			// Kubernetes representation (and its identical alias
			// contribution) is untouched.
			if err := repo.Delete(ctx, rtCloud.FullName(name)); err != nil {
				t.Fatalf("Delete: %v", err)
			}

			resolved, err := tx.ResourceIdentities().ResolveAlias(ctx, shared)
			if err != nil {
				t.Fatalf("ResolveAlias after cloud deleted: %v", err)
			}
			assertEqual(t, "resolved name", resolved, name)
		})

		t.Run("AliasFullyRetractedOnceAllContributingExtensionResourcesAreDeleted", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			rtCloud := domain.ResourceType("gcp.fleetshift.io/Instance")
			rtK8s := domain.ResourceType("kind.fleetshift.io/Node")
			if err := repo.CreateType(ctx, sampleInventoryType(rtCloud)); err != nil {
				t.Fatalf("CreateType(cloud): %v", err)
			}
			if err := repo.CreateType(ctx, sampleInventoryType(rtK8s)); err != nil {
				t.Fatalf("CreateType(k8s): %v", err)
			}

			name := domain.ResourceName("clusters/multi-contributor-delete-both")
			shared, _ := domain.NewAlias("gcp", "instance_id", "mc-delete-both-shared")

			now := fixedTime.Add(time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{
				{
					ResourceType: rtCloud,
					Name:         name,
					CandidateUID: domain.NewExtensionResourceUID(),
					Aliases:      []domain.Alias{shared},
					ObservedAt:   now,
					ReceivedAt:   now,
				},
				{
					ResourceType: rtK8s,
					Name:         name,
					CandidateUID: domain.NewExtensionResourceUID(),
					Aliases:      []domain.Alias{shared},
					ObservedAt:   now,
					ReceivedAt:   now,
				},
			}); err != nil {
				t.Fatalf("seed ReplaceInventory: %v", err)
			}

			if err := repo.Delete(ctx, rtCloud.FullName(name)); err != nil {
				t.Fatalf("Delete(cloud): %v", err)
			}
			if err := repo.Delete(ctx, rtK8s.FullName(name)); err != nil {
				t.Fatalf("Delete(k8s): %v", err)
			}

			if _, err := tx.ResourceIdentities().ResolveAlias(ctx, shared); !errors.Is(err, domain.ErrNotFound) {
				t.Errorf("ResolveAlias after both deleted: got %v, want ErrNotFound", err)
			}
		})

		// ReplacingOwnAliasWhileSiblingStillHoldsOldValueConflicts
		// covers the subtle interaction between "replace" and
		// "additive across contributors": a contributor is allowed to
		// change its own value (see
		// SameExtensionResourceReplacingOwnAliasValueSucceeds above),
		// but not when doing so would leave the target Name with two
		// contributors disagreeing on the same key -- that's still
		// AliasConflictResourceHasDifferentValue, just discovered via
		// a replace instead of a brand-new report.
		t.Run("ReplacingOwnAliasWhileSiblingStillHoldsOldValueConflicts", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			rtCloud := domain.ResourceType("gcp.fleetshift.io/Instance")
			rtK8s := domain.ResourceType("kind.fleetshift.io/Node")
			if err := repo.CreateType(ctx, sampleInventoryType(rtCloud)); err != nil {
				t.Fatalf("CreateType(cloud): %v", err)
			}
			if err := repo.CreateType(ctx, sampleInventoryType(rtK8s)); err != nil {
				t.Fatalf("CreateType(k8s): %v", err)
			}

			name := domain.ResourceName("clusters/multi-contributor-replace-vs-sibling")
			oldValue, _ := domain.NewAlias("gcp", "zone", "mc-replace-vs-sibling-old")
			newValue, _ := domain.NewAlias("gcp", "zone", "mc-replace-vs-sibling-new")

			t1 := fixedTime.Add(time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{
				{
					ResourceType: rtCloud,
					Name:         name,
					CandidateUID: domain.NewExtensionResourceUID(),
					Aliases:      []domain.Alias{oldValue},
					ObservedAt:   t1,
					ReceivedAt:   t1,
				},
				{
					ResourceType: rtK8s,
					Name:         name,
					CandidateUID: domain.NewExtensionResourceUID(),
					Aliases:      []domain.Alias{oldValue},
					ObservedAt:   t1,
					ReceivedAt:   t1,
				},
			}); err != nil {
				t.Fatalf("seed ReplaceInventory: %v", err)
			}

			// The cloud contributor tries to change its own value, but
			// the Kubernetes contributor is still asserting the old
			// one for the very same (name, namespace, key) -- letting
			// this through would leave the name with two live values
			// for one key at once.
			t2 := fixedTime.Add(2 * time.Minute)
			conflicts, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: rtCloud,
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{newValue},
				ObservedAt:   t2,
				ReceivedAt:   t2,
			}})
			if err != nil {
				t.Fatalf("ReplaceInventory (cloud replace): %v", err)
			}
			if len(conflicts) != 1 {
				t.Fatalf("conflicts len = %d, want 1: %+v", len(conflicts), conflicts)
			}
			assertEqual(t, "conflict.Kind", conflicts[0].Kind, domain.AliasConflictResourceHasDifferentValue)
			assertEqual(t, "conflict.TargetName", conflicts[0].TargetName, name)
			assertEqual(t, "conflict.ActualValue", conflicts[0].ActualValue, oldValue.Value)

			resolvedOld, err := tx.ResourceIdentities().ResolveAlias(ctx, oldValue)
			if err != nil {
				t.Fatalf("ResolveAlias(oldValue): %v", err)
			}
			assertEqual(t, "resolved oldValue", resolvedOld, name)
			if _, err := tx.ResourceIdentities().ResolveAlias(ctx, newValue); !errors.Is(err, domain.ErrNotFound) {
				t.Errorf("ResolveAlias(newValue): got %v, want ErrNotFound (rejected replace)", err)
			}
		})

		t.Run("ReplacingOwnAliasToValueAlreadyOwnedByDifferentNameConflicts", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			rt := domain.ResourceType("gcp.fleetshift.io/Instance")
			if err := repo.CreateType(ctx, sampleInventoryType(rt)); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			otherName := domain.ResourceName("clusters/multi-contributor-replace-other-owner")
			claimedValue, _ := domain.NewAlias("gcp", "zone", "mc-replace-other-owner-claimed")
			t0 := fixedTime.Add(time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: rt,
				Name:         otherName,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{claimedValue},
				ObservedAt:   t0,
				ReceivedAt:   t0,
			}}); err != nil {
				t.Fatalf("seed other-owner ReplaceInventory: %v", err)
			}

			name := domain.ResourceName("clusters/multi-contributor-replace-self")
			ownValue, _ := domain.NewAlias("gcp", "zone", "mc-replace-self-own")
			t1 := fixedTime.Add(2 * time.Minute)
			if _, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: rt,
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{ownValue},
				ObservedAt:   t1,
				ReceivedAt:   t1,
			}}); err != nil {
				t.Fatalf("seed self ReplaceInventory: %v", err)
			}

			// This extension resource tries to replace its own alias
			// value with one that already belongs to a totally
			// different name -- still a conflict, replace or not.
			t2 := fixedTime.Add(3 * time.Minute)
			conflicts, err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: rt,
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{claimedValue},
				ObservedAt:   t2,
				ReceivedAt:   t2,
			}})
			if err != nil {
				t.Fatalf("ReplaceInventory (self replace): %v", err)
			}
			if len(conflicts) != 1 {
				t.Fatalf("conflicts len = %d, want 1: %+v", len(conflicts), conflicts)
			}
			assertEqual(t, "conflict.Kind", conflicts[0].Kind, domain.AliasConflictValueClaimedByOther)
			assertEqual(t, "conflict.TargetName", conflicts[0].TargetName, name)
			assertEqual(t, "conflict.ActualName", conflicts[0].ActualName, otherName)

			// Its own original alias must still stand, untouched by
			// the rejected replace attempt.
			resolvedOwn, err := tx.ResourceIdentities().ResolveAlias(ctx, ownValue)
			if err != nil {
				t.Fatalf("ResolveAlias(ownValue): %v", err)
			}
			assertEqual(t, "resolved ownValue", resolvedOwn, name)
		})

		t.Run("ApplyInventoryDeltasAdditiveAcrossDifferentExtensionResourcesForSameName", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			rtCloud := domain.ResourceType("gcp.fleetshift.io/Instance")
			rtK8s := domain.ResourceType("kind.fleetshift.io/Node")
			if err := repo.CreateType(ctx, sampleInventoryType(rtCloud)); err != nil {
				t.Fatalf("CreateType(cloud): %v", err)
			}
			if err := repo.CreateType(ctx, sampleInventoryType(rtK8s)); err != nil {
				t.Fatalf("CreateType(k8s): %v", err)
			}

			name := domain.ResourceName("clusters/multi-contributor-delta")
			instanceID, _ := domain.NewAlias("gcp", "instance_id", "mc-delta-instance")
			nodeName, _ := domain.NewAlias("k8s", "node_name", "mc-delta-node")

			now := fixedTime.Add(time.Minute)
			conflicts, err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{
				{
					ResourceType: rtCloud,
					Name:         name,
					CandidateUID: domain.NewExtensionResourceUID(),
					Aliases:      []domain.Alias{instanceID},
					ObservedAt:   now,
					ReceivedAt:   now,
				},
				{
					ResourceType: rtK8s,
					Name:         name,
					CandidateUID: domain.NewExtensionResourceUID(),
					Aliases:      []domain.Alias{nodeName},
					ObservedAt:   now,
					ReceivedAt:   now,
				},
			})
			if err != nil {
				t.Fatalf("ApplyInventoryDeltas: %v", err)
			}
			if len(conflicts) != 0 {
				t.Fatalf("conflicts = %+v, want none", conflicts)
			}

			for _, a := range []domain.Alias{instanceID, nodeName} {
				resolved, err := tx.ResourceIdentities().ResolveAlias(ctx, a)
				if err != nil {
					t.Fatalf("ResolveAlias(%+v): %v", a, err)
				}
				assertEqual(t, fmt.Sprintf("resolved(%+v)", a), resolved, name)
			}
		})

		// ApplyInventoryDeltasNeverRetractsAliasesOnOmission locks in
		// an intentional asymmetry with ReplaceInventory: a delta's
		// Aliases is additive/upsert-only (like every other Delta
		// field), so omitting a previously-contributed alias from a
		// later delta must NOT retract it -- only a ReplaceInventory
		// call (the "this is my complete state" entry point) can do
		// that. See [domain.InventoryDelta]'s doc. Unlike the rest of
		// this group, this describes *current*, already-correct
		// behavior -- it's here to lock the asymmetry in as
		// intentional so it isn't "fixed" away by accident once
		// per-contributor replace lands for ReplaceInventory.
		t.Run("ApplyInventoryDeltasNeverRetractsAliasesOnOmission", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			rt := domain.ResourceType("gcp.fleetshift.io/Instance")
			if err := repo.CreateType(ctx, sampleInventoryType(rt)); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			name := domain.ResourceName("clusters/multi-contributor-delta-no-retract")
			alias, _ := domain.NewAlias("gcp", "instance_id", "mc-delta-no-retract")

			t1 := fixedTime.Add(time.Minute)
			if _, err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType: rt,
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      []domain.Alias{alias},
				ObservedAt:   t1,
				ReceivedAt:   t1,
			}}); err != nil {
				t.Fatalf("seed ApplyInventoryDeltas: %v", err)
			}

			t2 := fixedTime.Add(2 * time.Minute)
			if _, err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType: rt,
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      nil,
				ObservedAt:   t2,
				ReceivedAt:   t2,
			}}); err != nil {
				t.Fatalf("ApplyInventoryDeltas (heartbeat, no aliases): %v", err)
			}

			resolved, err := tx.ResourceIdentities().ResolveAlias(ctx, alias)
			if err != nil {
				t.Fatalf("ResolveAlias: %v", err)
			}
			assertEqual(t, "resolved name", resolved, name)
		})
	})
}

func assertEqual[T comparable](t *testing.T, field string, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %v, want %v", field, got, want)
	}
}

// mustCondition constructs a [domain.Condition] for use in
// [domain.InventoryReplacement.Conditions] / [domain.InventoryDelta]'s
// condition fields, failing the test on construction error.
func mustCondition(
	t *testing.T,
	conditionType domain.ConditionType,
	status domain.ConditionStatus,
	reason, message string,
	lastTransitionTime time.Time,
) domain.Condition {
	t.Helper()
	c, err := domain.NewCondition(conditionType, status, reason, message, lastTransitionTime)
	if err != nil {
		t.Fatalf("NewCondition: %v", err)
	}
	return c
}

// assertObservation asserts that a possibly-nil observation pointer is
// non-nil and matches the expected JSON payload.
func assertObservation(t *testing.T, field string, got *json.RawMessage, want string) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s is nil, want %q", field, want)
	}
	assertEqual(t, field, string(*got), want)
}
