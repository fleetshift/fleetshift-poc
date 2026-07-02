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

		t.Run("ReportsConflictWhenResourceHasDifferentValueForSameKeyAcrossCalls", func(t *testing.T) {
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
			if len(conflicts) != 1 {
				t.Fatalf("conflicts len = %d, want 1: %+v", len(conflicts), conflicts)
			}
			assertEqual(t, "conflict.Kind", conflicts[0].Kind, domain.AliasConflictResourceHasDifferentValue)
			assertEqual(t, "conflict.TargetName", conflicts[0].TargetName, name)
			assertEqual(t, "conflict.ActualValue", conflicts[0].ActualValue, firstValue.Value)

			// The resource's alias must still carry its original value.
			resolved, err := tx.ResourceIdentities().ResolveAlias(ctx, firstValue)
			if err != nil {
				t.Fatalf("ResolveAlias(firstValue): %v", err)
			}
			assertEqual(t, "resolved name", resolved, name)
			if _, err := tx.ResourceIdentities().ResolveAlias(ctx, secondValue); !errors.Is(err, domain.ErrNotFound) {
				t.Errorf("ResolveAlias(secondValue): got %v, want ErrNotFound (never inserted)", err)
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
