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

func aliasSet(aliases ...domain.Alias) domain.AliasSet {
	return domain.NewAliasSet(aliases)
}

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
		if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
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
		// CreatesLatestState also pins the new empty-history contract
		// described in the "History" subtest below: even a report
		// that carries both an observation and a condition -- the
		// exact shape that used to seed observation history and an
		// initial condition transition -- must leave both history
		// lists empty now that history bookkeeping is deferred to a
		// future async writer (see docs/design/architecture/resource_indexing.md
		// and this package's "History" subtest).
		t.Run("CreatesLatestState", func(t *testing.T) {
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
			err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
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
			if len(obsHistory) != 0 {
				t.Fatalf("observation history len = %d, want 0 (hot path no longer writes history)", len(obsHistory))
			}

			transitions, err := repo.ListConditionTransitions(ctx, r.UID(), nil, 10)
			if err != nil {
				t.Fatalf("ListConditionTransitions: %v", err)
			}
			if len(transitions) != 0 {
				t.Fatalf("transitions len = %d, want 0 (hot path no longer writes history)", len(transitions))
			}
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
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: &obs1,
				ObservedAt:  now,
				ReceivedAt:  now,
			}}); err != nil {
				t.Fatalf("first ReplaceInventory: %v", err)
			}

			later := now.Add(time.Minute)
			obs2 := json.RawMessage(`{"cpu":8}`)
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
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
			err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{
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

		// SameConditionDoesNotDuplicateTransition, ChangedConditionRecordsTransition,
		// and ReturnToPastStateRecordsGenuineTransition used to live
		// here, pinning synchronous condition-transition history
		// (dedup on repeat, a new row on genuine change, a new row on
		// return to a past state). That bookkeeping moved out of the
		// hot path -- see the "History" subtest's empty-history
		// assertions -- and the latest-state half of what they
		// checked (each successive replace's Conditions() reflecting
		// only the most recent report) is already covered by
		// RemovesConditionsAbsentFromReplacement below and by
		// UpdatesExisting above for observations.

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
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
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
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
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
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Labels:     map[string]string{"zone": "us-east-1", "team": "platform"},
				ObservedAt: t1,
				ReceivedAt: t1,
			}}); err != nil {
				t.Fatalf("first ReplaceInventory: %v", err)
			}

			t2 := fixedTime.Add(2 * time.Minute)
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
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
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: &obs,
				ObservedAt:  t1,
				ReceivedAt:  t1,
			}}); err != nil {
				t.Fatalf("first ReplaceInventory: %v", err)
			}

			t2 := fixedTime.Add(2 * time.Minute)
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
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
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: &obs,
				ObservedAt:  t1,
				ReceivedAt:  t1,
			}}); err != nil {
				t.Fatalf("first ReplaceInventory: %v", err)
			}

			// A non-nil pointer to the JSON literal null must behave
			// identically to a nil pointer: untouched.
			nullLiteral := json.RawMessage(`null`)
			t2 := fixedTime.Add(2 * time.Minute)
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
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
			err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
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
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
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
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Labels:     map[string]string{"zone": "us-east-1", "tier": "1"},
				ObservedAt: t1,
				ReceivedAt: t1,
			}}); err != nil {
				t.Fatalf("seed ReplaceInventory: %v", err)
			}

			t2 := fixedTime.Add(2 * time.Minute)
			if err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
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
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: &obs,
				ObservedAt:  t1,
				ReceivedAt:  t1,
			}}); err != nil {
				t.Fatalf("seed ReplaceInventory: %v", err)
			}

			t2 := fixedTime.Add(2 * time.Minute)
			if err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
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
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: &obs1,
				ObservedAt:  t1,
				ReceivedAt:  t1,
			}}); err != nil {
				t.Fatalf("seed ReplaceInventory: %v", err)
			}

			t2 := fixedTime.Add(2 * time.Minute)
			obs2 := json.RawMessage(`{"cpu":8}`)
			if err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
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
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: &obs,
				ObservedAt:  t1,
				ReceivedAt:  t1,
			}}); err != nil {
				t.Fatalf("seed ReplaceInventory: %v", err)
			}

			// A non-nil pointer to the JSON literal null must behave
			// identically to a nil pointer: untouched.
			nullLiteral := json.RawMessage(`null`)
			t2 := fixedTime.Add(2 * time.Minute)
			if err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
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
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
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
			if err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
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
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Labels:     map[string]string{"zone": "us-east-1"},
				ObservedAt: t1,
				ReceivedAt: t1,
			}}); err != nil {
				t.Fatalf("seed ReplaceInventory: %v", err)
			}

			t2 := fixedTime.Add(2 * time.Minute)
			if err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
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
			err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
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
			if err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				SetLabels:  map[string]string{"zone": "us-east-1"},
				ObservedAt: t1, ReceivedAt: t1,
			}}); err != nil {
				t.Fatalf("seed ApplyInventoryDeltas: %v", err)
			}

			t2 := fixedTime.Add(2 * time.Minute)
			err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
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
			if err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				UpsertConditions: []domain.Condition{mustCondition(t, "Ready", domain.ConditionTrue, "AllGood", "ok", t1)},
				ObservedAt:       t1, ReceivedAt: t1,
			}}); err != nil {
				t.Fatalf("seed ApplyInventoryDeltas: %v", err)
			}

			t2 := fixedTime.Add(2 * time.Minute)
			err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
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

		// RejectsDeleteAliasesAsUnimplemented/RejectsReplaceAliasesAsUnimplemented
		// guard [domain.InventoryDelta]'s doc: neither op is
		// implemented against the reported-alias payload yet, so
		// [domain.ValidateInventoryDelta] rejects any non-empty value
		// outright (with [domain.ErrUnimplemented], not silently
		// accepting-and-ignoring it, which would leave stale pending
		// aliases with no indication anything went wrong) regardless
		// of what else -- if anything -- the delta combines it with.
		t.Run("RejectsDeleteAliasesAsUnimplemented", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/delta-alias-delete-unimplemented")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}
			original, _ := domain.NewAlias("gcp", "instance_id", "delta-alias-delete-unimplemented-original")
			t1 := fixedTime.Add(time.Minute)
			if err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				UpsertAliases: aliasSet(original),
				ObservedAt:    t1, ReceivedAt: t1,
			}}); err != nil {
				t.Fatalf("seed ApplyInventoryDeltas: %v", err)
			}

			removeRef, _ := domain.NewAliasRef("gcp", "instance_id")
			t2 := fixedTime.Add(2 * time.Minute)
			err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				DeleteAliases: []domain.AliasRef{removeRef},
				ObservedAt:    t2, ReceivedAt: t2,
			}})
			if !errors.Is(err, domain.ErrUnimplemented) {
				t.Fatalf("ApplyInventoryDeltas err = %v, want ErrUnimplemented", err)
			}

			// Rejected before any write, so the pending alias payload
			// from the seed call must be untouched -- still just
			// [original], not retracted.
			got, err := repo.Get(ctx, r.ResourceType().FullName(r.Name()))
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if reported := collectAliases(got.ReportedAliases()); len(reported) != 1 || reported[0] != original {
				t.Fatalf("ReportedAliases() = %+v, want unchanged [%+v]", reported, original)
			}
		})

		t.Run("RejectsReplaceAliasesAsUnimplemented", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/delta-alias-replace-unimplemented")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}
			replaceAlias, _ := domain.NewAlias("gcp", "instance_id", "delta-alias-replace-unimplemented-value")
			now := fixedTime.Add(time.Minute)
			err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				ReplaceAliases: aliasSet(replaceAlias),
				ObservedAt:     now, ReceivedAt: now,
			}})
			if !errors.Is(err, domain.ErrUnimplemented) {
				t.Fatalf("ApplyInventoryDeltas err = %v, want ErrUnimplemented", err)
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
			if err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
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
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
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
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
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

	// History pins the "hot path no longer writes history" contract:
	// [domain.Observation]/[domain.ConditionTransition] and their
	// List methods are kept for a future async writer (see
	// docs/design/architecture/resource_indexing.md), but
	// ReplaceInventory/ApplyInventoryDeltas themselves never populate
	// extension_resource_inventory_observations or
	// extension_resource_inventory_condition_events any more -- not
	// even for the first report on a resource, and not even for a
	// report that changes an observation/condition from one real
	// value to another (the exact shapes that used to append rows
	// under the old synchronous dedup/transition-detection logic).
	// Filter-by-type and ordering coverage for the List methods
	// themselves is deferred until an async writer exists to
	// populate the tables in the first place; querying empty tables
	// can't exercise that logic.
	t.Run("History", func(t *testing.T) {
		t.Run("ReplaceInventoryNeverWritesHistory", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/history-replace")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			t1 := fixedTime.Add(time.Minute)
			obs1 := json.RawMessage(`{"v":1}`)
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: &obs1,
				Conditions:  []domain.Condition{mustCondition(t, "Ready", domain.ConditionTrue, "AllGood", "ok", t1)},
				ObservedAt:  t1,
				ReceivedAt:  t1,
			}}); err != nil {
				t.Fatalf("first ReplaceInventory: %v", err)
			}

			// A second report with a genuinely different observation
			// and condition value -- exactly the shape that used to
			// append a second history row and a transition under the
			// old synchronous dedup logic.
			t2 := fixedTime.Add(2 * time.Minute)
			obs2 := json.RawMessage(`{"v":2}`)
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation: &obs2,
				Conditions:  []domain.Condition{mustCondition(t, "Ready", domain.ConditionFalse, "Degraded", "broke", t2)},
				ObservedAt:  t2,
				ReceivedAt:  t2,
			}}); err != nil {
				t.Fatalf("second ReplaceInventory: %v", err)
			}

			obsHistory, err := repo.ListObservations(ctx, r.UID(), 10)
			if err != nil {
				t.Fatalf("ListObservations: %v", err)
			}
			if len(obsHistory) != 0 {
				t.Fatalf("observation history len = %d, want 0", len(obsHistory))
			}

			transitions, err := repo.ListConditionTransitions(ctx, r.UID(), nil, 10)
			if err != nil {
				t.Fatalf("ListConditionTransitions: %v", err)
			}
			if len(transitions) != 0 {
				t.Fatalf("transitions len = %d, want 0", len(transitions))
			}
		})

		t.Run("ApplyInventoryDeltasNeverWritesHistory", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}
			r := newInventoryER("inv.fleetshift.io/Node", "nodes/history-delta")
			if err := repo.Create(ctx, r); err != nil {
				t.Fatalf("Create: %v", err)
			}

			t1 := fixedTime.Add(time.Minute)
			obs1 := json.RawMessage(`{"v":1}`)
			if err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation:      &obs1,
				UpsertConditions: []domain.Condition{mustCondition(t, "Ready", domain.ConditionTrue, "AllGood", "ok", t1)},
				ObservedAt:       t1,
				ReceivedAt:       t1,
			}}); err != nil {
				t.Fatalf("first ApplyInventoryDeltas: %v", err)
			}

			t2 := fixedTime.Add(2 * time.Minute)
			obs2 := json.RawMessage(`{"v":2}`)
			if err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Observation:      &obs2,
				UpsertConditions: []domain.Condition{mustCondition(t, "Ready", domain.ConditionFalse, "Degraded", "broke", t2)},
				ObservedAt:       t2,
				ReceivedAt:       t2,
			}}); err != nil {
				t.Fatalf("second ApplyInventoryDeltas: %v", err)
			}

			obsHistory, err := repo.ListObservations(ctx, r.UID(), 10)
			if err != nil {
				t.Fatalf("ListObservations: %v", err)
			}
			if len(obsHistory) != 0 {
				t.Fatalf("observation history len = %d, want 0", len(obsHistory))
			}

			transitions, err := repo.ListConditionTransitions(ctx, r.UID(), nil, 10)
			if err != nil {
				t.Fatalf("ListConditionTransitions: %v", err)
			}
			if len(transitions) != 0 {
				t.Fatalf("transitions len = %d, want 0", len(transitions))
			}
		})

		// MultipleUIDsInOneDeltaBatchGetDistinctLatestState no longer
		// belongs under "History" now that there's no transition
		// history to inspect per UID, but it earns its keep as a
		// batch-correctness check in its own right: a batch bug that
		// copied the first delta's payload to every row in the batch
		// would slip past single-report tests entirely.
		t.Run("MultipleUIDsInOneDeltaBatchGetDistinctLatestState", func(t *testing.T) {
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
			t2 := fixedTime.Add(2 * time.Minute)
			if err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{
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
		})

		// AlternatingReplaceAndDeltaCallsConvergeOnLatestState replaces
		// the old CrossPathConsistencyAcrossReplaceAndDelta, which
		// mostly pinned synchronous transition-counting behavior that
		// no longer exists. What's left worth pinning: ReplaceInventory
		// and ApplyInventoryDeltas write the very same latest-state row,
		// so alternating between them must still converge on whichever
		// call happened last, with no path-dependent drift.
		t.Run("AlternatingReplaceAndDeltaCallsConvergeOnLatestState", func(t *testing.T) {
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

			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Conditions: []domain.Condition{mustCondition(t, "Ready", domain.ConditionTrue, "AllGood", "ok", t1)},
				ObservedAt: t1,
				ReceivedAt: t1,
			}}); err != nil {
				t.Fatalf("replace: %v", err)
			}
			if err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				UpsertConditions: []domain.Condition{mustCondition(t, "Ready", domain.ConditionFalse, "Degraded", "broke", t2)},
				ObservedAt:       t2,
				ReceivedAt:       t2,
			}}); err != nil {
				t.Fatalf("delta: %v", err)
			}
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: r.ResourceType(), Name: r.Name(), CandidateUID: domain.NewExtensionResourceUID(),
				Conditions: []domain.Condition{mustCondition(t, "Ready", domain.ConditionTrue, "Recovered", "back", t3)},
				ObservedAt: t3,
				ReceivedAt: t3,
			}}); err != nil {
				t.Fatalf("final replace: %v", err)
			}

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
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
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
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
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
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{
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

	// Aliases exercises the "pending, unreconciled" alias contract
	// ReplaceInventory/ApplyInventoryDeltas store on
	// extension_resources.reported_aliases (see
	// [domain.InventoryReplacement.Aliases]'s doc): the hot path
	// canonicalizes and stores whatever an extension resource
	// asserts, uses an internal unchanged-alias fast path to skip
	// redundant payload writes, and never classifies or rejects
	// conflicts synchronously -- reconciling conflicting assertions
	// from different extension resources is deferred to a future
	// accepted-identity process this branch does not implement.
	//
	// This replaces a much larger predecessor that pinned synchronous
	// cross-resource conflict detection against
	// resource_alias_claims/resource_alias_contributions (additive,
	// per-name claims with immediate alias-resolution visibility
	// through the resource identity repository). That
	// mechanism is now unreachable from inventory reporting; see
	// resourceidentityrepotest's own contract tests for the one path
	// that still exercises it (a platform-owned claim added directly
	// via AddAlias, independent of inventory reporting).
	t.Run("Aliases", func(t *testing.T) {
		t.Run("ReplaceInventoryStoresCanonicalPayload", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			name := domain.ResourceName("nodes/alias-canonical")
			// Reported out of canonical (namespace, key, value)
			// order -- the stored payload must still come back
			// sorted.
			zone, _ := domain.NewAlias("gcp", "zone", "alias-canonical-zone")
			instanceID, _ := domain.NewAlias("gcp", "instance_id", "alias-canonical-instance")
			now := fixedTime.Add(time.Minute)
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      aliasSet(zone, instanceID),
				ObservedAt:   now,
				ReceivedAt:   now,
			}}); err != nil {
				t.Fatalf("ReplaceInventory: %v", err)
			}

			got, err := repo.Get(ctx, domain.NewFullResourceName("inv.fleetshift.io", name))
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			assertAliasesEqual(t, "ReportedAliases()", got.ReportedAliases(), []domain.Alias{zone, instanceID})
		})

		t.Run("NoAliasesStoresEmptyPayload", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			name := domain.ResourceName("nodes/alias-none")
			now := fixedTime.Add(time.Minute)
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				ObservedAt:   now,
				ReceivedAt:   now,
			}}); err != nil {
				t.Fatalf("ReplaceInventory: %v", err)
			}

			got, err := repo.Get(ctx, domain.NewFullResourceName("inv.fleetshift.io", name))
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if reported := collectAliases(got.ReportedAliases()); len(reported) != 0 {
				t.Fatalf("ReportedAliases() = %+v, want empty (never nil)", reported)
			}
		})

		// DeltaCreatedResourceSupportsNoAliasReplaceSkip guards a
		// backend-consistency requirement: a resource resolve-or-created
		// by ApplyInventoryDeltas (which, unlike ReplaceInventory, never
		// carries a caller-supplied alias set to seed at creation -- see
		// [domain.InventoryDelta]'s doc) must still behave like a
		// resource whose latest reported alias payload is the empty set.
		// Both backends must agree, or a follow-up no-alias
		// ReplaceInventory report would redundantly rewrite the alias
		// payload instead of hitting the unchanged-alias fast path.
		t.Run("DeltaCreatedResourceSupportsNoAliasReplaceSkip", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			name := domain.ResourceName("nodes/delta-created-alias-payload")
			t1 := fixedTime.Add(time.Minute)
			if err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				SetLabels:    map[string]string{"env": "prod"},
				ObservedAt:   t1, ReceivedAt: t1,
			}}); err != nil {
				t.Fatalf("ApplyInventoryDeltas: %v", err)
			}

			got, err := repo.Get(ctx, domain.NewFullResourceName("inv.fleetshift.io", name))
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if reported := collectAliases(got.ReportedAliases()); len(reported) != 0 {
				t.Fatalf("ReportedAliases() after delta-create = %+v, want empty", reported)
			}
			extensionResourcesUpdatedAt := got.UpdatedAt()

			// A later no-alias ReplaceInventory report should find
			// its alias payload already matching and skip the alias
			// payload write entirely -- observable as
			// extension_resources.updated_at (and therefore
			// UpdatedAt()) staying put even though the report
			// succeeds and inventory freshness still moves.
			t2 := fixedTime.Add(2 * time.Minute)
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				ObservedAt:   t2, ReceivedAt: t2,
			}}); err != nil {
				t.Fatalf("ReplaceInventory: %v", err)
			}

			got2, err := repo.Get(ctx, domain.NewFullResourceName("inv.fleetshift.io", name))
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if !got2.UpdatedAt().Equal(extensionResourcesUpdatedAt) {
				t.Errorf("UpdatedAt() = %v, want unchanged %v (alias payload write should have been skipped)", got2.UpdatedAt(), extensionResourcesUpdatedAt)
			}
			if reported := collectAliases(got2.ReportedAliases()); len(reported) != 0 {
				t.Errorf("ReportedAliases() after follow-up report = %+v, want still empty", reported)
			}
		})

		// UnchangedReportedAliasesSkipPendingAliasPayloadWrite
		// observes the unchanged-payload write skip indirectly
		// through ExtensionResource.UpdatedAt(): ReplaceInventory
		// only touches extension_resources (and therefore only
		// moves its updated_at) when the alias payload write
		// actually happens, which is exactly when the reported
		// payload differs from the stored one. Inventory
		// freshness (Inventory().UpdatedAt(), a separate row
		// entirely) still moves on every report regardless.
		t.Run("UnchangedReportedAliasesSkipPendingAliasPayloadWrite", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			name := domain.ResourceName("nodes/alias-payload-skip")
			alias, _ := domain.NewAlias("gcp", "instance_id", "alias-payload-skip-1")
			t1 := fixedTime.Add(time.Minute)
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      aliasSet(alias),
				ObservedAt:   t1,
				ReceivedAt:   t1,
			}}); err != nil {
				t.Fatalf("first ReplaceInventory: %v", err)
			}

			afterFirst, err := repo.Get(ctx, domain.NewFullResourceName("inv.fleetshift.io", name))
			if err != nil {
				t.Fatalf("Get after first: %v", err)
			}
			firstUpdatedAt := afterFirst.UpdatedAt()

			// Steady-state poll: identical alias set, but a changed
			// inventory field so the report as a whole is not a
			// total no-op.
			t2 := fixedTime.Add(2 * time.Minute)
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Labels:       map[string]string{"zone": "us-east-1"},
				Aliases:      aliasSet(alias),
				ObservedAt:   t2,
				ReceivedAt:   t2,
			}}); err != nil {
				t.Fatalf("second ReplaceInventory: %v", err)
			}

			afterSecond, err := repo.Get(ctx, domain.NewFullResourceName("inv.fleetshift.io", name))
			if err != nil {
				t.Fatalf("Get after second: %v", err)
			}
			if !afterSecond.UpdatedAt().Equal(firstUpdatedAt) {
				t.Errorf("UpdatedAt = %v, want unchanged %v (alias payload write must be skipped when the payload is unchanged)", afterSecond.UpdatedAt(), firstUpdatedAt)
			}
			assertAliasesEqual(t, "ReportedAliases()", afterSecond.ReportedAliases(), []domain.Alias{alias})

			view, err := repo.GetView(ctx, domain.NewFullResourceName("inv.fleetshift.io", name))
			if err != nil {
				t.Fatalf("GetView: %v", err)
			}
			if !view.Resource.Inventory().UpdatedAt().Equal(t2) {
				t.Errorf("Inventory.UpdatedAt = %v, want %v (inventory freshness still moves on every report)", view.Resource.Inventory().UpdatedAt(), t2)
			}
		})

		t.Run("ChangedReportedAliasesUpdatePayloadAndUpdatedAt", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			name := domain.ResourceName("nodes/alias-changed")
			first, _ := domain.NewAlias("gcp", "instance_id", "alias-changed-v1")
			t1 := fixedTime.Add(time.Minute)
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      aliasSet(first),
				ObservedAt:   t1,
				ReceivedAt:   t1,
			}}); err != nil {
				t.Fatalf("first ReplaceInventory: %v", err)
			}

			second, _ := domain.NewAlias("gcp", "instance_id", "alias-changed-v2")
			t2 := fixedTime.Add(2 * time.Minute)
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      aliasSet(second),
				ObservedAt:   t2,
				ReceivedAt:   t2,
			}}); err != nil {
				t.Fatalf("second ReplaceInventory: %v", err)
			}

			got, err := repo.Get(ctx, domain.NewFullResourceName("inv.fleetshift.io", name))
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			assertAliasesEqual(t, "ReportedAliases()", got.ReportedAliases(), []domain.Alias{second})
			if !got.UpdatedAt().Equal(t2) {
				t.Errorf("UpdatedAt = %v, want %v (a changed alias payload must update the row)", got.UpdatedAt(), t2)
			}
		})

		// RepeatedConflictingAliasReportsAreAcceptedNotRejected
		// replaces the old ReportsConflictWhenValueClaimedByAnotherResourceAcrossCalls/
		// ReportsIntraBatchContradictionWithoutSQLError/
		// ReportsConflictsOnlyForActualConflictsInMixedBatch trio:
		// under the old synchronous claims model, two extension
		// resources asserting the same alias value was a rejected
		// (or partially rejected) write. It no longer is -- both
		// reports simply succeed, and each resource's own reported
		// payload reflects exactly what it asserted, with the
		// contradiction itself left for a future reconciliation
		// process to surface.
		t.Run("RepeatedConflictingAliasReportsAreAcceptedNotRejected", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			alias, _ := domain.NewAlias("gcp", "instance_id", "alias-conflicting")
			nameA := domain.ResourceName("nodes/alias-conflict-a")
			nameB := domain.ResourceName("nodes/alias-conflict-b")
			now := fixedTime.Add(time.Minute)
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{
				{
					ResourceType: "inv.fleetshift.io/Node",
					Name:         nameA,
					CandidateUID: domain.NewExtensionResourceUID(),
					Aliases:      aliasSet(alias),
					ObservedAt:   now,
					ReceivedAt:   now,
				},
				{
					ResourceType: "inv.fleetshift.io/Node",
					Name:         nameB,
					CandidateUID: domain.NewExtensionResourceUID(),
					Aliases:      aliasSet(alias),
					ObservedAt:   now,
					ReceivedAt:   now,
				},
			}); err != nil {
				t.Fatalf("ReplaceInventory: %v", err)
			}

			gotA, err := repo.Get(ctx, domain.NewFullResourceName("inv.fleetshift.io", nameA))
			if err != nil {
				t.Fatalf("Get(A): %v", err)
			}
			assertAliasesEqual(t, "A.ReportedAliases()", gotA.ReportedAliases(), []domain.Alias{alias})

			gotB, err := repo.Get(ctx, domain.NewFullResourceName("inv.fleetshift.io", nameB))
			if err != nil {
				t.Fatalf("Get(B): %v", err)
			}
			assertAliasesEqual(t, "B.ReportedAliases()", gotB.ReportedAliases(), []domain.Alias{alias})

			// Repeating the same conflicting report again must also
			// continue to succeed -- this is not a one-time "first
			// conflict is tolerated" grace period.
			later := fixedTime.Add(2 * time.Minute)
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         nameB,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      aliasSet(alias),
				ObservedAt:   later,
				ReceivedAt:   later,
			}}); err != nil {
				t.Fatalf("repeated conflicting ReplaceInventory: %v", err)
			}
		})

		t.Run("RemovingAliasesByFullReplacementStoresEmptyPayload", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			name := domain.ResourceName("nodes/alias-removed")
			alias, _ := domain.NewAlias("gcp", "instance_id", "alias-removed-1")
			t1 := fixedTime.Add(time.Minute)
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				Aliases:      aliasSet(alias),
				ObservedAt:   t1,
				ReceivedAt:   t1,
			}}); err != nil {
				t.Fatalf("first ReplaceInventory: %v", err)
			}

			// The second report's Aliases is empty, not omitted -- a
			// full replacement of the reported payload, mirroring
			// how an empty Labels/Conditions removes everything
			// absent from the report.
			t2 := fixedTime.Add(2 * time.Minute)
			if err := repo.ReplaceInventory(ctx, []domain.InventoryReplacement{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				ObservedAt:   t2,
				ReceivedAt:   t2,
			}}); err != nil {
				t.Fatalf("second ReplaceInventory (no aliases): %v", err)
			}

			got, err := repo.Get(ctx, domain.NewFullResourceName("inv.fleetshift.io", name))
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if reported := collectAliases(got.ReportedAliases()); len(reported) != 0 {
				t.Fatalf("ReportedAliases() = %+v, want empty after removal", reported)
			}
		})

		// ApplyInventoryDeltasUpsertAliasesAppliesAsPendingUpdate
		// covers UpsertAliases, the one alias delta operation this
		// branch implements (see [domain.InventoryDelta]'s doc for
		// why ReplaceAliases/DeleteAliases are deferred): each
		// upserted alias merges into the existing pending payload by
		// (namespace, key), adding new keys and overwriting the
		// value of keys that already exist.
		t.Run("ApplyInventoryDeltasUpsertAliasesAppliesAsPendingUpdate", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			name := domain.ResourceName("nodes/alias-delta-upsert")
			instanceIDv1, _ := domain.NewAlias("gcp", "instance_id", "alias-delta-upsert-instance-v1")
			t1 := fixedTime.Add(time.Minute)
			if err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType:  "inv.fleetshift.io/Node",
				Name:          name,
				CandidateUID:  domain.NewExtensionResourceUID(),
				UpsertAliases: aliasSet(instanceIDv1),
				ObservedAt:    t1,
				ReceivedAt:    t1,
			}}); err != nil {
				t.Fatalf("first ApplyInventoryDeltas: %v", err)
			}

			// A distinct (namespace, key) upserted via a second delta
			// must add to, not replace, the pending payload.
			zone, _ := domain.NewAlias("gcp", "zone", "alias-delta-upsert-zone")
			t2 := fixedTime.Add(2 * time.Minute)
			if err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType:  "inv.fleetshift.io/Node",
				Name:          name,
				CandidateUID:  domain.NewExtensionResourceUID(),
				UpsertAliases: aliasSet(zone),
				ObservedAt:    t2,
				ReceivedAt:    t2,
			}}); err != nil {
				t.Fatalf("second ApplyInventoryDeltas: %v", err)
			}

			// Upserting the same (namespace, key) again with a new
			// value must overwrite it in place, not append a
			// duplicate entry.
			instanceIDv2, _ := domain.NewAlias("gcp", "instance_id", "alias-delta-upsert-instance-v2")
			t3 := fixedTime.Add(3 * time.Minute)
			if err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType:  "inv.fleetshift.io/Node",
				Name:          name,
				CandidateUID:  domain.NewExtensionResourceUID(),
				UpsertAliases: aliasSet(instanceIDv2),
				ObservedAt:    t3,
				ReceivedAt:    t3,
			}}); err != nil {
				t.Fatalf("third ApplyInventoryDeltas: %v", err)
			}

			got, err := repo.Get(ctx, domain.NewFullResourceName("inv.fleetshift.io", name))
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			want := []domain.Alias{instanceIDv2, zone}
			assertAliasesEqual(t, "ReportedAliases()", got.ReportedAliases(), want)
		})

		// UnchangedAliasDeltaSkipsPayloadWrite documents that an
		// UpsertAliases delta whose merged result is identical to the
		// stored payload is a no-op for extension_resources itself:
		// the alias set stays correct, and UpdatedAt does not move.
		// Inventory freshness still moves independently through
		// extension_resource_inventory.
		t.Run("UnchangedAliasDeltaSkipsPayloadWrite", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			name := domain.ResourceName("nodes/alias-delta-unchanged-fp")
			alias, _ := domain.NewAlias("gcp", "instance_id", "alias-delta-unchanged-fp-value")
			t1 := fixedTime.Add(time.Minute)
			if err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType:  "inv.fleetshift.io/Node",
				Name:          name,
				CandidateUID:  domain.NewExtensionResourceUID(),
				UpsertAliases: aliasSet(alias),
				ObservedAt:    t1,
				ReceivedAt:    t1,
			}}); err != nil {
				t.Fatalf("seed ApplyInventoryDeltas: %v", err)
			}

			afterSeed, err := repo.Get(ctx, domain.NewFullResourceName("inv.fleetshift.io", name))
			if err != nil {
				t.Fatalf("Get after seed: %v", err)
			}
			seedUpdatedAt := afterSeed.UpdatedAt()

			// Upsert the exact same alias again -- the merged set is
			// identical, so the alias payload write should be skipped.
			t2 := fixedTime.Add(2 * time.Minute)
			if err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType:  "inv.fleetshift.io/Node",
				Name:          name,
				CandidateUID:  domain.NewExtensionResourceUID(),
				UpsertAliases: aliasSet(alias),
				ObservedAt:    t2,
				ReceivedAt:    t2,
			}}); err != nil {
				t.Fatalf("repeat ApplyInventoryDeltas: %v", err)
			}

			got, err := repo.Get(ctx, domain.NewFullResourceName("inv.fleetshift.io", name))
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			// The alias set itself must still be correct.
			assertAliasesEqual(t, "ReportedAliases()", got.ReportedAliases(), []domain.Alias{alias})
			if !got.UpdatedAt().Equal(seedUpdatedAt) {
				t.Errorf("UpdatedAt = %v, want unchanged %v (unchanged alias delta should skip the payload write)", got.UpdatedAt(), seedUpdatedAt)
			}
		})

		// ApplyInventoryDeltasOmittingAliasesDoesNoAliasWork guards
		// the narrower alias-delta contract described in
		// [domain.InventoryDelta]'s doc: a delta that sets none of
		// UpsertAliases/DeleteAliases/ReplaceAliases must do no alias
		// work at all, not even re-derive/rewrite an unchanged
		// payload -- a heartbeat-style label/condition-only delta
		// is the overwhelmingly common case this guards.
		t.Run("ApplyInventoryDeltasOmittingAliasesDoesNoAliasWork", func(t *testing.T) {
			tx := factory(t)
			defer tx.Rollback()
			repo := tx.ExtensionResources()

			if err := repo.CreateType(ctx, sampleInventoryType("inv.fleetshift.io/Node")); err != nil {
				t.Fatalf("CreateType: %v", err)
			}

			name := domain.ResourceName("nodes/alias-delta-omitted")
			alias, _ := domain.NewAlias("gcp", "instance_id", "alias-delta-omitted-1")
			t1 := fixedTime.Add(time.Minute)
			if err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType:  "inv.fleetshift.io/Node",
				Name:          name,
				CandidateUID:  domain.NewExtensionResourceUID(),
				UpsertAliases: aliasSet(alias),
				ObservedAt:    t1,
				ReceivedAt:    t1,
			}}); err != nil {
				t.Fatalf("seed ApplyInventoryDeltas: %v", err)
			}
			seeded, err := repo.Get(ctx, domain.NewFullResourceName("inv.fleetshift.io", name))
			if err != nil {
				t.Fatalf("Get after seed: %v", err)
			}
			seededUpdatedAt := seeded.UpdatedAt()

			t2 := fixedTime.Add(2 * time.Minute)
			if err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
				ResourceType: "inv.fleetshift.io/Node",
				Name:         name,
				CandidateUID: domain.NewExtensionResourceUID(),
				SetLabels:    map[string]string{"zone": "us-east-1"},
				ObservedAt:   t2,
				ReceivedAt:   t2,
			}}); err != nil {
				t.Fatalf("label-only ApplyInventoryDeltas: %v", err)
			}

			got, err := repo.Get(ctx, domain.NewFullResourceName("inv.fleetshift.io", name))
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			assertAliasesEqual(t, "ReportedAliases()", got.ReportedAliases(), []domain.Alias{alias})
			if !got.UpdatedAt().Equal(seededUpdatedAt) {
				t.Errorf("UpdatedAt = %v, want unchanged %v (no alias fields set, so no alias work)", got.UpdatedAt(), seededUpdatedAt)
			}
		})
	})
}

func assertEqual[T comparable](t *testing.T, field string, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %v, want %v", field, got, want)
	}
}

func collectAliases(set domain.AliasSet) []domain.Alias {
	return set.Slice()
}

// assertAliasesEqual asserts that got and want contain the same
// [domain.Alias] values once canonicalized by [domain.AliasSet].
func assertAliasesEqual(t *testing.T, field string, got domain.AliasSet, want []domain.Alias) {
	t.Helper()
	gotAliases := collectAliases(got)
	wantAliases := collectAliases(domain.NewAliasSet(want))
	if len(gotAliases) != len(wantAliases) {
		t.Fatalf("%s = %+v, want %+v", field, gotAliases, wantAliases)
	}
	for i := range wantAliases {
		if gotAliases[i] != wantAliases[i] {
			t.Fatalf("%s = %+v, want %+v", field, gotAliases, wantAliases)
		}
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
