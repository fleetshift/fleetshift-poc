// Package managedresourcerepotest provides contract tests for
// [domain.ManagedResourceRepository] implementations.
package managedresourcerepotest

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
// because managed resources reference fulfillments (foreign key).
type Factory func(t *testing.T) domain.Tx

// Run exercises the [domain.ManagedResourceRepository] contract.
func Run(t *testing.T, factory Factory) {
	t.Run("Types", func(t *testing.T) { runTypeTests(t, factory) })
	t.Run("Intents", func(t *testing.T) { runIntentTests(t, factory) })
	t.Run("Instances", func(t *testing.T) { runInstanceTests(t, factory) })
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

func runTypeTests(t *testing.T, factory Factory) {
	fixedTime := time.Date(2026, 4, 28, 12, 0, 0, 0, time.UTC)
	ctx := context.Background()

	sampleTypeDef := func(rt domain.ResourceType) domain.ManagedResourceTypeDef {
		svc := rt.ServiceName()
		if svc == "" {
			svc = "test.fleetshift.io"
		}
		typeName := rt.TypeName()
		if typeName == "" {
			typeName = string(rt)
		}
		return domain.ManagedResourceTypeDef{
			ResourceType:   rt,
			Relation:       domain.NewRegisteredSelfTarget("addon-"+domain.TargetID(typeName), domain.ManifestType("api.test."+strings.ToLower(typeName))),
			APIServiceName: svc,
			APIVersion:     "v1",
			CollectionID:   domain.CollectionID(strings.ToLower(typeName) + "s"),
			Signature: domain.Signature{
				Signer:         domain.FederatedIdentity{Subject: "addon-svc", Issuer: "https://issuer.test"},
				ContentHash:    []byte("hash"),
				SignatureBytes: []byte("sig"),
			},
			CreatedAt: fixedTime,
			UpdatedAt: fixedTime,
		}
	}

	t.Run("CreateAndGet", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ManagedResources()

		def := sampleTypeDef("test.fleetshift.io/Cluster")

		if err := repo.CreateType(ctx, def); err != nil {
			t.Fatalf("CreateType: %v", err)
		}

		got, err := repo.GetType(ctx, "test.fleetshift.io/Cluster")
		if err != nil {
			t.Fatalf("GetType: %v", err)
		}
		if got.ResourceType != "test.fleetshift.io/Cluster" {
			t.Errorf("ResourceType = %q, want %q", got.ResourceType, "test.fleetshift.io/Cluster")
		}
		rst, ok := got.Relation.(domain.RegisteredSelfTarget)
		if !ok {
			t.Fatalf("Relation type = %T, want RegisteredSelfTarget", got.Relation)
		}
		if rst.AddonTarget() != "addon-Cluster" {
			t.Errorf("AddonTarget() = %q, want %q", rst.AddonTarget(), "addon-Cluster")
		}
		if !got.CreatedAt.Equal(fixedTime) {
			t.Errorf("CreatedAt = %v, want %v", got.CreatedAt, fixedTime)
		}
	})

	t.Run("CreateDuplicate", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ManagedResources()

		def := sampleTypeDef("vms")
		if err := repo.CreateType(ctx, def); err != nil {
			t.Fatalf("first CreateType: %v", err)
		}
		err := repo.CreateType(ctx, def)
		if !errors.Is(err, domain.ErrAlreadyExists) {
			t.Fatalf("second CreateType: got %v, want ErrAlreadyExists", err)
		}
	})

	t.Run("GetNotFound", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()

		_, err := tx.ManagedResources().GetType(ctx, "nonexistent")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("GetType: got %v, want ErrNotFound", err)
		}
	})

	t.Run("ListTypes", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ManagedResources()

		for _, rt := range []domain.ResourceType{"aaa", "bbb"} {
			if err := repo.CreateType(ctx, sampleTypeDef(rt)); err != nil {
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

	t.Run("CreateAndGetWithAPIIdentity", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ManagedResources()

		def := sampleTypeDef("kind.fleetshift.io/Cluster")
		def.APIServiceName = "kind.fleetshift.io"
		def.APIVersion = "v1"
		def.CollectionID = "clusters"

		if err := repo.CreateType(ctx, def); err != nil {
			t.Fatalf("CreateType: %v", err)
		}

		got, err := repo.GetType(ctx, "kind.fleetshift.io/Cluster")
		if err != nil {
			t.Fatalf("GetType: %v", err)
		}
		if got.APIServiceName != "kind.fleetshift.io" {
			t.Errorf("APIServiceName = %q, want %q", got.APIServiceName, "kind.fleetshift.io")
		}
		if got.APIVersion != "v1" {
			t.Errorf("APIVersion = %q, want %q", got.APIVersion, "v1")
		}
		if got.CollectionID != "clusters" {
			t.Errorf("CollectionID = %q, want %q", got.CollectionID, "clusters")
		}
	})

	t.Run("DeleteType", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ManagedResources()

		if err := repo.CreateType(ctx, sampleTypeDef("del-me")); err != nil {
			t.Fatalf("CreateType: %v", err)
		}
		if err := repo.DeleteType(ctx, "del-me"); err != nil {
			t.Fatalf("DeleteType: %v", err)
		}
		_, err := repo.GetType(ctx, "del-me")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("GetType after delete: got %v, want ErrNotFound", err)
		}
	})

	t.Run("DeleteTypeNotFound", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()

		err := tx.ManagedResources().DeleteType(ctx, "ghost")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("DeleteType: got %v, want ErrNotFound", err)
		}
	})
}

func runIntentTests(t *testing.T, factory Factory) {
	fixedTime := time.Date(2026, 4, 28, 12, 0, 0, 0, time.UTC)
	ctx := context.Background()

	t.Run("DrainedOnCreateAndGet", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ManagedResources()

		fID := domain.FulfillmentID("f-intent-drain")
		seedFulfillment(t, tx, fID, fixedTime)

		mr := domain.ManagedResourceFromSnapshot(domain.ManagedResourceSnapshot{
			ResourceType:  "test.fleetshift.io/Cluster",
			Name:          "prod-1",
			UID:           domain.NewManagedResourceUID(),
			FulfillmentID: fID,
			CreatedAt:     fixedTime,
			UpdatedAt:     fixedTime,
		})
		mr.RecordIntent(json.RawMessage(`{"provider":"rosa"}`), fixedTime)

		if err := repo.CreateInstance(ctx, mr); err != nil {
			t.Fatalf("CreateInstance: %v", err)
		}

		got, err := repo.GetIntent(ctx, "test.fleetshift.io/Cluster", "prod-1", 1)
		if err != nil {
			t.Fatalf("GetIntent: %v", err)
		}
		if got.Version != 1 {
			t.Errorf("Version = %d, want 1", got.Version)
		}
		if string(got.Spec) != `{"provider":"rosa"}` {
			t.Errorf("Spec = %s, want {\"provider\":\"rosa\"}", got.Spec)
		}
		if !got.CreatedAt.Equal(fixedTime) {
			t.Errorf("CreatedAt = %v, want %v", got.CreatedAt, fixedTime)
		}
	})

	t.Run("GetNotFound", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()

		_, err := tx.ManagedResources().GetIntent(ctx, "test.fleetshift.io/Cluster", "nope", 99)
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("got %v, want ErrNotFound", err)
		}
	})

	t.Run("DeleteIntents", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ManagedResources()

		fID := domain.FulfillmentID("f-intent-delete")
		seedFulfillment(t, tx, fID, fixedTime)

		mr := domain.ManagedResourceFromSnapshot(domain.ManagedResourceSnapshot{
			ResourceType:  "test.fleetshift.io/Cluster",
			Name:          "prod-delete",
			UID:           domain.NewManagedResourceUID(),
			FulfillmentID: fID,
			CreatedAt:     fixedTime,
			UpdatedAt:     fixedTime,
		})
		mr.RecordIntent(json.RawMessage(`{"provider":"rosa"}`), fixedTime)

		if err := repo.CreateInstance(ctx, mr); err != nil {
			t.Fatalf("CreateInstance: %v", err)
		}
		if err := repo.DeleteInstance(ctx, "test.fleetshift.io/Cluster", "prod-delete"); err != nil {
			t.Fatalf("DeleteInstance: %v", err)
		}
		if err := repo.DeleteIntents(ctx, "test.fleetshift.io/Cluster", "prod-delete"); err != nil {
			t.Fatalf("DeleteIntents: %v", err)
		}

		_, err := repo.GetIntent(ctx, "test.fleetshift.io/Cluster", "prod-delete", 1)
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("GetIntent after DeleteIntents: got %v, want ErrNotFound", err)
		}

		if err := repo.DeleteIntents(ctx, "test.fleetshift.io/Cluster", "prod-delete"); err != nil {
			t.Fatalf("DeleteIntents second call: %v", err)
		}
	})
}

func runInstanceTests(t *testing.T, factory Factory) {
	fixedTime := time.Date(2026, 4, 28, 12, 0, 0, 0, time.UTC)
	ctx := context.Background()

	// newMR constructs a ManagedResource with a single recorded intent,
	// ready for CreateInstance to drain.
	newMR := func(rt domain.ResourceType, name domain.ResourceName, fID domain.FulfillmentID) *domain.ManagedResource {
		mr := domain.ManagedResourceFromSnapshot(domain.ManagedResourceSnapshot{
			ResourceType:  rt,
			Name:          name,
			UID:           domain.NewManagedResourceUID(),
			FulfillmentID: fID,
			CreatedAt:     fixedTime,
			UpdatedAt:     fixedTime,
		})
		mr.RecordIntent(json.RawMessage(`{"provider":"rosa"}`), fixedTime)
		return mr
	}

	t.Run("CreateAndGet", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ManagedResources()

		fID := domain.FulfillmentID("f-mr-create")
		seedFulfillment(t, tx, fID, fixedTime)

		mr := newMR("test.fleetshift.io/Cluster", "prod-1", fID)
		if err := repo.CreateInstance(ctx, mr); err != nil {
			t.Fatalf("CreateInstance: %v", err)
		}

		got, err := repo.GetInstance(ctx, "test.fleetshift.io/Cluster", "prod-1")
		if err != nil {
			t.Fatalf("GetInstance: %v", err)
		}
		if got.UID().IsZero() {
			t.Error("UID is zero, want non-zero")
		}
		if got.FulfillmentID() != fID {
			t.Errorf("FulfillmentID = %q, want %q", got.FulfillmentID(), fID)
		}
		if got.CurrentVersion() != 1 {
			t.Errorf("CurrentVersion = %d, want 1", got.CurrentVersion())
		}
	})

	t.Run("CreateDuplicate", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ManagedResources()

		fID := domain.FulfillmentID("f-mr-dup")
		seedFulfillment(t, tx, fID, fixedTime)

		mr := newMR("test.fleetshift.io/Cluster", "dup-res", fID)
		if err := repo.CreateInstance(ctx, mr); err != nil {
			t.Fatalf("first: %v", err)
		}
		mr2 := domain.ManagedResourceFromSnapshot(domain.ManagedResourceSnapshot{
			ResourceType:  "test.fleetshift.io/Cluster",
			Name:          "dup-res",
			UID:           domain.NewManagedResourceUID(),
			FulfillmentID: fID,
			CreatedAt:     fixedTime,
			UpdatedAt:     fixedTime,
		})
		mr2.RecordIntent(json.RawMessage(`{}`), fixedTime)
		err := repo.CreateInstance(ctx, mr2)
		if !errors.Is(err, domain.ErrAlreadyExists) {
			t.Fatalf("second: got %v, want ErrAlreadyExists", err)
		}
	})

	t.Run("GetNotFound", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()

		_, err := tx.ManagedResources().GetInstance(ctx, "test.fleetshift.io/Cluster", "ghost")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("got %v, want ErrNotFound", err)
		}
	})

	t.Run("GetView", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ManagedResources()

		fID := domain.FulfillmentID("f-mr-view")
		seedFulfillment(t, tx, fID, fixedTime)

		mr := newMR("test.fleetshift.io/Cluster", "view-res", fID)
		if err := repo.CreateInstance(ctx, mr); err != nil {
			t.Fatalf("CreateInstance: %v", err)
		}

		v, err := repo.GetView(ctx, "test.fleetshift.io/Cluster", "view-res")
		if err != nil {
			t.Fatalf("GetView: %v", err)
		}
		if v.ManagedResource.Name() != "view-res" {
			t.Errorf("Name = %q, want %q", v.ManagedResource.Name(), "view-res")
		}
		if string(v.Intent.Spec) != `{"provider":"rosa"}` {
			t.Errorf("Intent.Spec = %s", v.Intent.Spec)
		}
		if v.Fulfillment.ID() != fID {
			t.Errorf("Fulfillment.ID = %q, want %q", v.Fulfillment.ID(), fID)
		}
		if v.Fulfillment.State() != domain.FulfillmentStateCreating {
			t.Errorf("Fulfillment.State = %q, want %q", v.Fulfillment.State(), domain.FulfillmentStateCreating)
		}
	})

	t.Run("ListViewsByType", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ManagedResources()

		for i, name := range []domain.ResourceName{"a-res", "b-res"} {
			fID := domain.FulfillmentID(fmt.Sprintf("f-list-%d", i))
			seedFulfillment(t, tx, fID, fixedTime)
			mr := newMR("test.fleetshift.io/Cluster", name, fID)
			if err := repo.CreateInstance(ctx, mr); err != nil {
				t.Fatalf("CreateInstance %s: %v", name, err)
			}
		}

		views, err := repo.ListViewsByType(ctx, "test.fleetshift.io/Cluster")
		if err != nil {
			t.Fatalf("ListViewsByType: %v", err)
		}
		if len(views) != 2 {
			t.Fatalf("len = %d, want 2", len(views))
		}
		if views[0].ManagedResource.Name() != "a-res" {
			t.Errorf("first name = %q, want %q", views[0].ManagedResource.Name(), "a-res")
		}
	})

	t.Run("Delete", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()
		repo := tx.ManagedResources()

		fID := domain.FulfillmentID("f-mr-del")
		seedFulfillment(t, tx, fID, fixedTime)

		mr := newMR("test.fleetshift.io/Cluster", "del-res", fID)
		if err := repo.CreateInstance(ctx, mr); err != nil {
			t.Fatalf("CreateInstance: %v", err)
		}
		if err := repo.DeleteInstance(ctx, "test.fleetshift.io/Cluster", "del-res"); err != nil {
			t.Fatalf("DeleteInstance: %v", err)
		}
		_, err := repo.GetInstance(ctx, "test.fleetshift.io/Cluster", "del-res")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("GetInstance after delete: got %v, want ErrNotFound", err)
		}
	})

	t.Run("DeleteNotFound", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()

		err := tx.ManagedResources().DeleteInstance(ctx, "test.fleetshift.io/Cluster", "ghost")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("got %v, want ErrNotFound", err)
		}
	})
}
