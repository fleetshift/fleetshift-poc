// Package edgerepotest provides contract tests for
// [domain.EdgeRepository] implementations.
package edgerepotest

import (
	"context"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Factory creates a fresh [domain.EdgeRepository] for each test.
type Factory func(t *testing.T) domain.EdgeRepository

// Run exercises the [domain.EdgeRepository] contract.
func Run(t *testing.T, factory Factory) {
	targetID := domain.TargetID("test-target")

	edges := []domain.InventoryEdge{
		{EdgeType: "ownedBy", SourceUID: "pod-1", DestUID: "rs-1", SourceKind: "Pod", DestKind: "ReplicaSet"},
		{EdgeType: "ownedBy", SourceUID: "rs-1", DestUID: "deploy-1", SourceKind: "ReplicaSet", DestKind: "Deployment"},
		{EdgeType: "runsOn", SourceUID: "pod-1", DestUID: "node-1", SourceKind: "Pod", DestKind: "Node"},
		{EdgeType: "selects", SourceUID: "svc-1", DestUID: "pod-1", SourceKind: "Service", DestKind: "Pod"},
	}

	t.Run("ListBySourceUID", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		if err := repo.CreateOrUpdate(ctx, targetID, edges); err != nil {
			t.Fatalf("CreateOrUpdate: %v", err)
		}

		got, err := repo.ListBySourceUID(ctx, targetID, "pod-1")
		if err != nil {
			t.Fatalf("ListBySourceUID: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("len = %d, want 2 (ownedBy + runsOn)", len(got))
		}

		types := map[string]bool{}
		for _, e := range got {
			types[e.EdgeType] = true
			if e.SourceUID != "pod-1" {
				t.Errorf("SourceUID = %q, want pod-1", e.SourceUID)
			}
		}
		if !types["ownedBy"] {
			t.Error("missing ownedBy edge")
		}
		if !types["runsOn"] {
			t.Error("missing runsOn edge")
		}
	})

	t.Run("ListBySourceUID_Empty", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		got, err := repo.ListBySourceUID(ctx, targetID, "nonexistent")
		if err != nil {
			t.Fatalf("ListBySourceUID: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("len = %d, want 0", len(got))
		}
	})

	t.Run("ListByDestUID", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		if err := repo.CreateOrUpdate(ctx, targetID, edges); err != nil {
			t.Fatalf("CreateOrUpdate: %v", err)
		}

		got, err := repo.ListByDestUID(ctx, targetID, "pod-1")
		if err != nil {
			t.Fatalf("ListByDestUID: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("len = %d, want 1 (selects from svc-1)", len(got))
		}
		if got[0].EdgeType != "selects" {
			t.Errorf("EdgeType = %q, want selects", got[0].EdgeType)
		}
		if got[0].SourceUID != "svc-1" {
			t.Errorf("SourceUID = %q, want svc-1", got[0].SourceUID)
		}
	})

	t.Run("ListByDestUID_Multiple", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		if err := repo.CreateOrUpdate(ctx, targetID, edges); err != nil {
			t.Fatalf("CreateOrUpdate: %v", err)
		}

		got, err := repo.ListByDestUID(ctx, targetID, "rs-1")
		if err != nil {
			t.Fatalf("ListByDestUID: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("len = %d, want 1 (ownedBy from pod-1)", len(got))
		}
		if got[0].SourceUID != "pod-1" {
			t.Errorf("SourceUID = %q, want pod-1", got[0].SourceUID)
		}
	})

	t.Run("ListBySourceUID_WrongTarget", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		if err := repo.CreateOrUpdate(ctx, targetID, edges); err != nil {
			t.Fatalf("CreateOrUpdate: %v", err)
		}

		got, err := repo.ListBySourceUID(ctx, "other-target", "pod-1")
		if err != nil {
			t.Fatalf("ListBySourceUID: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("len = %d, want 0 (wrong target)", len(got))
		}
	})

	t.Run("ListByDestUID_AfterDelete", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		if err := repo.CreateOrUpdate(ctx, targetID, edges); err != nil {
			t.Fatalf("CreateOrUpdate: %v", err)
		}

		if err := repo.Delete(ctx, targetID, []domain.InventoryEdge{edges[3]}); err != nil {
			t.Fatalf("Delete: %v", err)
		}

		got, err := repo.ListByDestUID(ctx, targetID, "pod-1")
		if err != nil {
			t.Fatalf("ListByDestUID: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("len = %d, want 0 (deleted)", len(got))
		}
	})
}
