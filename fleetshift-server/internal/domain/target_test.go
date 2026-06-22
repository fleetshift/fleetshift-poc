package domain_test

import (
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestToPlacementTarget_OmitsProperties(t *testing.T) {
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:         "t1",
		Name:       "cluster-a",
		Labels:     map[string]string{"env": "prod"},
		Properties: map[string]string{"region": "us-east"},
	})
	got := domain.ToPlacementTarget(target)
	if got.ID != target.ID() || got.Name != target.Name() {
		t.Errorf("ID or Name changed: got %+v", got)
	}
	if got.Labels["env"] != "prod" {
		t.Errorf("Labels[env] = %q, want prod", got.Labels["env"])
	}
	// PlacementTarget has no Properties field; conversion omits them by type.
}

func TestToPlacementTarget_PropagatesAcceptedManifestTypes(t *testing.T) {
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:                    "t1",
		Name:                  "cluster-a",
		AcceptedManifestTypes: []domain.ManifestType{"api.kind.cluster", "kubernetes"},
	})
	got := domain.ToPlacementTarget(target)
	if len(got.AcceptedManifestTypes) != 2 {
		t.Fatalf("len(AcceptedManifestTypes) = %d, want 2", len(got.AcceptedManifestTypes))
	}
	if got.AcceptedManifestTypes[0] != "api.kind.cluster" || got.AcceptedManifestTypes[1] != "kubernetes" {
		t.Errorf("AcceptedManifestTypes = %v, want [api.kind.cluster, kubernetes]", got.AcceptedManifestTypes)
	}

	// Verify it's a copy, not a shared slice.
	got.AcceptedManifestTypes[0] = "mutated"
	if target.AcceptedManifestTypes()[0] == "mutated" {
		t.Error("AcceptedManifestTypes should be copied, not shared")
	}
}

func TestPlacementTargets_PreservesOrderAndLength(t *testing.T) {
	pool := []domain.TargetInfo{
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "a", Name: "n1", State: domain.TargetStateReady, Labels: map[string]string{"x": "1"}}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "b", Name: "n2", State: domain.TargetStateReady, Labels: map[string]string{"y": "2"}}),
	}
	got := domain.PlacementTargets(pool)
	if len(got) != 2 {
		t.Fatalf("len(got) = %d, want 2", len(got))
	}
	if got[0].ID != "a" || got[1].ID != "b" {
		t.Errorf("order or IDs wrong: got [%s, %s]", got[0].ID, got[1].ID)
	}
}

func TestPlacementTargets_FiltersNonReadyTargets(t *testing.T) {
	pool := []domain.TargetInfo{
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "a", Name: "n1", State: domain.TargetStateReady}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "b", Name: "n2", State: domain.TargetStateInitializing}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "c", Name: "n3", State: domain.TargetStateDraining}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "d", Name: "n4", State: domain.TargetStateTerminated}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "e", Name: "n5", State: domain.TargetStateDiscovered}),
	}
	got := domain.PlacementTargets(pool)
	if len(got) != 1 {
		t.Fatalf("len(got) = %d, want 1 (only ready targets)", len(got))
	}
	if got[0].ID != "a" {
		t.Errorf("got[0].ID = %s, want a", got[0].ID)
	}
}

func TestPlacementTargets_EmptyStateIsEligible(t *testing.T) {
	pool := []domain.TargetInfo{
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "a", Name: "n1"}),
	}
	got := domain.PlacementTargets(pool)
	if len(got) != 1 {
		t.Fatalf("len(got) = %d, want 1 (empty state treated as ready)", len(got))
	}
}

func TestResolvedTargetInfos_LookupAndOrder(t *testing.T) {
	pool := []domain.TargetInfo{
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "c1", Labels: map[string]string{"env": "prod"}, Properties: map[string]string{"region": "us"}}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t2", Name: "c2", Labels: map[string]string{"env": "staging"}, Properties: map[string]string{"region": "eu"}}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t3", Name: "c3", Labels: map[string]string{"env": "prod"}, Properties: nil}),
	}
	resolved := []domain.PlacementTarget{
		{ID: "t3", Name: "c3", Labels: map[string]string{"env": "prod"}},
		{ID: "t1", Name: "c1", Labels: map[string]string{"env": "prod"}},
	}
	got := domain.ResolvedTargetInfos(resolved, pool)
	if len(got) != 2 {
		t.Fatalf("len(got) = %d, want 2", len(got))
	}
	if got[0].ID() != "t3" || got[1].ID() != "t1" {
		t.Errorf("order wrong: got [%s, %s], want [t3, t1]", got[0].ID(), got[1].ID())
	}
	if got[1].Properties() == nil || got[1].Properties()["region"] != "us" {
		t.Errorf("full TargetInfo from pool: got[1].Properties = %v, want map with region=us", got[1].Properties())
	}
}

func TestResolvedTargetInfos_OmitsMissingFromPool(t *testing.T) {
	pool := []domain.TargetInfo{
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "c1", Labels: nil}),
	}
	resolved := []domain.PlacementTarget{
		{ID: "t1", Name: "c1", Labels: nil},
		{ID: "missing", Name: "m", Labels: nil},
	}
	got := domain.ResolvedTargetInfos(resolved, pool)
	if len(got) != 1 {
		t.Fatalf("len(got) = %d, want 1 (missing ID omitted)", len(got))
	}
	if got[0].ID() != "t1" {
		t.Errorf("got[0].ID = %s, want t1", got[0].ID())
	}
}
