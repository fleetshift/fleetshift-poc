package domain_test

import (
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestToPlacementTarget_OmitsProperties(t *testing.T) {
	target := domain.TargetInfo{
		ID:         "t1",
		Name:       "cluster-a",
		Labels:     map[string]string{"env": "prod"},
		Properties: map[string]string{"region": "us-east"},
	}
	got := domain.ToPlacementTarget(target)
	if got.ID != target.ID || got.Name != target.Name {
		t.Errorf("ID or Name changed: got %+v", got)
	}
	if got.Labels["env"] != "prod" {
		t.Errorf("Labels[env] = %q, want prod", got.Labels["env"])
	}
	// PlacementTarget has no Properties field; conversion omits them by type.
}

func TestPlacementTargets_PreservesOrderAndLength(t *testing.T) {
	pool := []domain.TargetInfo{
		{ID: "a", Name: "n1", Labels: map[string]string{"x": "1"}},
		{ID: "b", Name: "n2", Labels: map[string]string{"y": "2"}},
	}
	got := domain.PlacementTargets(pool)
	if len(got) != 2 {
		t.Fatalf("len(got) = %d, want 2", len(got))
	}
	if got[0].ID != "a" || got[1].ID != "b" {
		t.Errorf("order or IDs wrong: got [%s, %s]", got[0].ID, got[1].ID)
	}
}

func TestResolvedTargetInfos_LookupAndOrder(t *testing.T) {
	pool := []domain.TargetInfo{
		{ID: "t1", Name: "c1", Labels: map[string]string{"env": "prod"}, Properties: map[string]string{"region": "us"}},
		{ID: "t2", Name: "c2", Labels: map[string]string{"env": "staging"}, Properties: map[string]string{"region": "eu"}},
		{ID: "t3", Name: "c3", Labels: map[string]string{"env": "prod"}, Properties: nil},
	}
	resolved := []domain.PlacementTarget{
		{ID: "t3", Name: "c3", Labels: map[string]string{"env": "prod"}},
		{ID: "t1", Name: "c1", Labels: map[string]string{"env": "prod"}},
	}
	got := domain.ResolvedTargetInfos(resolved, pool)
	if len(got) != 2 {
		t.Fatalf("len(got) = %d, want 2", len(got))
	}
	if got[0].ID != "t3" || got[1].ID != "t1" {
		t.Errorf("order wrong: got [%s, %s], want [t3, t1]", got[0].ID, got[1].ID)
	}
	if got[1].Properties == nil || got[1].Properties["region"] != "us" {
		t.Errorf("full TargetInfo from pool: got[1].Properties = %v, want map with region=us", got[1].Properties)
	}
}

func TestResolvedTargetInfos_OmitsMissingFromPool(t *testing.T) {
	pool := []domain.TargetInfo{
		{ID: "t1", Name: "c1", Labels: nil},
	}
	resolved := []domain.PlacementTarget{
		{ID: "t1", Name: "c1", Labels: nil},
		{ID: "missing", Name: "m", Labels: nil},
	}
	got := domain.ResolvedTargetInfos(resolved, pool)
	if len(got) != 1 {
		t.Fatalf("len(got) = %d, want 1 (missing ID omitted)", len(got))
	}
	if got[0].ID != "t1" {
		t.Errorf("got[0].ID = %s, want t1", got[0].ID)
	}
}
