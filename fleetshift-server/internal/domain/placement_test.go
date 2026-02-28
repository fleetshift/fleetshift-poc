package domain_test

import (
	"context"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

var testPool = []domain.TargetInfo{
	{ID: "t1", Name: "cluster-a", Labels: map[string]string{"env": "prod", "region": "us-east"}},
	{ID: "t2", Name: "cluster-b", Labels: map[string]string{"env": "staging", "region": "us-west"}},
	{ID: "t3", Name: "cluster-c", Labels: map[string]string{"env": "prod", "region": "eu-west"}},
}

func TestStaticPlacement_ResolvesExactTargets(t *testing.T) {
	s := &domain.StaticPlacement{Targets: []domain.TargetID{"t1", "t3"}}
	got, err := s.Resolve(context.Background(), testPool)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 targets, got %d", len(got))
	}
	if got[0].ID != "t1" || got[1].ID != "t3" {
		t.Fatalf("expected [t1, t3], got [%s, %s]", got[0].ID, got[1].ID)
	}
}

func TestStaticPlacement_PreservesOrder(t *testing.T) {
	s := &domain.StaticPlacement{Targets: []domain.TargetID{"t3", "t1"}}
	got, err := s.Resolve(context.Background(), testPool)
	if err != nil {
		t.Fatal(err)
	}
	if got[0].ID != "t3" || got[1].ID != "t1" {
		t.Fatalf("expected [t3, t1], got [%s, %s]", got[0].ID, got[1].ID)
	}
}

func TestStaticPlacement_UnknownTargetReturnsError(t *testing.T) {
	s := &domain.StaticPlacement{Targets: []domain.TargetID{"t1", "missing"}}
	_, err := s.Resolve(context.Background(), testPool)
	if err == nil {
		t.Fatal("expected error for unknown target")
	}
}

func TestAllPlacement_ReturnsEntirePool(t *testing.T) {
	s := &domain.AllPlacement{}
	got, err := s.Resolve(context.Background(), testPool)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(testPool) {
		t.Fatalf("expected %d targets, got %d", len(testPool), len(got))
	}
}

func TestAllPlacement_EmptyPool(t *testing.T) {
	s := &domain.AllPlacement{}
	got, err := s.Resolve(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("expected 0 targets, got %d", len(got))
	}
}

func TestSelectorPlacement_MatchesSingleLabel(t *testing.T) {
	s := &domain.SelectorPlacement{
		Selector: domain.TargetSelector{MatchLabels: map[string]string{"env": "prod"}},
	}
	got, err := s.Resolve(context.Background(), testPool)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 targets, got %d", len(got))
	}
}

func TestSelectorPlacement_MatchesMultipleLabels(t *testing.T) {
	s := &domain.SelectorPlacement{
		Selector: domain.TargetSelector{MatchLabels: map[string]string{"env": "prod", "region": "eu-west"}},
	}
	got, err := s.Resolve(context.Background(), testPool)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 target, got %d", len(got))
	}
	if got[0].ID != "t3" {
		t.Fatalf("expected t3, got %s", got[0].ID)
	}
}

func TestSelectorPlacement_NoMatches(t *testing.T) {
	s := &domain.SelectorPlacement{
		Selector: domain.TargetSelector{MatchLabels: map[string]string{"env": "dev"}},
	}
	got, err := s.Resolve(context.Background(), testPool)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("expected 0 targets, got %d", len(got))
	}
}

func TestSelectorPlacement_EmptySelector(t *testing.T) {
	s := &domain.SelectorPlacement{
		Selector: domain.TargetSelector{MatchLabels: map[string]string{}},
	}
	got, err := s.Resolve(context.Background(), testPool)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(testPool) {
		t.Fatalf("empty selector should match all; expected %d targets, got %d", len(testPool), len(got))
	}
}
