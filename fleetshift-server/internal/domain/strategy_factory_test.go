package domain_test

import (
	"encoding/json"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestStrategyFactory_ManifestStrategy_Inline(t *testing.T) {
	store, _ := setupStore(t)
	factory := domain.StrategyFactory{Store: store}
	spec := domain.ManifestStrategySpec{
		Type:      domain.ManifestStrategyInline,
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
	}

	s, err := factory.ManifestStrategy(spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.(*domain.InlineManifestStrategy); !ok {
		t.Fatalf("expected *InlineManifestStrategy, got %T", s)
	}
}

func TestStrategyFactory_ManifestStrategy_ManagedResource(t *testing.T) {
	store, _ := setupStore(t)
	factory := domain.StrategyFactory{Store: store}
	spec := domain.ManifestStrategySpec{
		Type:      domain.ManifestStrategyManagedResource,
		IntentRef: domain.IntentRef{ResourceType: "clusters", Name: "prod", Version: 1},
	}

	s, err := factory.ManifestStrategy(spec)
	if err != nil {
		t.Fatal(err)
	}
	mrs, ok := s.(*domain.ManagedResourceManifestStrategy)
	if !ok {
		t.Fatalf("expected *ManagedResourceManifestStrategy, got %T", s)
	}
	if mrs.Ref.ResourceType != "clusters" {
		t.Errorf("Ref.ResourceType = %q, want %q", mrs.Ref.ResourceType, "clusters")
	}
}

func TestStrategyFactory_ManifestStrategy_UnsupportedType(t *testing.T) {
	factory := domain.StrategyFactory{}
	spec := domain.ManifestStrategySpec{Type: "bogus"}

	_, err := factory.ManifestStrategy(spec)
	if err == nil {
		t.Fatal("expected error for unsupported type")
	}
}

func TestStrategyFactory_PlacementStrategy_Static(t *testing.T) {
	factory := domain.StrategyFactory{}
	spec := domain.PlacementStrategySpec{
		Type:    domain.PlacementStrategyStatic,
		Targets: []domain.TargetID{"t1"},
	}

	s, err := factory.PlacementStrategy(spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.(*domain.StaticPlacement); !ok {
		t.Fatalf("expected *StaticPlacement, got %T", s)
	}
}

func TestStrategyFactory_PlacementStrategy_All(t *testing.T) {
	factory := domain.StrategyFactory{}
	spec := domain.PlacementStrategySpec{Type: domain.PlacementStrategyAll}

	s, err := factory.PlacementStrategy(spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.(*domain.AllPlacement); !ok {
		t.Fatalf("expected *AllPlacement, got %T", s)
	}
}

func TestStrategyFactory_PlacementStrategy_Selector(t *testing.T) {
	factory := domain.StrategyFactory{}
	spec := domain.PlacementStrategySpec{
		Type:           domain.PlacementStrategySelector,
		TargetSelector: &domain.TargetSelector{MatchLabels: map[string]string{"env": "prod"}},
	}

	s, err := factory.PlacementStrategy(spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.(*domain.SelectorPlacement); !ok {
		t.Fatalf("expected *SelectorPlacement, got %T", s)
	}
}

func TestStrategyFactory_RolloutStrategy_NilSpec(t *testing.T) {
	factory := domain.StrategyFactory{}
	s := factory.RolloutStrategy(nil)
	if _, ok := s.(*domain.ImmediateRollout); !ok {
		t.Fatalf("expected *ImmediateRollout, got %T", s)
	}
}

func TestStrategyFactory_RolloutStrategy_Immediate(t *testing.T) {
	factory := domain.StrategyFactory{}
	s := factory.RolloutStrategy(&domain.RolloutStrategySpec{Type: domain.RolloutStrategyImmediate})
	if _, ok := s.(*domain.ImmediateRollout); !ok {
		t.Fatalf("expected *ImmediateRollout, got %T", s)
	}
}
