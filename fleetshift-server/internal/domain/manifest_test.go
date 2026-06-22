package domain_test

import (
	"encoding/json"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestFilterAcceptedManifests_UnconstrainedTarget(t *testing.T) {
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "unconstrained"})
	manifests := []domain.Manifest{
		{ManifestType: "api.kind.cluster", Raw: json.RawMessage(`{}`)},
		{ManifestType: "kubernetes", Raw: json.RawMessage(`{}`)},
	}
	got := domain.FilterAcceptedManifests(target, manifests)
	if len(got) != 2 {
		t.Fatalf("unconstrained target should pass all manifests; got %d, want 2", len(got))
	}
}

func TestFilterAcceptedManifests_FiltersToAcceptedTypes(t *testing.T) {
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:                    "t1",
		Name:                  "kind-only",
		AcceptedManifestTypes: []domain.ManifestType{"api.kind.cluster"},
	})
	manifests := []domain.Manifest{
		{ManifestType: "api.kind.cluster", Raw: json.RawMessage(`{"name":"c1"}`)},
		{ManifestType: "kubernetes", Raw: json.RawMessage(`{"kind":"ConfigMap"}`)},
	}
	got := domain.FilterAcceptedManifests(target, manifests)
	if len(got) != 1 {
		t.Fatalf("expected 1 manifest after filtering; got %d", len(got))
	}
	if got[0].ManifestType != "api.kind.cluster" {
		t.Errorf("expected api.kind.cluster, got %s", got[0].ManifestType)
	}
}

func TestFilterAcceptedManifests_AllFiltered(t *testing.T) {
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:                    "t1",
		Name:                  "k8s",
		AcceptedManifestTypes: []domain.ManifestType{"kubernetes"},
	})
	manifests := []domain.Manifest{
		{ManifestType: "api.kind.cluster", Raw: json.RawMessage(`{"name":"c1"}`)},
	}
	got := domain.FilterAcceptedManifests(target, manifests)
	if len(got) != 0 {
		t.Fatalf("expected 0 manifests after filtering; got %d", len(got))
	}
}

func TestFilterAcceptedManifests_EmptyManifests(t *testing.T) {
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:                    "t1",
		Name:                  "k8s",
		AcceptedManifestTypes: []domain.ManifestType{"kubernetes"},
	})
	got := domain.FilterAcceptedManifests(target, nil)
	if len(got) != 0 {
		t.Fatalf("expected 0 manifests for nil input; got %d", len(got))
	}
}

func TestFilterAcceptedManifests_MultipleAcceptedTypes(t *testing.T) {
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:                    "t1",
		Name:                  "multi",
		AcceptedManifestTypes: []domain.ManifestType{"api.kind.cluster", "kubernetes"},
	})
	manifests := []domain.Manifest{
		{ManifestType: "api.kind.cluster", Raw: json.RawMessage(`{}`)},
		{ManifestType: "kubernetes", Raw: json.RawMessage(`{}`)},
		{ManifestType: "helm.chart", Raw: json.RawMessage(`{}`)},
	}
	got := domain.FilterAcceptedManifests(target, manifests)
	if len(got) != 2 {
		t.Fatalf("expected 2 manifests after filtering; got %d", len(got))
	}
}
