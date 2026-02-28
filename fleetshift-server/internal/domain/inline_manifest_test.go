package domain_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestInlineManifest_ReturnsManifestsVerbatim(t *testing.T) {
	manifests := []domain.Manifest{
		{Raw: json.RawMessage(`{"kind":"ConfigMap","metadata":{"name":"cm1"}}`)},
		{Raw: json.RawMessage(`{"kind":"Secret","metadata":{"name":"s1"}}`)},
	}
	s := &domain.InlineManifestStrategy{Manifests: manifests}

	got, err := s.Generate(context.Background(), domain.GenerateContext{
		Target: domain.TargetInfo{ID: "t1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 manifests, got %d", len(got))
	}
	if string(got[0].Raw) != string(manifests[0].Raw) {
		t.Fatalf("manifest mismatch: %s != %s", got[0].Raw, manifests[0].Raw)
	}
}

func TestInlineManifest_ReturnsCopy(t *testing.T) {
	manifests := []domain.Manifest{
		{Raw: json.RawMessage(`{}`)},
	}
	s := &domain.InlineManifestStrategy{Manifests: manifests}

	got, _ := s.Generate(context.Background(), domain.GenerateContext{})
	got[0].Raw = json.RawMessage(`{"mutated":true}`)

	if string(s.Manifests[0].Raw) != "{}" {
		t.Fatal("Generate should return a copy; original was mutated")
	}
}

func TestInlineManifest_OnRemovedIsNoop(t *testing.T) {
	s := &domain.InlineManifestStrategy{}
	if err := s.OnRemoved(context.Background(), "t1"); err != nil {
		t.Fatalf("OnRemoved should be a no-op, got error: %v", err)
	}
}
