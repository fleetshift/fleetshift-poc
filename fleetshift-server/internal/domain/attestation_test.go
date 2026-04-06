package domain_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

var testValidUntil = time.Date(2026, 3, 11, 12, 0, 0, 0, time.UTC)

func TestBuildSignedInputEnvelope_Deterministic(t *testing.T) {
	ms := domain.ManifestStrategySpec{
		Type: domain.ManifestStrategyInline,
		Manifests: []domain.Manifest{{
			ResourceType: "api.kind.cluster",
			Raw:          json.RawMessage(`{"name":"test-cluster"}`),
		}},
	}
	ps := domain.PlacementStrategySpec{
		Type:    domain.PlacementStrategyStatic,
		Targets: []domain.TargetID{"t1", "t2"},
	}

	a, err := domain.BuildSignedInputEnvelope("dep-1", ms, ps, testValidUntil, nil, 1)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	b, err := domain.BuildSignedInputEnvelope("dep-1", ms, ps, testValidUntil, nil, 1)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}

	if string(a) != string(b) {
		t.Errorf("envelopes differ:\n  a: %s\n  b: %s", a, b)
	}
}

func TestBuildSignedInputEnvelope_DifferentInputs(t *testing.T) {
	ms := domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline}
	ps := domain.PlacementStrategySpec{Type: domain.PlacementStrategyAll}

	a, _ := domain.BuildSignedInputEnvelope("dep-1", ms, ps, testValidUntil, nil, 1)
	b, _ := domain.BuildSignedInputEnvelope("dep-2", ms, ps, testValidUntil, nil, 1)

	if string(a) == string(b) {
		t.Error("different deployment IDs should produce different envelopes")
	}
}

func TestBuildSignedInputEnvelope_OmitsZeroGeneration(t *testing.T) {
	ms := domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline}
	ps := domain.PlacementStrategySpec{Type: domain.PlacementStrategyAll}

	env, _ := domain.BuildSignedInputEnvelope("dep-1", ms, ps, testValidUntil, nil, 0)

	var parsed map[string]any
	if err := json.Unmarshal(env, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := parsed["expected_generation"]; ok {
		t.Error("expected_generation should be omitted when zero")
	}
}

func TestHashIntent_ProducesFixedLength(t *testing.T) {
	data := []byte(`{"content":{"deployment_id":"test"}}`)
	hash := domain.HashIntent(data)
	if len(hash) != 32 {
		t.Fatalf("hash should be 32 bytes (SHA-256), got %d", len(hash))
	}
}

func TestHashIntent_Deterministic(t *testing.T) {
	data := []byte(`{"content":{"deployment_id":"test"}}`)
	a := domain.HashIntent(data)
	b := domain.HashIntent(data)

	for i := range a {
		if a[i] != b[i] {
			t.Fatal("hash should be deterministic")
		}
	}
}
