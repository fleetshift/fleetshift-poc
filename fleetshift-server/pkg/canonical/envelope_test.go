package canonical_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/pkg/canonical"
)

var testValidUntil = time.Date(2026, 3, 11, 12, 0, 0, 0, time.UTC)

func TestBuildSignedInputEnvelope_Deterministic(t *testing.T) {
	ms := canonical.ManifestStrategy{
		Type: "inline",
		Manifests: []canonical.Manifest{{
			ResourceType: "api.kind.cluster",
			Raw:          json.RawMessage(`{"name":"test-cluster"}`),
		}},
	}
	ps := canonical.PlacementStrategy{
		Type:    "static",
		Targets: []string{"t1", "t2"},
	}

	a, err := canonical.BuildSignedInputEnvelope("dep-1", ms, ps, testValidUntil, nil, 1)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	b, err := canonical.BuildSignedInputEnvelope("dep-1", ms, ps, testValidUntil, nil, 1)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}

	if string(a) != string(b) {
		t.Errorf("envelopes differ:\n  a: %s\n  b: %s", a, b)
	}
}

func TestBuildSignedInputEnvelope_DifferentInputs(t *testing.T) {
	ms := canonical.ManifestStrategy{Type: "inline"}
	ps := canonical.PlacementStrategy{Type: "all"}

	a, _ := canonical.BuildSignedInputEnvelope("dep-1", ms, ps, testValidUntil, nil, 1)
	b, _ := canonical.BuildSignedInputEnvelope("dep-2", ms, ps, testValidUntil, nil, 1)

	if string(a) == string(b) {
		t.Error("different deployment IDs should produce different envelopes")
	}
}

func TestBuildSignedInputEnvelope_OmitsZeroGeneration(t *testing.T) {
	ms := canonical.ManifestStrategy{Type: "inline"}
	ps := canonical.PlacementStrategy{Type: "all"}

	env, _ := canonical.BuildSignedInputEnvelope("dep-1", ms, ps, testValidUntil, nil, 0)

	var parsed map[string]any
	if err := json.Unmarshal(env, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := parsed["expected_generation"]; ok {
		t.Error("expected_generation should be omitted when zero")
	}
}

func TestBuildSignedInputEnvelope_IncludesNonZeroGeneration(t *testing.T) {
	ms := canonical.ManifestStrategy{Type: "inline"}
	ps := canonical.PlacementStrategy{Type: "all"}

	env, _ := canonical.BuildSignedInputEnvelope("dep-1", ms, ps, testValidUntil, nil, 3)

	var parsed map[string]any
	if err := json.Unmarshal(env, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	gen, ok := parsed["expected_generation"]
	if !ok {
		t.Fatal("expected_generation should be present for non-zero value")
	}
	if gen.(float64) != 3 {
		t.Errorf("expected_generation = %v, want 3", gen)
	}
}

func TestBuildSignedInputEnvelope_EmptyConstraints(t *testing.T) {
	ms := canonical.ManifestStrategy{Type: "inline"}
	ps := canonical.PlacementStrategy{Type: "all"}

	env, _ := canonical.BuildSignedInputEnvelope("dep-1", ms, ps, testValidUntil, nil, 1)

	var parsed map[string]any
	if err := json.Unmarshal(env, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	constraints := parsed["output_constraints"].([]any)
	if len(constraints) != 0 {
		t.Errorf("output_constraints should be empty slice, got %v", constraints)
	}
}

func TestBuildSignedInputEnvelope_ConstraintsSorted(t *testing.T) {
	ms := canonical.ManifestStrategy{Type: "inline"}
	ps := canonical.PlacementStrategy{Type: "all"}

	constraints := []canonical.OutputConstraint{
		{Name: "z-constraint", Expression: "output.foo == true"},
		{Name: "a-constraint", Expression: "output.bar == true"},
	}

	env, _ := canonical.BuildSignedInputEnvelope("dep-1", ms, ps, testValidUntil, constraints, 1)

	var parsed struct {
		OutputConstraints []struct {
			Expression string `json:"expression"`
			Name       string `json:"name"`
		} `json:"output_constraints"`
	}
	if err := json.Unmarshal(env, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(parsed.OutputConstraints) != 2 {
		t.Fatalf("expected 2 constraints, got %d", len(parsed.OutputConstraints))
	}
	if parsed.OutputConstraints[0].Name != "a-constraint" {
		t.Errorf("first constraint should be a-constraint (sorted), got %q", parsed.OutputConstraints[0].Name)
	}
}

func TestBuildSignedInputEnvelope_StructureMatchesPOC(t *testing.T) {
	ms := canonical.ManifestStrategy{
		Type: "inline",
		Manifests: []canonical.Manifest{{
			ResourceType: "api.kind.cluster",
			Raw:          json.RawMessage(`{"name":"c1"}`),
		}},
	}
	ps := canonical.PlacementStrategy{
		Type:    "static",
		Targets: []string{"target-a"},
	}

	env, _ := canonical.BuildSignedInputEnvelope("my-dep", ms, ps, testValidUntil, nil, 1)

	var parsed map[string]any
	if err := json.Unmarshal(env, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	for _, key := range []string{"content", "output_constraints", "valid_until"} {
		if _, ok := parsed[key]; !ok {
			t.Errorf("missing top-level key %q", key)
		}
	}

	content := parsed["content"].(map[string]any)
	for _, key := range []string{"deployment_id", "manifest_strategy", "placement_strategy"} {
		if _, ok := content[key]; !ok {
			t.Errorf("missing content key %q", key)
		}
	}
}

func TestHashIntent_Deterministic(t *testing.T) {
	data := []byte(`{"content":{"deployment_id":"test"}}`)
	a := canonical.HashIntent(data)
	b := canonical.HashIntent(data)

	if len(a) != 32 {
		t.Fatalf("hash should be 32 bytes (SHA-256), got %d", len(a))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatal("hash should be deterministic")
		}
	}
}
