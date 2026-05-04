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

func TestBuildManagedResourceEnvelope_Deterministic(t *testing.T) {
	spec := json.RawMessage(`{"provider":"rosa","version":"4.16.2"}`)

	a, err := domain.BuildManagedResourceEnvelope("clusters", "prod-us-east-1", spec, testValidUntil, nil, 1)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	b, err := domain.BuildManagedResourceEnvelope("clusters", "prod-us-east-1", spec, testValidUntil, nil, 1)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}

	if string(a) != string(b) {
		t.Errorf("envelopes differ:\n  a: %s\n  b: %s", a, b)
	}
}

func TestSignedInput_ComposesProvenanceAndSigner(t *testing.T) {
	prov := domain.Provenance{
		Content: domain.DeploymentContent{
			DeploymentID: "dep-42",
			ManifestStrategy: domain.ManifestStrategySpec{
				Type: domain.ManifestStrategyInline,
			},
			PlacementStrategy: domain.PlacementStrategySpec{
				Type: domain.PlacementStrategyAll,
			},
		},
		Sig: domain.Signature{
			Signer:         domain.FederatedIdentity{Subject: "alice", Issuer: "https://idp.example.com"},
			ContentHash:    []byte("fakehash"),
			SignatureBytes: []byte("fakesig"),
		},
		ValidUntil:         testValidUntil,
		ExpectedGeneration: 3,
		OutputConstraints:  []domain.OutputConstraint{{Name: "c1", Expression: "true"}},
	}
	signer := domain.SignerAssertion{
		IdentityToken:   "tok",
		RegistryID:      "github.com",
		RegistrySubject: "alice-gh",
	}

	si := domain.SignedInput{
		Provenance: prov,
		Signer:     signer,
	}

	if si.Provenance.Content.ContentID() != "dep-42" {
		t.Errorf("Content accessible through Provenance: got %q", si.Provenance.Content.ContentID())
	}
	if si.Signer.RegistrySubject != "alice-gh" {
		t.Errorf("Signer: got %q", si.Signer.RegistrySubject)
	}
}

func TestSignedInput_JSONRoundTrip(t *testing.T) {
	si := domain.SignedInput{
		Provenance: domain.Provenance{
			Content: domain.DeploymentContent{
				DeploymentID: "dep-rt",
				ManifestStrategy: domain.ManifestStrategySpec{
					Type: domain.ManifestStrategyInline,
				},
				PlacementStrategy: domain.PlacementStrategySpec{
					Type: domain.PlacementStrategyAll,
				},
			},
			Sig: domain.Signature{
				Signer:         domain.FederatedIdentity{Subject: "bob", Issuer: "https://idp.example.com"},
				ContentHash:    []byte("hash"),
				SignatureBytes: []byte("sig"),
			},
			ValidUntil:         testValidUntil,
			ExpectedGeneration: 1,
		},
		Signer: domain.SignerAssertion{
			IdentityToken:   "tok",
			RegistryID:      "github.com",
			RegistrySubject: "bob-gh",
		},
	}

	data, err := json.Marshal(si)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got domain.SignedInput
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got.Provenance.Content.ContentID() != "dep-rt" {
		t.Errorf("Content.ContentID = %q, want dep-rt", got.Provenance.Content.ContentID())
	}
	if got.Provenance.Sig.Signer.Subject != "bob" {
		t.Errorf("Sig.Signer.Subject = %q, want bob", got.Provenance.Sig.Signer.Subject)
	}
	if got.Signer.RegistrySubject != "bob-gh" {
		t.Errorf("Signer.RegistrySubject = %q, want bob-gh", got.Signer.RegistrySubject)
	}
	if !got.Provenance.ValidUntil.Equal(testValidUntil) {
		t.Errorf("ValidUntil = %v, want %v", got.Provenance.ValidUntil, testValidUntil)
	}
	if got.Provenance.ExpectedGeneration != 1 {
		t.Errorf("ExpectedGeneration = %d, want 1", got.Provenance.ExpectedGeneration)
	}
}

func TestAttestation_JSONRoundTrip_WithComposedSignedInput(t *testing.T) {
	att := domain.Attestation{
		Input: domain.SignedInput{
			Provenance: domain.Provenance{
				Content: domain.DeploymentContent{
					DeploymentID: "dep-att",
					ManifestStrategy: domain.ManifestStrategySpec{
						Type:      domain.ManifestStrategyInline,
						Manifests: []domain.Manifest{{ResourceType: "api.kind.cluster", Raw: json.RawMessage(`{}`)}},
					},
					PlacementStrategy: domain.PlacementStrategySpec{
						Type: domain.PlacementStrategyAll,
					},
				},
				Sig: domain.Signature{
					Signer:         domain.FederatedIdentity{Subject: "carol", Issuer: "https://idp.example.com"},
					ContentHash:    []byte("hash"),
					SignatureBytes: []byte("sig"),
				},
				ValidUntil:         testValidUntil,
				ExpectedGeneration: 2,
			},
			Signer: domain.SignerAssertion{
				IdentityToken:   "tok",
				RegistryID:      "github.com",
				RegistrySubject: "carol-gh",
			},
		},
		SignedRelation: &domain.SignedRelation{
			Relation:  domain.RegisteredSelfTarget{AddonTarget: "addon-cluster-mgmt"},
			Signature: domain.Signature{Signer: domain.FederatedIdentity{Subject: "addon-svc", Issuer: "https://addon.example.com"}},
		},
		Output: &domain.PutManifests{
			Manifests: []domain.Manifest{{ResourceType: "api.kind.cluster", Raw: json.RawMessage(`{}`)}},
		},
	}

	data, err := json.Marshal(att)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got domain.Attestation
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got.Input.Provenance.Content.ContentID() != "dep-att" {
		t.Errorf("Input.Provenance.Content.ContentID = %q, want dep-att", got.Input.Provenance.Content.ContentID())
	}
	if got.Input.Signer.RegistrySubject != "carol-gh" {
		t.Errorf("Input.Signer = %q, want carol-gh", got.Input.Signer.RegistrySubject)
	}
	if got.SignedRelation == nil {
		t.Fatal("SignedRelation = nil, want populated relation")
	}
	if rel, ok := got.SignedRelation.Relation.(domain.RegisteredSelfTarget); !ok || rel.AddonTarget != "addon-cluster-mgmt" {
		t.Fatalf("SignedRelation.Relation = %#v, want RegisteredSelfTarget(addon-cluster-mgmt)", got.SignedRelation.Relation)
	}
	pm, ok := got.Output.(*domain.PutManifests)
	if !ok {
		t.Fatalf("Output type = %T, want *PutManifests", got.Output)
	}
	if len(pm.Manifests) != 1 {
		t.Errorf("Output.Manifests len = %d, want 1", len(pm.Manifests))
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
