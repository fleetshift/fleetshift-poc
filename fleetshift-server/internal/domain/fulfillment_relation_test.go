package domain_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestNewTargetID(t *testing.T) {
	if _, err := domain.NewTargetID("addon-cluster"); err != nil {
		t.Fatalf("valid: unexpected error: %v", err)
	}
	_, err := domain.NewTargetID("")
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("empty: got %v, want ErrInvalidArgument", err)
	}
}

func TestNewManifestType(t *testing.T) {
	if _, err := domain.NewManifestType("api.kind.cluster"); err != nil {
		t.Fatalf("valid: unexpected error: %v", err)
	}
	_, err := domain.NewManifestType("")
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("empty: got %v, want ErrInvalidArgument", err)
	}
}

func TestNewRegisteredSelfTarget(t *testing.T) {
	rel := domain.NewRegisteredSelfTarget("addon-cluster-mgmt", "api.kind.cluster")
	if rel.AddonTarget() != "addon-cluster-mgmt" {
		t.Errorf("AddonTarget() = %q, want %q", rel.AddonTarget(), "addon-cluster-mgmt")
	}
	if rel.ManifestType() != "api.kind.cluster" {
		t.Errorf("ManifestType() = %q, want %q", rel.ManifestType(), "api.kind.cluster")
	}
}

func TestRegisteredSelfTarget_DeriveStrategies(t *testing.T) {
	rel := domain.NewRegisteredSelfTarget("addon-cluster-mgmt", "api.kind.cluster")
	intent := domain.ResourceIntent{
		ResourceType: "test.fleetshift.io/Cluster",
		Name:         "prod-us-east-1",
		Version:      1,
		Spec:         json.RawMessage(`{"provider":"rosa","version":"4.16.2"}`),
		CreatedAt:    time.Now(),
	}

	ms, ps, rs := rel.DeriveStrategies(intent)

	if ms.Type != domain.ManifestStrategyManagedResource {
		t.Errorf("ManifestStrategy.Type = %q, want %q", ms.Type, domain.ManifestStrategyManagedResource)
	}
	if ms.IntentRef.ResourceType != "test.fleetshift.io/Cluster" {
		t.Errorf("IntentRef.ResourceType = %q, want %q", ms.IntentRef.ResourceType, "test.fleetshift.io/Cluster")
	}
	if ms.IntentRef.Name != "prod-us-east-1" {
		t.Errorf("IntentRef.Name = %q, want %q", ms.IntentRef.Name, "prod-us-east-1")
	}
	if ms.IntentRef.Version != 1 {
		t.Errorf("IntentRef.Version = %d, want 1", ms.IntentRef.Version)
	}

	if ps.Type != domain.PlacementStrategyStatic {
		t.Errorf("PlacementStrategy.Type = %q, want %q", ps.Type, domain.PlacementStrategyStatic)
	}
	if len(ps.Targets) != 1 || ps.Targets[0] != "addon-cluster-mgmt" {
		t.Errorf("PlacementStrategy.Targets = %v, want [addon-cluster-mgmt]", ps.Targets)
	}

	if rs == nil {
		t.Fatal("RolloutStrategy is nil")
	}
	if rs.Type != domain.RolloutStrategyImmediate {
		t.Errorf("RolloutStrategy.Type = %q, want %q", rs.Type, domain.RolloutStrategyImmediate)
	}
}

func TestFulfillmentRelation_JSONRoundTrip(t *testing.T) {
	original := domain.NewRegisteredSelfTarget("addon-1", "api.kind.cluster")

	data, err := domain.MarshalFulfillmentRelation(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	got, err := domain.UnmarshalFulfillmentRelation(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	rst, ok := got.(domain.RegisteredSelfTarget)
	if !ok {
		t.Fatalf("got type %T, want RegisteredSelfTarget", got)
	}
	if rst.AddonTarget() != "addon-1" {
		t.Errorf("AddonTarget() = %q, want %q", rst.AddonTarget(), "addon-1")
	}
	if rst.ManifestType() != "api.kind.cluster" {
		t.Errorf("ManifestType() = %q, want %q", rst.ManifestType(), "api.kind.cluster")
	}
}

func TestFulfillmentRelation_UnmarshalRejectsEmptyFields(t *testing.T) {
	tests := []struct {
		name string
		json string
	}{
		{
			name: "empty addon_target",
			json: `{"Type":"RegisteredSelfTarget","RegisteredSelfTarget":{"addon_target":"","manifest_type":"api.kind.cluster"}}`,
		},
		{
			name: "empty manifest_type",
			json: `{"Type":"RegisteredSelfTarget","RegisteredSelfTarget":{"addon_target":"addon-1","manifest_type":""}}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := domain.UnmarshalFulfillmentRelation([]byte(tc.json))
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestSignedRelation_JSONRoundTrip(t *testing.T) {
	rel := domain.NewRegisteredSelfTarget("addon-cluster-mgmt", "api.kind.cluster")
	original := domain.SignedRelation{
		ResourceType: "test.fleetshift.io/Cluster",
		Relation:     rel,
		Signature: domain.Signature{
			Signer:         domain.FederatedIdentity{Subject: "addon-svc", Issuer: "https://issuer.example"},
			ContentHash:    []byte("hash123"),
			SignatureBytes: []byte("sig456"),
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var got domain.SignedRelation
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if got.ResourceType != "test.fleetshift.io/Cluster" {
		t.Errorf("ResourceType = %q, want %q", got.ResourceType, "test.fleetshift.io/Cluster")
	}
	rst, ok := got.Relation.(domain.RegisteredSelfTarget)
	if !ok {
		t.Fatalf("Relation type = %T, want RegisteredSelfTarget", got.Relation)
	}
	if rst.AddonTarget() != "addon-cluster-mgmt" {
		t.Errorf("AddonTarget() = %q, want %q", rst.AddonTarget(), "addon-cluster-mgmt")
	}
	if got.Signature.Signer.Subject != "addon-svc" {
		t.Errorf("Signer.Subject = %q, want %q", got.Signature.Signer.Subject, "addon-svc")
	}
}
