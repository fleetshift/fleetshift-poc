package domain_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestRegisteredSelfTarget_DeriveStrategies(t *testing.T) {
	rel := domain.RegisteredSelfTarget{AddonTarget: "addon-cluster-mgmt"}
	intent := domain.ResourceIntent{
		ResourceType: "clusters",
		Name:         "prod-us-east-1",
		Version:      1,
		Spec:         json.RawMessage(`{"provider":"rosa","version":"4.16.2"}`),
		CreatedAt:    time.Now(),
	}

	ms, ps, rs := rel.DeriveStrategies(intent)

	if ms.Type != domain.ManifestStrategyManagedResource {
		t.Errorf("ManifestStrategy.Type = %q, want %q", ms.Type, domain.ManifestStrategyManagedResource)
	}
	if ms.IntentRef.ResourceType != "clusters" {
		t.Errorf("IntentRef.ResourceType = %q, want %q", ms.IntentRef.ResourceType, "clusters")
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
	original := domain.RegisteredSelfTarget{AddonTarget: "addon-1"}

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
	if rst.AddonTarget != "addon-1" {
		t.Errorf("AddonTarget = %q, want %q", rst.AddonTarget, "addon-1")
	}
}

func TestSignedRelation_JSONRoundTrip(t *testing.T) {
	original := domain.SignedRelation{
		ResourceType: "clusters",
		Relation:     domain.RegisteredSelfTarget{AddonTarget: "addon-cluster-mgmt"},
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

	if got.ResourceType != "clusters" {
		t.Errorf("ResourceType = %q, want %q", got.ResourceType, "clusters")
	}
	rst, ok := got.Relation.(domain.RegisteredSelfTarget)
	if !ok {
		t.Fatalf("Relation type = %T, want RegisteredSelfTarget", got.Relation)
	}
	if rst.AddonTarget != "addon-cluster-mgmt" {
		t.Errorf("AddonTarget = %q, want %q", rst.AddonTarget, "addon-cluster-mgmt")
	}
	if got.Signature.Signer.Subject != "addon-svc" {
		t.Errorf("Signer.Subject = %q, want %q", got.Signature.Signer.Subject, "addon-svc")
	}
}
