package domain

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// ExtensionResourceUID
// ---------------------------------------------------------------------------

func TestNewExtensionResourceUID(t *testing.T) {
	uid := NewExtensionResourceUID()
	if uid.IsZero() {
		t.Fatal("expected non-zero UID")
	}
}

func TestParseExtensionResourceUID(t *testing.T) {
	uid := NewExtensionResourceUID()
	parsed, err := ParseExtensionResourceUID(uid.String())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if parsed != uid {
		t.Errorf("got %s, want %s", parsed, uid)
	}
}

func TestParseExtensionResourceUID_Invalid(t *testing.T) {
	_, err := ParseExtensionResourceUID("not-a-uuid")
	if err == nil {
		t.Fatal("expected error for invalid UUID")
	}
}

// ---------------------------------------------------------------------------
// ExtensionResourceType construction and accessors
// ---------------------------------------------------------------------------

func TestNewExtensionResourceType(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	rt := ResourceType("kind.fleetshift.io/Cluster")

	ert := NewExtensionResourceType(rt, "v1", "clusters", now)

	assertEq(t, "ResourceType", ert.ResourceType(), rt)
	assertEq(t, "APIServiceName (derived)", ert.APIServiceName(), ServiceName("kind.fleetshift.io"))
	assertEq(t, "APIVersion", ert.APIVersion(), APIVersion("v1"))
	assertEq(t, "CollectionID", ert.CollectionID(), CollectionID("clusters"))
	assertEq(t, "CreatedAt", ert.CreatedAt(), now)
	assertEq(t, "UpdatedAt", ert.UpdatedAt(), now)
	if ert.Management() != nil {
		t.Error("expected nil management for type without management metadata")
	}
}

func TestExtensionResourceType_WithManagement(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	rt := ResourceType("kind.fleetshift.io/Cluster")
	relation := NewRegisteredSelfTarget("target-kind", "managed-resource")
	sig := Signature{Signer: FederatedIdentity{Subject: "addon", Issuer: "https://issuer.example.com"}}

	ert := NewExtensionResourceType(rt, "v1", "clusters", now,
		WithManagement(relation, sig),
	)

	if ert.Management() == nil {
		t.Fatal("expected non-nil management")
	}
	mgmt := ert.Management()
	rst, ok := mgmt.Relation().(RegisteredSelfTarget)
	if !ok {
		t.Fatal("expected RegisteredSelfTarget relation")
	}
	assertEq(t, "Relation.AddonTarget", rst.AddonTarget(), TargetID("target-kind"))
	assertEq(t, "Signature.Signer.Subject", mgmt.Signature().Signer.Subject, "addon")
}

// ---------------------------------------------------------------------------
// ManagementType construction and validation
// ---------------------------------------------------------------------------

func TestNewManagementType(t *testing.T) {
	relation := NewRegisteredSelfTarget("target-kind", "managed-resource")
	sig := Signature{Signer: FederatedIdentity{Subject: "addon", Issuer: "https://issuer.example.com"}}

	mt, err := NewManagementType(relation, sig)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mt.Relation() == nil {
		t.Fatal("expected non-nil relation")
	}
	assertEq(t, "Signature.Signer.Subject", mt.Signature().Signer.Subject, "addon")
}

func TestNewManagementType_NilRelationRejected(t *testing.T) {
	_, err := NewManagementType(nil, Signature{})
	if err == nil {
		t.Fatal("expected error for nil relation")
	}
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("got %v, want ErrInvalidArgument", err)
	}
}

// ---------------------------------------------------------------------------
// ExtensionResource construction and accessors
// ---------------------------------------------------------------------------

func TestNewExtensionResource(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	uid := NewExtensionResourceUID()
	rt := ResourceType("kind.fleetshift.io/Cluster")
	name := ResourceName("clusters/dev")

	r := NewExtensionResource(uid, rt, name, now)

	assertEq(t, "UID", r.UID(), uid)
	assertEq(t, "ResourceType", r.ResourceType(), rt)
	assertEq(t, "Name", r.Name(), name)
	assertEq(t, "CreatedAt", r.CreatedAt(), now)
	assertEq(t, "UpdatedAt", r.UpdatedAt(), now)
	if r.Managed() != nil {
		t.Error("expected nil managed state without WithManagedState")
	}
	if len(r.Labels()) != 0 {
		t.Errorf("expected empty labels, got %v", r.Labels())
	}
}

func TestExtensionResource_WithLabels(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	uid := NewExtensionResourceUID()
	labels := map[string]string{"env": "dev", "tier": "test"}

	r := NewExtensionResource(uid, "kind.fleetshift.io/Cluster", "clusters/dev", now,
		WithExtensionLabels(labels),
	)

	assertEq(t, "Labels[env]", r.Labels()["env"], "dev")
	assertEq(t, "Labels[tier]", r.Labels()["tier"], "test")
}

func TestExtensionResource_WithManagedState(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	uid := NewExtensionResourceUID()

	r := NewExtensionResource(uid, "kind.fleetshift.io/Cluster", "clusters/dev", now,
		WithManagedState("fulfillment-1"),
	)

	if r.Managed() == nil {
		t.Fatal("expected non-nil managed state")
	}
	assertEq(t, "FulfillmentID", r.Managed().FulfillmentID(), FulfillmentID("fulfillment-1"))
	assertEq(t, "CurrentVersion", r.Managed().CurrentVersion(), IntentVersion(0))
}

// ---------------------------------------------------------------------------
// RecordIntent
// ---------------------------------------------------------------------------

func TestExtensionResource_RecordIntent(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	uid := NewExtensionResourceUID()

	r := NewExtensionResource(uid, "kind.fleetshift.io/Cluster", "clusters/dev", now,
		WithManagedState("fulfillment-1"),
	)

	spec := json.RawMessage(`{"version":"1.29"}`)
	later := now.Add(time.Minute)
	intent, err := r.RecordIntent(spec, later)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertEq(t, "intent.Version", intent.Version, IntentVersion(1))
	assertEq(t, "intent.ResourceType", intent.ResourceType, ResourceType("kind.fleetshift.io/Cluster"))
	assertEq(t, "intent.Name", intent.Name, ResourceName("clusters/dev"))
	assertEq(t, "managed.CurrentVersion", r.Managed().CurrentVersion(), IntentVersion(1))
}

func TestExtensionResource_RecordIntent_Increments(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	uid := NewExtensionResourceUID()

	r := NewExtensionResource(uid, "kind.fleetshift.io/Cluster", "clusters/dev", now,
		WithManagedState("fulfillment-1"),
	)

	i1, _ := r.RecordIntent(json.RawMessage(`{"v":1}`), now)
	i2, _ := r.RecordIntent(json.RawMessage(`{"v":2}`), now.Add(time.Minute))

	assertEq(t, "first intent version", i1.Version, IntentVersion(1))
	assertEq(t, "second intent version", i2.Version, IntentVersion(2))
	assertEq(t, "managed.CurrentVersion", r.Managed().CurrentVersion(), IntentVersion(2))
}

func TestExtensionResource_RecordIntent_WithoutManagedState(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	uid := NewExtensionResourceUID()

	r := NewExtensionResource(uid, "kind.fleetshift.io/Cluster", "clusters/dev", now)

	_, err := r.RecordIntent(json.RawMessage(`{}`), now)
	if err == nil {
		t.Fatal("expected error when recording intent without managed state")
	}
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("got %v, want ErrInvalidArgument", err)
	}
}

// ---------------------------------------------------------------------------
// JSON round-trip for ExtensionResourceType (workflow replay fidelity)
// ---------------------------------------------------------------------------

func TestExtensionResourceType_JSONRoundTrip(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	relation := NewRegisteredSelfTarget("target-kind", "managed-resource")
	sig := Signature{Signer: FederatedIdentity{Subject: "addon", Issuer: "iss"}}

	original := NewExtensionResourceType(
		"kind.fleetshift.io/Cluster", "v1", "clusters", now,
		WithManagement(relation, sig),
	)

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var restored ExtensionResourceType
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	assertEq(t, "ResourceType", restored.ResourceType(), original.ResourceType())
	assertEq(t, "APIServiceName", restored.APIServiceName(), original.APIServiceName())
	assertEq(t, "APIVersion", restored.APIVersion(), original.APIVersion())
	assertEq(t, "CollectionID", restored.CollectionID(), original.CollectionID())
	assertEq(t, "CreatedAt", restored.CreatedAt(), original.CreatedAt())
	assertEq(t, "UpdatedAt", restored.UpdatedAt(), original.UpdatedAt())
	if restored.Management() == nil {
		t.Fatal("management is nil after round-trip")
	}
	rst, ok := restored.Management().Relation().(RegisteredSelfTarget)
	if !ok {
		t.Fatal("expected RegisteredSelfTarget after round-trip")
	}
	assertEq(t, "Relation.AddonTarget", rst.AddonTarget(), TargetID("target-kind"))
	assertEq(t, "Signature.Signer.Subject", restored.Management().Signature().Signer.Subject, "addon")
}

func TestExtensionResourceType_JSONRoundTrip_NoManagement(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	original := NewExtensionResourceType("kind.fleetshift.io/Cluster", "v1", "clusters", now)

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var restored ExtensionResourceType
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	assertEq(t, "ResourceType", restored.ResourceType(), original.ResourceType())
	if restored.Management() != nil {
		t.Error("expected nil management after round-trip")
	}
}
