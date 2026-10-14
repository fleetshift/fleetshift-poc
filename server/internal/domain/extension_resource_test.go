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

func TestExtensionResource_SetLabels(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	r := NewExtensionResource(
		NewExtensionResourceUID(),
		"kind.fleetshift.io/Cluster",
		"clusters/dev",
		now,
		WithExtensionLabels(map[string]string{"env": "dev"}),
	)

	later := now.Add(time.Minute)
	incoming := map[string]string{"env": "prod", "tier": "1"}
	r.SetLabels(incoming, later)

	assertEq(t, "Labels[env]", r.Labels()["env"], "prod")
	assertEq(t, "Labels[tier]", r.Labels()["tier"], "1")
	assertEq(t, "UpdatedAt", r.UpdatedAt(), later)

	// Caller mutation must not affect the resource's stored map.
	incoming["env"] = "mutated"
	assertEq(t, "Labels[env] after caller mutate", r.Labels()["env"], "prod")

	r.SetLabels(nil, later.Add(time.Second))
	if len(r.Labels()) != 0 {
		t.Errorf("SetLabels(nil): got %v, want empty", r.Labels())
	}
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
	assertEq(t, "intent.ExtensionResourceUID", intent.ExtensionResourceUID, uid)
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
// ConditionType
// ---------------------------------------------------------------------------

func TestNewConditionType_Valid(t *testing.T) {
	ct, err := NewConditionType("Ready")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertEq(t, "ConditionType", string(ct), "Ready")
}

func TestNewConditionType_Empty_Rejected(t *testing.T) {
	_, err := NewConditionType("")
	if err == nil {
		t.Fatal("expected error for empty condition type")
	}
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("got %v, want ErrInvalidArgument", err)
	}
}

// ---------------------------------------------------------------------------
// ConditionStatus
// ---------------------------------------------------------------------------

func TestParseConditionStatus_Valid(t *testing.T) {
	for _, s := range []string{"True", "False", "Unknown"} {
		cs, err := ParseConditionStatus(s)
		if err != nil {
			t.Errorf("ParseConditionStatus(%q): unexpected error: %v", s, err)
		}
		assertEq(t, "ConditionStatus", string(cs), s)
	}
}

func TestParseConditionStatus_Invalid_Rejected(t *testing.T) {
	for _, s := range []string{"", "Bogus", "true", "false"} {
		_, err := ParseConditionStatus(s)
		if err == nil {
			t.Errorf("ParseConditionStatus(%q): expected error", s)
		}
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("ParseConditionStatus(%q): got %v, want ErrInvalidArgument", s, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Condition
// ---------------------------------------------------------------------------

func TestNewCondition(t *testing.T) {
	ct, _ := NewConditionType("Ready")
	transTime := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	cond, err := NewCondition(ct, ConditionTrue, "AllGood", "everything is fine", transTime)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertEq(t, "Type", cond.Type(), ct)
	assertEq(t, "Status", cond.Status(), ConditionTrue)
	assertEq(t, "Reason", cond.Reason(), "AllGood")
	assertEq(t, "Message", cond.Message(), "everything is fine")
	assertEq(t, "LastTransitionTime", cond.LastTransitionTime(), transTime)
}

// ---------------------------------------------------------------------------
// InventoryType
// ---------------------------------------------------------------------------

func TestNewInventoryType(t *testing.T) {
	it := NewInventoryType()
	// InventoryType is a pure capability marker; nothing to assert beyond
	// successful construction.
	_ = it
}

// ---------------------------------------------------------------------------
// ExtensionResourceType with Inventory
// ---------------------------------------------------------------------------

func TestExtensionResourceType_WithInventory(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	rt := ResourceType("kind.fleetshift.io/Cluster")

	ert := NewExtensionResourceType(rt, "v1", "clusters", now,
		WithInventory(),
	)

	if ert.Inventory() == nil {
		t.Fatal("expected non-nil inventory")
	}
}

func TestExtensionResourceType_ManagedOnly_NoInventory(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	rt := ResourceType("kind.fleetshift.io/Cluster")
	relation := NewRegisteredSelfTarget("target-kind", "managed-resource")
	sig := Signature{Signer: FederatedIdentity{Subject: "addon", Issuer: "https://issuer.example.com"}}

	ert := NewExtensionResourceType(rt, "v1", "clusters", now,
		WithManagement(relation, sig),
	)

	if ert.Inventory() != nil {
		t.Error("expected nil inventory for managed-only type")
	}
}

func TestExtensionResourceType_InventoryOnly_NoManagement(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	rt := ResourceType("kind.fleetshift.io/Cluster")

	ert := NewExtensionResourceType(rt, "v1", "clusters", now,
		WithInventory(),
	)

	if ert.Management() != nil {
		t.Error("expected nil management for inventory-only type")
	}
	if ert.Inventory() == nil {
		t.Fatal("expected non-nil inventory")
	}
}

func TestExtensionResourceType_ManagedPlusInventory(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	rt := ResourceType("kind.fleetshift.io/Cluster")
	relation := NewRegisteredSelfTarget("target-kind", "managed-resource")
	sig := Signature{Signer: FederatedIdentity{Subject: "addon", Issuer: "https://issuer.example.com"}}

	ert := NewExtensionResourceType(rt, "v1", "clusters", now,
		WithManagement(relation, sig),
		WithInventory(),
	)

	if ert.Management() == nil {
		t.Fatal("expected non-nil management")
	}
	if ert.Inventory() == nil {
		t.Fatal("expected non-nil inventory")
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
