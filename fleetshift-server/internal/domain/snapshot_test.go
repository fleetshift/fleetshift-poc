package domain

import (
	"encoding/json"
	"testing"
	"time"
)

// refTime is a fixed reference time used across snapshot tests.
var refTime = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

func TestFulfillmentSnapshot_RoundTrip(t *testing.T) {
	gen := Generation(7)
	snap := FulfillmentSnapshot{
		ID:                       "f-1",
		ManifestStrategy:         ManifestStrategySpec{Type: ManifestStrategyInline},
		ManifestStrategyVersion:  3,
		PlacementStrategy:        PlacementStrategySpec{Type: PlacementStrategyAll},
		PlacementStrategyVersion: 2,
		RolloutStrategy:          &RolloutStrategySpec{Type: RolloutStrategyImmediate},
		RolloutStrategyVersion:   1,
		ResolvedTargets:          []TargetID{"t1", "t2"},
		State:                    FulfillmentStateActive,
		StatusReason:             "all good",
		Auth:                     DeliveryAuth{Token: "tok"},
		Provenance:               &Provenance{Sig: Signature{Signer: FederatedIdentity{Subject: "u1", Issuer: "iss"}}},
		AttestationRef:           &AttestationRef{RelationRef: ptrTo(ResourceType("test.fleetshift.io/K8s"))},
		Generation:               gen,
		ObservedGeneration:       5,
		ActiveWorkflowGen:        &gen,
		CreatedAt:                refTime,
		UpdatedAt:                refTime.Add(time.Hour),
		PendingStrategyRecords: PendingStrategyRecords{
			Manifest: []ManifestStrategyRecord{{FulfillmentID: "f-1", Version: 4}},
		},
	}

	f := FulfillmentFromSnapshot(snap)
	got := f.Snapshot()

	// Persisted state must round-trip.
	assertEq(t, "ID", got.ID, snap.ID)
	assertEq(t, "ManifestStrategy.Type", got.ManifestStrategy.Type, snap.ManifestStrategy.Type)
	assertEq(t, "ManifestStrategyVersion", got.ManifestStrategyVersion, snap.ManifestStrategyVersion)
	assertEq(t, "PlacementStrategy.Type", got.PlacementStrategy.Type, snap.PlacementStrategy.Type)
	assertEq(t, "PlacementStrategyVersion", got.PlacementStrategyVersion, snap.PlacementStrategyVersion)
	if got.RolloutStrategy == nil {
		t.Fatal("RolloutStrategy is nil, want non-nil")
	}
	assertEq(t, "RolloutStrategy.Type", got.RolloutStrategy.Type, snap.RolloutStrategy.Type)
	assertEq(t, "RolloutStrategyVersion", got.RolloutStrategyVersion, snap.RolloutStrategyVersion)
	assertEq(t, "len(ResolvedTargets)", len(got.ResolvedTargets), len(snap.ResolvedTargets))
	assertEq(t, "State", got.State, snap.State)
	assertEq(t, "StatusReason", got.StatusReason, snap.StatusReason)
	assertEq(t, "Auth.Token", got.Auth.Token, snap.Auth.Token)
	if got.Provenance == nil {
		t.Fatal("Provenance is nil, want non-nil")
	}
	assertEq(t, "Provenance.Sig.Signer.Subject", got.Provenance.Sig.Signer.Subject, snap.Provenance.Sig.Signer.Subject)
	if got.AttestationRef == nil {
		t.Fatal("AttestationRef is nil, want non-nil")
	}
	assertEq(t, "AttestationRef.RelationRef", *got.AttestationRef.RelationRef, *snap.AttestationRef.RelationRef)
	assertEq(t, "Generation", got.Generation, snap.Generation)
	assertEq(t, "ObservedGeneration", got.ObservedGeneration, snap.ObservedGeneration)
	if got.ActiveWorkflowGen == nil {
		t.Fatal("ActiveWorkflowGen is nil, want non-nil")
	}
	assertEq(t, "*ActiveWorkflowGen", *got.ActiveWorkflowGen, *snap.ActiveWorkflowGen)
	assertEq(t, "CreatedAt", got.CreatedAt, snap.CreatedAt)
	assertEq(t, "UpdatedAt", got.UpdatedAt, snap.UpdatedAt)

	// Pending buffers must be zeroed on hydration round-trip.
	if len(got.PendingStrategyRecords.Manifest) != 0 {
		t.Errorf("PendingStrategyRecords.Manifest len = %d, want 0 after round-trip",
			len(got.PendingStrategyRecords.Manifest))
	}
	if len(got.PendingStrategyRecords.Placement) != 0 {
		t.Errorf("PendingStrategyRecords.Placement len = %d, want 0 after round-trip",
			len(got.PendingStrategyRecords.Placement))
	}
	if len(got.PendingStrategyRecords.Rollout) != 0 {
		t.Errorf("PendingStrategyRecords.Rollout len = %d, want 0 after round-trip",
			len(got.PendingStrategyRecords.Rollout))
	}
}

func TestFulfillmentFromSnapshot_SetsLoadedGeneration(t *testing.T) {
	f := FulfillmentFromSnapshot(FulfillmentSnapshot{
		ID:         "f-1",
		Generation: 10,
	})

	// advanceGeneration should set Generation to loadedGeneration+1 = 11.
	f.advanceGeneration()
	assertEq(t, "Generation after advanceGeneration", f.Generation(), Generation(11))
}

func TestDeploymentSnapshot_RoundTrip(t *testing.T) {
	uid := NewDeploymentUID()
	snap := DeploymentSnapshot{
		Name:          "deployments/d-1",
		UID:           uid,
		FulfillmentID: "f-1",
		CreatedAt:     refTime,
		UpdatedAt:     refTime.Add(time.Minute),
	}

	d := DeploymentFromSnapshot(snap)
	got := d.Snapshot()

	assertEq(t, "Name", got.Name, snap.Name)
	assertEq(t, "UID", got.UID, snap.UID)
	assertEq(t, "FulfillmentID", got.FulfillmentID, snap.FulfillmentID)
	assertEq(t, "CreatedAt", got.CreatedAt, snap.CreatedAt)
	assertEq(t, "UpdatedAt", got.UpdatedAt, snap.UpdatedAt)
}

func TestDeliverySnapshot_RoundTrip(t *testing.T) {
	snap := DeliverySnapshot{
		ID:            "del-1",
		FulfillmentID: "f-1",
		TargetID:      "t-1",
		Manifests:     []Manifest{{ManifestType: "k8s", ManifestID: "app", Raw: json.RawMessage(`{}`)}},
		Generation:    5,
		State:         DeliveryStatePending,
		CreatedAt:     refTime,
		UpdatedAt:     refTime.Add(time.Second),
	}

	d := DeliveryFromSnapshot(snap)
	got := d.Snapshot()

	assertEq(t, "ID", got.ID, snap.ID)
	assertEq(t, "FulfillmentID", got.FulfillmentID, snap.FulfillmentID)
	assertEq(t, "TargetID", got.TargetID, snap.TargetID)
	assertEq(t, "len(Manifests)", len(got.Manifests), len(snap.Manifests))
	assertEq(t, "Manifests[0].ManifestType", got.Manifests[0].ManifestType, snap.Manifests[0].ManifestType)
	assertEq(t, "Generation", got.Generation, snap.Generation)
	assertEq(t, "State", got.State, snap.State)
	assertEq(t, "CreatedAt", got.CreatedAt, snap.CreatedAt)
	assertEq(t, "UpdatedAt", got.UpdatedAt, snap.UpdatedAt)
}

func TestTargetInfoSnapshot_RoundTrip(t *testing.T) {
	snap := TargetInfoSnapshot{
		ID:                    "t-1",
		InventoryItemID:       "inv-1",
		Type:                  "kubernetes",
		Name:                  "prod-east",
		State:                 TargetStateReady,
		Labels:                map[string]string{"env": "prod"},
		Properties:            map[string]string{"kubeconfig": "ref://vault/kc"},
		AcceptedManifestTypes: []ManifestType{"k8s"},
	}

	ti := TargetInfoFromSnapshot(snap)
	got := ti.Snapshot()

	assertEq(t, "ID", got.ID, snap.ID)
	assertEq(t, "InventoryItemID", got.InventoryItemID, snap.InventoryItemID)
	assertEq(t, "Type", got.Type, snap.Type)
	assertEq(t, "Name", got.Name, snap.Name)
	assertEq(t, "State", got.State, snap.State)
	assertEq(t, "Labels[env]", got.Labels["env"], snap.Labels["env"])
	assertEq(t, "Properties[kubeconfig]", got.Properties["kubeconfig"], snap.Properties["kubeconfig"])
	assertEq(t, "len(AcceptedManifestTypes)", len(got.AcceptedManifestTypes), len(snap.AcceptedManifestTypes))
}

func TestInventoryItemSnapshot_RoundTrip(t *testing.T) {
	did := DeliveryID("del-1")
	snap := InventoryItemSnapshot{
		ID:               "inv-1",
		Type:             "kind.cluster",
		Name:             "dev-cluster",
		Properties:       json.RawMessage(`{"version":"1.29"}`),
		Labels:           map[string]string{"tier": "dev"},
		SourceDeliveryID: &did,
		CreatedAt:        refTime,
		UpdatedAt:        refTime.Add(time.Hour),
	}

	item := InventoryItemFromSnapshot(snap)
	got := item.Snapshot()

	assertEq(t, "ID", got.ID, snap.ID)
	assertEq(t, "Type", got.Type, snap.Type)
	assertEq(t, "Name", got.Name, snap.Name)
	assertEq(t, "string(Properties)", string(got.Properties), string(snap.Properties))
	assertEq(t, "Labels[tier]", got.Labels["tier"], snap.Labels["tier"])
	if got.SourceDeliveryID == nil {
		t.Fatal("SourceDeliveryID is nil, want non-nil")
	}
	assertEq(t, "*SourceDeliveryID", *got.SourceDeliveryID, *snap.SourceDeliveryID)
	assertEq(t, "CreatedAt", got.CreatedAt, snap.CreatedAt)
	assertEq(t, "UpdatedAt", got.UpdatedAt, snap.UpdatedAt)
}

func TestAuthMethodSnapshot_RoundTrip(t *testing.T) {
	snap := AuthMethodSnapshot{
		ID:   "auth-1",
		Type: AuthMethodTypeOIDC,
		OIDC: &OIDCConfig{
			IssuerURL: "https://accounts.example.com",
			Audience:  "fleetshift",
		},
	}

	m := AuthMethodFromSnapshot(snap)
	got := m.Snapshot()

	assertEq(t, "ID", got.ID, snap.ID)
	assertEq(t, "Type", got.Type, snap.Type)
	if got.OIDC == nil {
		t.Fatal("OIDC is nil, want non-nil")
	}
	assertEq(t, "OIDC.IssuerURL", got.OIDC.IssuerURL, snap.OIDC.IssuerURL)
	assertEq(t, "OIDC.Audience", got.OIDC.Audience, snap.OIDC.Audience)
}

func TestSignerEnrollmentSnapshot_RoundTrip(t *testing.T) {
	snap := SignerEnrollmentSnapshot{
		ID:                "se-1",
		FederatedIdentity: FederatedIdentity{Subject: "user@example.com", Issuer: "https://issuer.example.com"},
		IdentityToken:     "eyJhbGciOiJSUzI1NiJ9...",
		RegistrySubject:   "ghuser",
		RegistryID:        "github.com",
		CreatedAt:         refTime,
		ExpiresAt:         refTime.Add(24 * time.Hour),
	}

	e := SignerEnrollmentFromSnapshot(snap)
	got := e.Snapshot()

	assertEq(t, "ID", got.ID, snap.ID)
	assertEq(t, "Subject", got.Subject, snap.Subject)
	assertEq(t, "Issuer", got.Issuer, snap.Issuer)
	assertEq(t, "IdentityToken", got.IdentityToken, snap.IdentityToken)
	assertEq(t, "RegistrySubject", got.RegistrySubject, snap.RegistrySubject)
	assertEq(t, "RegistryID", got.RegistryID, snap.RegistryID)
	assertEq(t, "CreatedAt", got.CreatedAt, snap.CreatedAt)
	assertEq(t, "ExpiresAt", got.ExpiresAt, snap.ExpiresAt)
}

func TestFulfillmentSnapshot_CapturesPendingBuffers(t *testing.T) {
	f := FulfillmentFromSnapshot(FulfillmentSnapshot{
		ID:         "f-1",
		State:      FulfillmentStateActive,
		Generation: 1,
	})

	now := refTime
	f.AdvanceManifestStrategy(ManifestStrategySpec{Type: ManifestStrategyInline}, now)
	f.AdvancePlacementStrategy(PlacementStrategySpec{Type: PlacementStrategyAll}, now)

	snap := f.Snapshot()

	if len(snap.PendingStrategyRecords.Manifest) != 1 {
		t.Errorf("PendingStrategyRecords.Manifest len = %d, want 1",
			len(snap.PendingStrategyRecords.Manifest))
	}
	if len(snap.PendingStrategyRecords.Placement) != 1 {
		t.Errorf("PendingStrategyRecords.Placement len = %d, want 1",
			len(snap.PendingStrategyRecords.Placement))
	}
}

func TestFulfillment_DrainPendingStrategyRecords(t *testing.T) {
	f := FulfillmentFromSnapshot(FulfillmentSnapshot{
		ID:         "f-1",
		State:      FulfillmentStateActive,
		Generation: 1,
	})

	now := refTime
	f.AdvanceManifestStrategy(ManifestStrategySpec{Type: ManifestStrategyInline}, now)
	f.AdvancePlacementStrategy(PlacementStrategySpec{Type: PlacementStrategyAll}, now)

	// Snapshot still captures them (non-mutating).
	snap := f.Snapshot()
	if len(snap.PendingStrategyRecords.Manifest) != 1 {
		t.Fatalf("Snapshot().Manifest len = %d, want 1", len(snap.PendingStrategyRecords.Manifest))
	}
	if len(snap.PendingStrategyRecords.Placement) != 1 {
		t.Fatalf("Snapshot().Placement len = %d, want 1", len(snap.PendingStrategyRecords.Placement))
	}

	// Drain returns the records and clears the buffers.
	drained := f.DrainPendingStrategyRecords()
	if len(drained.Manifest) != 1 {
		t.Fatalf("drained Manifest len = %d, want 1", len(drained.Manifest))
	}
	if len(drained.Placement) != 1 {
		t.Fatalf("drained Placement len = %d, want 1", len(drained.Placement))
	}

	// After drain, both Snapshot and a second drain return empty.
	snap2 := f.Snapshot()
	if len(snap2.PendingStrategyRecords.Manifest) != 0 {
		t.Errorf("post-drain Snapshot().Manifest len = %d, want 0", len(snap2.PendingStrategyRecords.Manifest))
	}
	if len(snap2.PendingStrategyRecords.Placement) != 0 {
		t.Errorf("post-drain Snapshot().Placement len = %d, want 0", len(snap2.PendingStrategyRecords.Placement))
	}
	drained2 := f.DrainPendingStrategyRecords()
	if len(drained2.Manifest) != 0 {
		t.Errorf("second drain Manifest len = %d, want 0", len(drained2.Manifest))
	}
}

func TestPlatformResourceSnapshot_RoundTrip(t *testing.T) {
	uid := NewPlatformResourceUID()
	targetUID := NewPlatformResourceUID()
	snap := PlatformResourceSnapshot{
		UID:       uid,
		Name:      "clusters/prod",
		Labels:    map[string]string{"env": "prod"},
		CreatedAt: refTime,
		UpdatedAt: refTime.Add(time.Hour),
		Representations: []ResourceRepresentationSnapshot{
			{
				PlatformUID: uid,
				ServiceName: "kind.fleetshift.io",
				Version:     "v1",
				Name:        "clusters/prod",
				Roles:       []RepresentationRole{RepresentationRoleManaged},
				Labels:      map[string]string{"runtime": "containerd"},
				CreatedAt:   refTime,
				UpdatedAt:   refTime,
			},
			{
				PlatformUID: uid,
				ServiceName: "gcp.fleetshift.io",
				Version:     "v1",
				Name:        "clusters/prod",
				Roles:       []RepresentationRole{RepresentationRoleInventory},
				Labels:      map[string]string{"project": "my-proj"},
				CreatedAt:   refTime,
				UpdatedAt:   refTime,
				Deleted:     true,
			},
		},
		Aliases: []ResourceAliasSnapshot{
			{Namespace: "gcp", Key: "project_id", Value: "my-proj"},
		},
		Relationships: []ResourceRelationshipSnapshot{
			{SourceUID: uid, Type: "runs-on", TargetUID: targetUID, SourceService: "kind.fleetshift.io", CreatedAt: refTime},
		},
	}

	r := PlatformResourceFromSnapshot(snap)
	got := r.Snapshot()

	assertEq(t, "UID", got.UID, snap.UID)
	assertEq(t, "Name", got.Name, snap.Name)
	assertEq(t, "Labels[env]", got.Labels["env"], snap.Labels["env"])
	assertEq(t, "CreatedAt", got.CreatedAt, snap.CreatedAt)
	assertEq(t, "UpdatedAt", got.UpdatedAt, snap.UpdatedAt)

	if len(got.Representations) != 2 {
		t.Fatalf("Representations len = %d, want 2", len(got.Representations))
	}
	assertEq(t, "Rep[0].ServiceName", got.Representations[0].ServiceName, ServiceName("kind.fleetshift.io"))
	assertEq(t, "Rep[1].ServiceName", got.Representations[1].ServiceName, ServiceName("gcp.fleetshift.io"))
	if !got.Representations[1].Deleted {
		t.Fatal("Rep[1].Deleted is false, want true")
	}

	reps := r.Representations()
	if len(reps) != 1 {
		t.Fatalf("Representations() len = %d, want 1", len(reps))
	}

	if len(got.Aliases) != 1 {
		t.Fatalf("Aliases len = %d, want 1", len(got.Aliases))
	}
	assertEq(t, "Alias.Namespace", got.Aliases[0].Namespace, AliasNamespace("gcp"))

	if len(got.Relationships) != 1 {
		t.Fatalf("Relationships len = %d, want 1", len(got.Relationships))
	}
	assertEq(t, "Rel.Type", got.Relationships[0].Type, RelationshipType("runs-on"))
}

// ---------------------------------------------------------------------------
// ExtensionResourceType snapshot round-trips
// ---------------------------------------------------------------------------

func TestExtensionResourceTypeSnapshot_RoundTrip(t *testing.T) {
	relation := NewRegisteredSelfTarget("target-kind", "managed-resource")
	sig := Signature{Signer: FederatedIdentity{Subject: "addon", Issuer: "iss"}}

	snap := ExtensionResourceTypeSnapshot{
		ResourceType: "kind.fleetshift.io/Cluster",
		APIVersion:   "v1",
		CollectionID: "clusters",
		Management: &ManagementTypeSnapshot{
			Relation:  relation,
			Signature: sig,
		},
		CreatedAt: refTime,
		UpdatedAt: refTime.Add(time.Hour),
	}

	ert := ExtensionResourceTypeFromSnapshot(snap)
	got := ert.Snapshot()

	assertEq(t, "ResourceType", got.ResourceType, snap.ResourceType)
	assertEq(t, "APIVersion", got.APIVersion, snap.APIVersion)
	assertEq(t, "CollectionID", got.CollectionID, snap.CollectionID)
	assertEq(t, "CreatedAt", got.CreatedAt, snap.CreatedAt)
	assertEq(t, "UpdatedAt", got.UpdatedAt, snap.UpdatedAt)
	if got.Management == nil {
		t.Fatal("Management is nil after round-trip")
	}
	assertEq(t, "Signature.Signer.Subject", got.Management.Signature.Signer.Subject, "addon")
	rst, ok := got.Management.Relation.(RegisteredSelfTarget)
	if !ok {
		t.Fatal("expected RegisteredSelfTarget after round-trip")
	}
	assertEq(t, "Relation.AddonTarget", rst.AddonTarget(), TargetID("target-kind"))
}

func TestExtensionResourceTypeSnapshot_RoundTrip_NoManagement(t *testing.T) {
	snap := ExtensionResourceTypeSnapshot{
		ResourceType: "kind.fleetshift.io/Cluster",
		APIVersion:   "v1",
		CollectionID: "clusters",
		CreatedAt:    refTime,
		UpdatedAt:    refTime,
	}

	ert := ExtensionResourceTypeFromSnapshot(snap)
	got := ert.Snapshot()

	assertEq(t, "ResourceType", got.ResourceType, snap.ResourceType)
	if got.Management != nil {
		t.Error("expected nil Management after round-trip")
	}
}

// ---------------------------------------------------------------------------
// ExtensionResource snapshot round-trips
// ---------------------------------------------------------------------------

func TestExtensionResourceSnapshot_RoundTrip(t *testing.T) {
	uid := NewExtensionResourceUID()
	snap := ExtensionResourceSnapshot{
		UID:          uid,
		ResourceType: "kind.fleetshift.io/Cluster",
		Name:         "clusters/dev",
		Labels:       map[string]string{"env": "dev"},
		CreatedAt:    refTime,
		UpdatedAt:    refTime.Add(time.Hour),
		PendingIntents: []ResourceIntent{
			{ResourceType: "kind.fleetshift.io/Cluster", Name: "clusters/dev", Version: 1, Spec: json.RawMessage(`{}`)},
		},
	}

	r := ExtensionResourceFromSnapshot(snap)
	got := r.Snapshot()

	assertEq(t, "UID", got.UID, snap.UID)
	assertEq(t, "ResourceType", got.ResourceType, snap.ResourceType)
	assertEq(t, "Name", got.Name, snap.Name)
	assertEq(t, "Labels[env]", got.Labels["env"], "dev")
	assertEq(t, "CreatedAt", got.CreatedAt, snap.CreatedAt)
	assertEq(t, "UpdatedAt", got.UpdatedAt, snap.UpdatedAt)
	if got.Managed != nil {
		t.Error("expected nil Managed for resource without managed state")
	}

	// Pending intents must be zeroed on hydration round-trip.
	if len(got.PendingIntents) != 0 {
		t.Errorf("PendingIntents len = %d, want 0 after round-trip", len(got.PendingIntents))
	}
}

func TestExtensionResourceSnapshot_ManagedState_RoundTrip(t *testing.T) {
	uid := NewExtensionResourceUID()
	snap := ExtensionResourceSnapshot{
		UID:          uid,
		ResourceType: "kind.fleetshift.io/Cluster",
		Name:         "clusters/prod",
		Labels:       map[string]string{},
		Managed: &ManagedStateSnapshot{
			CurrentVersion: 5,
			FulfillmentID:  "f-1",
		},
		CreatedAt: refTime,
		UpdatedAt: refTime.Add(time.Hour),
	}

	r := ExtensionResourceFromSnapshot(snap)
	got := r.Snapshot()

	if got.Managed == nil {
		t.Fatal("Managed is nil after round-trip")
	}
	assertEq(t, "Managed.CurrentVersion", got.Managed.CurrentVersion, IntentVersion(5))
	assertEq(t, "Managed.FulfillmentID", got.Managed.FulfillmentID, FulfillmentID("f-1"))
}

func TestExtensionResourceFromSnapshot_PreservesVersionBaseline(t *testing.T) {
	r := ExtensionResourceFromSnapshot(ExtensionResourceSnapshot{
		UID:          NewExtensionResourceUID(),
		ResourceType: "kind.fleetshift.io/Cluster",
		Name:         "clusters/prod",
		Managed: &ManagedStateSnapshot{
			CurrentVersion: 7,
			FulfillmentID:  "f-1",
		},
	})

	intent, err := r.RecordIntent(json.RawMessage(`{"v":8}`), refTime)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertEq(t, "intent.Version", intent.Version, IntentVersion(8))
	assertEq(t, "managed.CurrentVersion after RecordIntent", r.Managed().CurrentVersion(), IntentVersion(8))
}

func TestExtensionResourceSnapshot_CapturesPendingIntents(t *testing.T) {
	r := ExtensionResourceFromSnapshot(ExtensionResourceSnapshot{
		UID:          NewExtensionResourceUID(),
		ResourceType: "kind.fleetshift.io/Cluster",
		Name:         "clusters/prod",
		Managed: &ManagedStateSnapshot{
			CurrentVersion: 0,
			FulfillmentID:  "f-1",
		},
	})

	r.RecordIntent(json.RawMessage(`{"spec":"v1"}`), refTime)

	snap := r.Snapshot()
	if len(snap.PendingIntents) != 1 {
		t.Fatalf("PendingIntents len = %d, want 1", len(snap.PendingIntents))
	}
	assertEq(t, "PendingIntents[0].Version", snap.PendingIntents[0].Version, IntentVersion(1))
}

// assertEq is a generic test helper that compares two comparable values.
func assertEq[T comparable](t *testing.T, field string, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %v, want %v", field, got, want)
	}
}

func ptrTo[T any](v T) *T { return &v }
