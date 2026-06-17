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
		AttestationRef:           &AttestationRef{RelationRef: ptrTo(ResourceType("k8s"))},
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
	snap := DeploymentSnapshot{
		ID:            "d-1",
		UID:           "uid-abc",
		FulfillmentID: "f-1",
		CreatedAt:     refTime,
		UpdatedAt:     refTime.Add(time.Minute),
	}

	d := DeploymentFromSnapshot(snap)
	got := d.Snapshot()

	assertEq(t, "ID", got.ID, snap.ID)
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
		Manifests:     []Manifest{{ResourceType: "k8s", Name: "app", Raw: json.RawMessage(`{}`)}},
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
	assertEq(t, "Manifests[0].ResourceType", got.Manifests[0].ResourceType, snap.Manifests[0].ResourceType)
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
		AcceptedResourceTypes: []ResourceType{"k8s"},
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
	assertEq(t, "len(AcceptedResourceTypes)", len(got.AcceptedResourceTypes), len(snap.AcceptedResourceTypes))
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

func TestManagedResourceSnapshot_RoundTrip(t *testing.T) {
	deletedAt := refTime.Add(2 * time.Hour)
	snap := ManagedResourceSnapshot{
		ResourceType:   "clusters",
		Name:           "prod-east",
		UID:            "uid-mr",
		CurrentVersion: 4,
		FulfillmentID:  "f-1",
		CreatedAt:      refTime,
		UpdatedAt:      refTime.Add(time.Hour),
		DeletedAt:      &deletedAt,
		PendingIntents: []ResourceIntent{
			{ResourceType: "clusters", Name: "prod-east", Version: 5, Spec: json.RawMessage(`{}`)},
		},
	}

	mr := ManagedResourceFromSnapshot(snap)
	got := mr.Snapshot()

	assertEq(t, "ResourceType", got.ResourceType, snap.ResourceType)
	assertEq(t, "Name", got.Name, snap.Name)
	assertEq(t, "UID", got.UID, snap.UID)
	assertEq(t, "CurrentVersion", got.CurrentVersion, snap.CurrentVersion)
	assertEq(t, "FulfillmentID", got.FulfillmentID, snap.FulfillmentID)
	assertEq(t, "CreatedAt", got.CreatedAt, snap.CreatedAt)
	assertEq(t, "UpdatedAt", got.UpdatedAt, snap.UpdatedAt)
	if got.DeletedAt == nil {
		t.Fatal("DeletedAt is nil, want non-nil")
	}
	assertEq(t, "DeletedAt", *got.DeletedAt, *snap.DeletedAt)

	// Pending intents must be zeroed on hydration round-trip.
	if len(got.PendingIntents) != 0 {
		t.Errorf("PendingIntents len = %d, want 0 after round-trip", len(got.PendingIntents))
	}
}

func TestManagedResourceFromSnapshot_PreservesVersionBaseline(t *testing.T) {
	mr := ManagedResourceFromSnapshot(ManagedResourceSnapshot{
		ResourceType:   "clusters",
		Name:           "prod",
		CurrentVersion: 7,
		FulfillmentID:  "f-1",
	})

	intent := mr.RecordIntent(json.RawMessage(`{"v":8}`), refTime)
	assertEq(t, "intent.Version", intent.Version, IntentVersion(8))
	assertEq(t, "mr.CurrentVersion after RecordIntent", mr.CurrentVersion(), IntentVersion(8))
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

func TestManagedResourceSnapshot_CapturesPendingIntents(t *testing.T) {
	mr := ManagedResourceFromSnapshot(ManagedResourceSnapshot{
		ResourceType:   "clusters",
		Name:           "prod",
		CurrentVersion: 0,
		FulfillmentID:  "f-1",
	})

	mr.RecordIntent(json.RawMessage(`{"spec":"v1"}`), refTime)

	snap := mr.Snapshot()
	if len(snap.PendingIntents) != 1 {
		t.Fatalf("PendingIntents len = %d, want 1", len(snap.PendingIntents))
	}
	assertEq(t, "PendingIntents[0].Version", snap.PendingIntents[0].Version, IntentVersion(1))
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

func TestManagedResource_DrainPendingIntents(t *testing.T) {
	mr := ManagedResourceFromSnapshot(ManagedResourceSnapshot{
		ResourceType:   "clusters",
		Name:           "prod",
		CurrentVersion: 0,
		FulfillmentID:  "f-1",
	})

	mr.RecordIntent(json.RawMessage(`{"spec":"v1"}`), refTime)

	// Snapshot still captures them (non-mutating).
	snap := mr.Snapshot()
	if len(snap.PendingIntents) != 1 {
		t.Fatalf("Snapshot().PendingIntents len = %d, want 1", len(snap.PendingIntents))
	}

	// Drain returns the intents and clears the buffer.
	drained := mr.DrainPendingIntents()
	if len(drained) != 1 {
		t.Fatalf("drained intents len = %d, want 1", len(drained))
	}
	assertEq(t, "drained[0].Version", drained[0].Version, IntentVersion(1))

	// After drain, both Snapshot and a second drain return empty.
	snap2 := mr.Snapshot()
	if len(snap2.PendingIntents) != 0 {
		t.Errorf("post-drain Snapshot().PendingIntents len = %d, want 0", len(snap2.PendingIntents))
	}
	drained2 := mr.DrainPendingIntents()
	if len(drained2) != 0 {
		t.Errorf("second drain intents len = %d, want 0", len(drained2))
	}
}

func TestPlatformResourceSnapshot_RoundTrip(t *testing.T) {
	deletedAt := refTime.Add(2 * time.Hour)
	repDeletedAt := refTime.Add(3 * time.Hour)
	snap := PlatformResourceSnapshot{
		UID:          "plat-1",
		CollectionID: "clusters",
		RelativeName: "clusters/prod",
		Labels:       map[string]string{"env": "prod"},
		CreatedAt:    refTime,
		UpdatedAt:    refTime.Add(time.Hour),
		DeletedAt:    &deletedAt,
		Representations: []ResourceRepresentationSnapshot{
			{
				PlatformUID:  "plat-1",
				ServiceName:  "kind.fleetshift.io",
				Version:      "v1",
				CollectionID: "clusters",
				RelativeName: "clusters/prod",
				Roles:        []RepresentationRole{RepresentationRoleManaged},
				Labels:       map[string]string{"runtime": "containerd"},
				CreatedAt:    refTime,
				UpdatedAt:    refTime,
			},
			{
				PlatformUID:  "plat-1",
				ServiceName:  "gcp.fleetshift.io",
				Version:      "v1",
				CollectionID: "clusters",
				RelativeName: "clusters/prod",
				Roles:        []RepresentationRole{RepresentationRoleInventory},
				Labels:       map[string]string{"project": "my-proj"},
				CreatedAt:    refTime,
				UpdatedAt:    refTime,
				DeletedAt:    &repDeletedAt,
			},
		},
		Aliases: []ResourceAliasSnapshot{
			{Namespace: "gcp", Key: "project_id", Value: "my-proj"},
		},
		Relationships: []ResourceRelationshipSnapshot{
			{SourceUID: "plat-1", Type: "runs-on", TargetUID: "plat-2", SourceService: "kind.fleetshift.io", CreatedAt: refTime},
		},
	}

	r := PlatformResourceFromSnapshot(snap)
	got := r.Snapshot()

	assertEq(t, "UID", got.UID, snap.UID)
	assertEq(t, "CollectionID", got.CollectionID, snap.CollectionID)
	assertEq(t, "RelativeName", got.RelativeName, snap.RelativeName)
	assertEq(t, "Labels[env]", got.Labels["env"], snap.Labels["env"])
	assertEq(t, "CreatedAt", got.CreatedAt, snap.CreatedAt)
	assertEq(t, "UpdatedAt", got.UpdatedAt, snap.UpdatedAt)
	if got.DeletedAt == nil {
		t.Fatal("DeletedAt is nil, want non-nil")
	}
	assertEq(t, "DeletedAt", *got.DeletedAt, *snap.DeletedAt)

	if len(got.Representations) != 2 {
		t.Fatalf("Representations len = %d, want 2", len(got.Representations))
	}
	assertEq(t, "Rep[0].ServiceName", got.Representations[0].ServiceName, ServiceName("kind.fleetshift.io"))
	assertEq(t, "Rep[1].ServiceName", got.Representations[1].ServiceName, ServiceName("gcp.fleetshift.io"))
	if got.Representations[1].DeletedAt == nil {
		t.Fatal("Rep[1].DeletedAt is nil, want non-nil")
	}

	// Verify active-only accessor filters tombstoned representations.
	activeReps := r.Representations()
	if len(activeReps) != 1 {
		t.Fatalf("active Representations() len = %d, want 1", len(activeReps))
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

func TestPlatformResourceSnapshot_RoundTrip_NilDeletedAt(t *testing.T) {
	snap := PlatformResourceSnapshot{
		UID:          "plat-2",
		CollectionID: "clusters",
		RelativeName: "clusters/staging",
		Labels:       map[string]string{},
		CreatedAt:    refTime,
		UpdatedAt:    refTime,
	}

	r := PlatformResourceFromSnapshot(snap)
	got := r.Snapshot()

	assertEq(t, "UID", got.UID, snap.UID)
	if got.DeletedAt != nil {
		t.Errorf("DeletedAt = %v, want nil", got.DeletedAt)
	}
}

// assertEq is a generic test helper that compares two comparable values.
func assertEq[T comparable](t *testing.T, field string, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %v, want %v", field, got, want)
	}
}

func ptrTo[T any](v T) *T { return &v }
