package domain

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func fulfillmentValue(s FulfillmentSnapshot) Fulfillment {
	return *FulfillmentFromSnapshot(s)
}

func TestDeploymentView_Etag_Deterministic(t *testing.T) {
	depUID := NewDeploymentUID()
	v := DeploymentView{
		Deployment: DeploymentFromSnapshot(DeploymentSnapshot{
			Name: "deployments/dep-1",
			UID:  depUID,
		}),
		Fulfillment: fulfillmentValue(FulfillmentSnapshot{
			ID:         "f-1",
			Generation: 3,
			State:      FulfillmentStateActive,
			ManifestStrategy: ManifestStrategySpec{
				Type: ManifestStrategyInline,
			},
			PlacementStrategy: PlacementStrategySpec{
				Type: PlacementStrategyAll,
			},
			ResolvedTargets: []TargetID{"t1", "t2"},
		}),
	}

	e1 := v.Etag()
	e2 := v.Etag()
	if e1 != e2 {
		t.Errorf("etag is not deterministic: %q != %q", e1, e2)
	}
}

func TestDeploymentView_Etag_WeakPrefix(t *testing.T) {
	v := DeploymentView{
		Deployment:  DeploymentFromSnapshot(DeploymentSnapshot{Name: "deployments/dep-1", UID: NewDeploymentUID()}),
		Fulfillment: fulfillmentValue(FulfillmentSnapshot{ID: "f-1", Generation: 1, State: FulfillmentStateCreating}),
	}
	etag := string(v.Etag())
	if !strings.HasPrefix(etag, `W/"`) {
		t.Errorf("etag should start with W/\", got %q", etag)
	}
	if !strings.HasSuffix(etag, `"`) {
		t.Errorf("etag should end with \", got %q", etag)
	}
}

func TestDeploymentView_Etag_ChangesOnStateChange(t *testing.T) {
	base := DeploymentView{
		Deployment: DeploymentFromSnapshot(DeploymentSnapshot{Name: "deployments/dep-1", UID: NewDeploymentUID()}),
		Fulfillment: fulfillmentValue(FulfillmentSnapshot{
			ID:              "f-1",
			Generation:      3,
			State:           FulfillmentStateActive,
			ResolvedTargets: []TargetID{"t1"},
		}),
	}
	baseEtag := base.Etag()

	t.Run("state change", func(t *testing.T) {
		snap := base.Fulfillment.Snapshot()
		snap.State = FulfillmentStateFailed
		v := base
		v.Fulfillment = fulfillmentValue(snap)
		if v.Etag() == baseEtag {
			t.Error("etag should change when state changes")
		}
	})

	t.Run("generation change", func(t *testing.T) {
		snap := base.Fulfillment.Snapshot()
		snap.Generation = 4
		v := base
		v.Fulfillment = fulfillmentValue(snap)
		if v.Etag() == baseEtag {
			t.Error("etag should change when generation changes")
		}
	})

	t.Run("resolved targets change", func(t *testing.T) {
		snap := base.Fulfillment.Snapshot()
		snap.ResolvedTargets = []TargetID{"t1", "t2"}
		v := base
		v.Fulfillment = fulfillmentValue(snap)
		if v.Etag() == baseEtag {
			t.Error("etag should change when resolved targets change")
		}
	})
}

func TestDeploymentView_Etag_FieldBoundariesAreUnambiguous(t *testing.T) {
	sharedUID := NewDeploymentUID()
	a := DeploymentView{
		Deployment:  DeploymentFromSnapshot(DeploymentSnapshot{Name: "deployments/ab", UID: sharedUID}),
		Fulfillment: fulfillmentValue(FulfillmentSnapshot{Generation: 1, State: FulfillmentStateActive}),
	}
	b := DeploymentView{
		Deployment:  DeploymentFromSnapshot(DeploymentSnapshot{Name: "deployments/a", UID: sharedUID}),
		Fulfillment: fulfillmentValue(FulfillmentSnapshot{Generation: 1, State: FulfillmentStateActive}),
	}
	if a.Etag() == b.Etag() {
		t.Error("etags must differ when field values differ, even if concatenation is the same")
	}
}

func TestFulfillment_Etag_ResolvedTargetBoundariesAreUnambiguous(t *testing.T) {
	// Two views whose ResolvedTargets concatenate to the same bytes:
	// ["ab","c"] vs ["a","bc"]. They must produce distinct etags.
	sharedUID := NewDeploymentUID()
	a := DeploymentView{
		Deployment: DeploymentFromSnapshot(DeploymentSnapshot{Name: "deployments/d", UID: sharedUID}),
		Fulfillment: fulfillmentValue(FulfillmentSnapshot{
			Generation:      1,
			State:           FulfillmentStateActive,
			ResolvedTargets: []TargetID{"ab", "c"},
		}),
	}
	b := DeploymentView{
		Deployment: DeploymentFromSnapshot(DeploymentSnapshot{Name: "deployments/d", UID: sharedUID}),
		Fulfillment: fulfillmentValue(FulfillmentSnapshot{
			Generation:      1,
			State:           FulfillmentStateActive,
			ResolvedTargets: []TargetID{"a", "bc"},
		}),
	}
	if a.Etag() == b.Etag() {
		t.Error("etags must differ when resolved target boundaries differ")
	}
}

func TestFulfillment_Etag_ResolvedTargetCountMatters(t *testing.T) {
	// ["abc"] vs ["ab","c"] — same concatenated bytes but different
	// slice lengths. Must produce distinct etags.
	sharedUID2 := NewDeploymentUID()
	a := DeploymentView{
		Deployment: DeploymentFromSnapshot(DeploymentSnapshot{Name: "deployments/d", UID: sharedUID2}),
		Fulfillment: fulfillmentValue(FulfillmentSnapshot{
			Generation:      1,
			State:           FulfillmentStateActive,
			ResolvedTargets: []TargetID{"abc"},
		}),
	}
	b := DeploymentView{
		Deployment: DeploymentFromSnapshot(DeploymentSnapshot{Name: "deployments/d", UID: sharedUID2}),
		Fulfillment: fulfillmentValue(FulfillmentSnapshot{
			Generation:      1,
			State:           FulfillmentStateActive,
			ResolvedTargets: []TargetID{"ab", "c"},
		}),
	}
	if a.Etag() == b.Etag() {
		t.Error("etags must differ when resolved target count differs")
	}
}

// ---------------------------------------------------------------------------
// ExtensionResourceView etag tests
// ---------------------------------------------------------------------------

// extensionView builds a managed ExtensionResourceView with sensible
// defaults suitable for etag testing. Callers mutate the returned value
// before computing the etag.
func extensionView() ExtensionResourceView {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	uid := NewExtensionResourceUID()
	er := NewExtensionResource(uid, "kind.fleetshift.io/Cluster", "clusters/dev", now,
		WithManagedState("f-1"))
	er.RecordIntent(json.RawMessage(`{"replicas":3}`), now)

	f := FulfillmentFromSnapshot(FulfillmentSnapshot{
		ID:              "f-1",
		Generation:      1,
		State:           FulfillmentStateActive,
		ResolvedTargets: []TargetID{"t1"},
	})

	intent := er.Snapshot().PendingIntents[0]

	return ExtensionResourceView{
		Resource:    *er,
		Intent:      &intent,
		Fulfillment: f,
	}
}

func TestExtensionResourceView_Etag_Deterministic(t *testing.T) {
	v := extensionView()
	e1 := v.Etag()
	e2 := v.Etag()
	if e1 != e2 {
		t.Errorf("etag is not deterministic: %q != %q", e1, e2)
	}
}

func TestExtensionResourceView_Etag_WeakPrefix(t *testing.T) {
	etag := string(extensionView().Etag())
	if !strings.HasPrefix(etag, `W/"`) {
		t.Errorf("etag should start with W/\", got %q", etag)
	}
	if !strings.HasSuffix(etag, `"`) {
		t.Errorf("etag should end with \", got %q", etag)
	}
}

func TestExtensionResourceView_Etag_ChangesOnMutation(t *testing.T) {
	base := extensionView()
	baseEtag := base.Etag()

	t.Run("fulfillment state change", func(t *testing.T) {
		snap := base.Fulfillment.Snapshot()
		snap.State = FulfillmentStateFailed
		v := base
		v.Fulfillment = FulfillmentFromSnapshot(snap)
		if v.Etag() == baseEtag {
			t.Error("etag should change when fulfillment state changes")
		}
	})

	t.Run("fulfillment generation change", func(t *testing.T) {
		snap := base.Fulfillment.Snapshot()
		snap.Generation = 99
		v := base
		v.Fulfillment = FulfillmentFromSnapshot(snap)
		if v.Etag() == baseEtag {
			t.Error("etag should change when fulfillment generation changes")
		}
	})

	t.Run("intent version change", func(t *testing.T) {
		v := base
		changed := *base.Intent
		changed.Version = 42
		v.Intent = &changed
		if v.Etag() == baseEtag {
			t.Error("etag should change when intent version changes")
		}
	})

	t.Run("intent spec change", func(t *testing.T) {
		v := base
		changed := *base.Intent
		changed.Spec = json.RawMessage(`{"replicas":5}`)
		v.Intent = &changed
		if v.Etag() == baseEtag {
			t.Error("etag should change when intent spec changes")
		}
	})

	t.Run("managed version change", func(t *testing.T) {
		now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
		er := ExtensionResourceFromSnapshot(ExtensionResourceSnapshot{
			UID:          base.Resource.UID(),
			ResourceType: base.Resource.ResourceType(),
			Name:         base.Resource.Name(),
			Managed: &ManagedStateSnapshot{
				CurrentVersion: 10,
				FulfillmentID:  "f-1",
			},
			CreatedAt: now,
			UpdatedAt: now,
		})
		v := base
		v.Resource = *er
		if v.Etag() == baseEtag {
			t.Error("etag should change when managed version changes")
		}
	})

	t.Run("labels change", func(t *testing.T) {
		now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
		er := ExtensionResourceFromSnapshot(ExtensionResourceSnapshot{
			UID:          base.Resource.UID(),
			ResourceType: base.Resource.ResourceType(),
			Name:         base.Resource.Name(),
			Labels:       map[string]string{"env": "prod"},
			Managed: &ManagedStateSnapshot{
				CurrentVersion: 1,
				FulfillmentID:  "f-1",
			},
			CreatedAt: now,
			UpdatedAt: now,
		})
		v := base
		v.Resource = *er
		if v.Etag() == baseEtag {
			t.Error("etag should change when labels change")
		}
	})

	t.Run("resolved targets change", func(t *testing.T) {
		snap := base.Fulfillment.Snapshot()
		snap.ResolvedTargets = []TargetID{"t1", "t2", "t3"}
		v := base
		v.Fulfillment = FulfillmentFromSnapshot(snap)
		if v.Etag() == baseEtag {
			t.Error("etag should change when resolved targets change")
		}
	})
}

func TestExtensionResourceView_Etag_FieldBoundariesAreUnambiguous(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	sharedUID := NewExtensionResourceUID()

	// ResourceType and Name are adjacent variable-length fields in the
	// hash. These two pairs have identical raw concatenations
	// ("svc.io/Foobars/x") but different field boundaries, so they
	// would collide if hashString stopped length-prefixing.
	a := ExtensionResourceView{
		Resource: *NewExtensionResource(sharedUID, "svc.io/Foo", "bars/x", now,
			WithManagedState("f-1")),
	}
	b := ExtensionResourceView{
		Resource: *NewExtensionResource(sharedUID, "svc.io/Foobar", "s/x", now,
			WithManagedState("f-1")),
	}
	if a.Etag() == b.Etag() {
		t.Error("etags must differ when field values differ, even if concatenation is the same")
	}
}

func TestExtensionResourceView_Etag_NilFulfillmentDiffers(t *testing.T) {
	withFulfillment := extensionView()
	without := withFulfillment
	without.Fulfillment = nil
	if withFulfillment.Etag() == without.Etag() {
		t.Error("etag should differ between views with and without fulfillment")
	}
}

func TestExtensionResourceView_Etag_NilIntentDiffers(t *testing.T) {
	withIntent := extensionView()
	without := withIntent
	without.Intent = nil
	if withIntent.Etag() == without.Etag() {
		t.Error("etag should differ between views with and without intent")
	}
}

// ---------------------------------------------------------------------------
// ExtensionResourceView etag tests -- inventory
// ---------------------------------------------------------------------------

func TestExtensionResourceView_Etag_ChangesOnInventoryState(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	uid := NewExtensionResourceUID()

	baseSnap := ExtensionResourceSnapshot{
		UID:          uid,
		ResourceType: "kind.fleetshift.io/Cluster",
		Name:         "clusters/dev",
		Inventory: &InventoryResourceSnapshot{
			Observation: json.RawMessage(`{"v":1}`),
			Conditions: []ConditionSnapshot{
				{Type: "Ready", Status: ConditionTrue, Reason: "OK", Message: "ready", LastTransitionTime: now},
			},
			ObservedAt: now,
			UpdatedAt:  now.Add(time.Minute),
		},
		CreatedAt: now,
		UpdatedAt: now.Add(time.Minute),
	}
	base := ExtensionResourceView{Resource: *ExtensionResourceFromSnapshot(baseSnap)}
	baseEtag := base.Etag()

	// Change the observation.
	changedObsSnap := baseSnap
	changedObsSnap.Inventory = &InventoryResourceSnapshot{
		Observation: json.RawMessage(`{"v":2}`),
		Conditions:  baseSnap.Inventory.Conditions,
		ObservedAt:  now,
		UpdatedAt:   now.Add(time.Minute),
	}
	changed := ExtensionResourceView{Resource: *ExtensionResourceFromSnapshot(changedObsSnap)}
	if changed.Etag() == baseEtag {
		t.Error("etag should change when inventory observation changes")
	}

	// Change a condition status.
	changedCondSnap := baseSnap
	changedCondSnap.Inventory = &InventoryResourceSnapshot{
		Observation: json.RawMessage(`{"v":1}`),
		Conditions: []ConditionSnapshot{
			{Type: "Ready", Status: ConditionFalse, Reason: "NotReady", Message: "degraded", LastTransitionTime: now},
		},
		ObservedAt: now,
		UpdatedAt:  now.Add(time.Minute),
	}
	condChanged := ExtensionResourceView{Resource: *ExtensionResourceFromSnapshot(changedCondSnap)}
	if condChanged.Etag() == baseEtag {
		t.Error("etag should change when condition status changes")
	}
}

func TestExtensionResourceView_Etag_NilInventoryDiffers(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	uid := NewExtensionResourceUID()

	withInvSnap := ExtensionResourceSnapshot{
		UID:          uid,
		ResourceType: "kind.fleetshift.io/Cluster",
		Name:         "clusters/dev",
		Inventory: &InventoryResourceSnapshot{
			Observation: json.RawMessage(`{}`),
			ObservedAt:  now,
			UpdatedAt:   now.Add(time.Minute),
		},
		CreatedAt: now,
		UpdatedAt: now.Add(time.Minute),
	}
	withoutInvSnap := ExtensionResourceSnapshot{
		UID:          uid,
		ResourceType: "kind.fleetshift.io/Cluster",
		Name:         "clusters/dev",
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	v1 := ExtensionResourceView{Resource: *ExtensionResourceFromSnapshot(withInvSnap)}
	v2 := ExtensionResourceView{Resource: *ExtensionResourceFromSnapshot(withoutInvSnap)}
	if v1.Etag() == v2.Etag() {
		t.Error("etag should differ between nil and non-nil inventory")
	}
}

func TestExtensionResourceView_Etag_ChangesOnReportedAliases(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	uid := NewExtensionResourceUID()
	instanceID, err := NewAlias("gcp", "instance_id", "cluster-1")
	if err != nil {
		t.Fatalf("NewAlias(instance_id): %v", err)
	}
	zone, err := NewAlias("gcp", "zone", "us-east1-b")
	if err != nil {
		t.Fatalf("NewAlias(zone): %v", err)
	}

	baseSnap := ExtensionResourceSnapshot{
		UID:             uid,
		ResourceType:    "kind.fleetshift.io/Cluster",
		Name:            "clusters/dev",
		ReportedAliases: NewAliasSet([]Alias{instanceID, zone}).Snapshot(),
		CreatedAt:       now,
		UpdatedAt:       now.Add(time.Minute),
	}
	base := ExtensionResourceView{Resource: *ExtensionResourceFromSnapshot(baseSnap)}
	baseEtag := base.Etag()

	t.Run("value change", func(t *testing.T) {
		updatedInstanceID, err := NewAlias("gcp", "instance_id", "cluster-2")
		if err != nil {
			t.Fatalf("NewAlias(updatedInstanceID): %v", err)
		}
		changedSnap := baseSnap
		changedSnap.ReportedAliases = NewAliasSet([]Alias{updatedInstanceID, zone}).Snapshot()
		changed := ExtensionResourceView{Resource: *ExtensionResourceFromSnapshot(changedSnap)}
		if changed.Etag() == baseEtag {
			t.Error("etag should change when reported aliases change")
		}
	})

	t.Run("order only", func(t *testing.T) {
		reorderedSnap := baseSnap
		reorderedSnap.ReportedAliases = NewAliasSet([]Alias{zone, instanceID}).Snapshot()
		reordered := ExtensionResourceView{Resource: *ExtensionResourceFromSnapshot(reorderedSnap)}
		if reordered.Etag() != baseEtag {
			t.Error("etag should be order-independent for the same reported alias set")
		}
	})
}
