package domain

import (
	"encoding/json"
	"strings"
	"testing"
)

func fulfillmentValue(s FulfillmentSnapshot) Fulfillment {
	return *FulfillmentFromSnapshot(s)
}

func TestDeploymentView_Etag_Deterministic(t *testing.T) {
	v := DeploymentView{
		Deployment: DeploymentFromSnapshot(DeploymentSnapshot{
			ID:  "dep-1",
			UID: "uid-abc",
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
		Deployment:  DeploymentFromSnapshot(DeploymentSnapshot{ID: "dep-1", UID: "uid-abc"}),
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
		Deployment: DeploymentFromSnapshot(DeploymentSnapshot{ID: "dep-1", UID: "uid-abc"}),
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
	// Two views whose variable-length fields concatenate to the same
	// byte stream without length-framing: (ID="ab", UID="c") vs
	// (ID="a", UID="bc"). They must produce distinct etags.
	a := DeploymentView{
		Deployment:  DeploymentFromSnapshot(DeploymentSnapshot{ID: "ab", UID: "c"}),
		Fulfillment: fulfillmentValue(FulfillmentSnapshot{Generation: 1, State: FulfillmentStateActive}),
	}
	b := DeploymentView{
		Deployment:  DeploymentFromSnapshot(DeploymentSnapshot{ID: "a", UID: "bc"}),
		Fulfillment: fulfillmentValue(FulfillmentSnapshot{Generation: 1, State: FulfillmentStateActive}),
	}
	if a.Etag() == b.Etag() {
		t.Error("etags must differ when field values differ, even if concatenation is the same")
	}
}

func TestFulfillment_Etag_ResolvedTargetBoundariesAreUnambiguous(t *testing.T) {
	// Two views whose ResolvedTargets concatenate to the same bytes:
	// ["ab","c"] vs ["a","bc"]. They must produce distinct etags.
	a := DeploymentView{
		Deployment: DeploymentFromSnapshot(DeploymentSnapshot{ID: "d", UID: "u"}),
		Fulfillment: fulfillmentValue(FulfillmentSnapshot{
			Generation:      1,
			State:           FulfillmentStateActive,
			ResolvedTargets: []TargetID{"ab", "c"},
		}),
	}
	b := DeploymentView{
		Deployment: DeploymentFromSnapshot(DeploymentSnapshot{ID: "d", UID: "u"}),
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
	a := DeploymentView{
		Deployment: DeploymentFromSnapshot(DeploymentSnapshot{ID: "d", UID: "u"}),
		Fulfillment: fulfillmentValue(FulfillmentSnapshot{
			Generation:      1,
			State:           FulfillmentStateActive,
			ResolvedTargets: []TargetID{"abc"},
		}),
	}
	b := DeploymentView{
		Deployment: DeploymentFromSnapshot(DeploymentSnapshot{ID: "d", UID: "u"}),
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

func managedResourceValue(s ManagedResourceSnapshot) ManagedResource {
	return *ManagedResourceFromSnapshot(s)
}

func TestManagedResourceView_Etag_FieldBoundariesAreUnambiguous(t *testing.T) {
	// (Name="ab", UID="c") vs (Name="a", UID="bc") — same concat,
	// must differ.
	a := ManagedResourceView{
		ManagedResource: managedResourceValue(ManagedResourceSnapshot{ResourceType: "t", Name: "ab", UID: "c"}),
		Intent:          ResourceIntent{Spec: json.RawMessage(`{}`)},
		Fulfillment:     fulfillmentValue(FulfillmentSnapshot{Generation: 1, State: FulfillmentStateActive}),
	}
	b := ManagedResourceView{
		ManagedResource: managedResourceValue(ManagedResourceSnapshot{ResourceType: "t", Name: "a", UID: "bc"}),
		Intent:          ResourceIntent{Spec: json.RawMessage(`{}`)},
		Fulfillment:     fulfillmentValue(FulfillmentSnapshot{Generation: 1, State: FulfillmentStateActive}),
	}
	if a.Etag() == b.Etag() {
		t.Error("etags must differ when field values differ, even if concatenation is the same")
	}
}

func TestManagedResourceView_Etag_Deterministic(t *testing.T) {
	v := ManagedResourceView{
		ManagedResource: managedResourceValue(ManagedResourceSnapshot{
			ResourceType:   "api.kind.cluster",
			Name:           "test-cluster",
			UID:            "uid-mr",
			CurrentVersion: 2,
		}),
		Intent: ResourceIntent{
			ResourceType: "api.kind.cluster",
			Name:         "test-cluster",
			Version:      2,
			Spec:         json.RawMessage(`{"replicas":3}`),
		},
		Fulfillment: fulfillmentValue(FulfillmentSnapshot{
			ID:         "f-2",
			Generation: 5,
			State:      FulfillmentStateActive,
		}),
	}

	e1 := v.Etag()
	e2 := v.Etag()
	if e1 != e2 {
		t.Errorf("etag is not deterministic: %q != %q", e1, e2)
	}
}

func TestManagedResourceView_Etag_WeakPrefix(t *testing.T) {
	v := ManagedResourceView{
		ManagedResource: managedResourceValue(ManagedResourceSnapshot{
			ResourceType: "api.kind.cluster",
			Name:         "test-cluster",
		}),
		Fulfillment: fulfillmentValue(FulfillmentSnapshot{ID: "f-2", Generation: 1, State: FulfillmentStateCreating}),
	}
	etag := string(v.Etag())
	if !strings.HasPrefix(etag, `W/"`) {
		t.Errorf("etag should start with W/\", got %q", etag)
	}
	if !strings.HasSuffix(etag, `"`) {
		t.Errorf("etag should end with \", got %q", etag)
	}
}

func TestManagedResourceView_Etag_ChangesOnStateChange(t *testing.T) {
	base := ManagedResourceView{
		ManagedResource: managedResourceValue(ManagedResourceSnapshot{
			ResourceType:   "api.kind.cluster",
			Name:           "test-cluster",
			CurrentVersion: 1,
		}),
		Intent: ResourceIntent{
			Version: 1,
			Spec:    json.RawMessage(`{"replicas":3}`),
		},
		Fulfillment: fulfillmentValue(FulfillmentSnapshot{
			ID:         "f-2",
			Generation: 5,
			State:      FulfillmentStateActive,
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

	t.Run("intent version change", func(t *testing.T) {
		mrSnap := base.ManagedResource.Snapshot()
		mrSnap.CurrentVersion = 2
		v := base
		v.ManagedResource = managedResourceValue(mrSnap)
		v.Intent.Version = 2
		if v.Etag() == baseEtag {
			t.Error("etag should change when intent version changes")
		}
	})

	t.Run("spec change", func(t *testing.T) {
		v := base
		v.Intent.Spec = json.RawMessage(`{"replicas":5}`)
		if v.Etag() == baseEtag {
			t.Error("etag should change when spec changes")
		}
	})
}
