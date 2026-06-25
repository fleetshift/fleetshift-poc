package domain

import (
	"strings"
	"testing"
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
