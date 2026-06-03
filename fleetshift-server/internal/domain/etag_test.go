package domain

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestDeploymentView_Etag_Deterministic(t *testing.T) {
	v := DeploymentView{
		Deployment: Deployment{
			ID:  "dep-1",
			UID: "uid-abc",
		},
		Fulfillment: Fulfillment{
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
			CreatedAt:       time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			UpdatedAt:       time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		},
	}

	e1 := v.Etag()
	e2 := v.Etag()
	if e1 != e2 {
		t.Errorf("etag is not deterministic: %q != %q", e1, e2)
	}
}

func TestDeploymentView_Etag_WeakPrefix(t *testing.T) {
	v := DeploymentView{
		Deployment:  Deployment{ID: "dep-1", UID: "uid-abc"},
		Fulfillment: Fulfillment{ID: "f-1", Generation: 1, State: FulfillmentStateCreating},
	}
	etag := v.Etag()
	if !strings.HasPrefix(etag, `W/"`) {
		t.Errorf("etag should start with W/\", got %q", etag)
	}
	if !strings.HasSuffix(etag, `"`) {
		t.Errorf("etag should end with \", got %q", etag)
	}
}

func TestDeploymentView_Etag_ChangesOnStateChange(t *testing.T) {
	base := DeploymentView{
		Deployment: Deployment{ID: "dep-1", UID: "uid-abc"},
		Fulfillment: Fulfillment{
			ID:              "f-1",
			Generation:      3,
			State:           FulfillmentStateActive,
			ResolvedTargets: []TargetID{"t1"},
			CreatedAt:       time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			UpdatedAt:       time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		},
	}
	baseEtag := base.Etag()

	t.Run("state change", func(t *testing.T) {
		v := base
		v.Fulfillment.State = FulfillmentStatePausedAuth
		if v.Etag() == baseEtag {
			t.Error("etag should change when state changes")
		}
	})

	t.Run("generation change", func(t *testing.T) {
		v := base
		v.Fulfillment.Generation = 4
		if v.Etag() == baseEtag {
			t.Error("etag should change when generation changes")
		}
	})

	t.Run("resolved targets change", func(t *testing.T) {
		v := base
		v.Fulfillment.ResolvedTargets = []TargetID{"t1", "t2"}
		if v.Etag() == baseEtag {
			t.Error("etag should change when resolved targets change")
		}
	})

	t.Run("update time change", func(t *testing.T) {
		v := base
		v.Fulfillment.UpdatedAt = time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
		if v.Etag() == baseEtag {
			t.Error("etag should change when update time changes")
		}
	})
}

func TestManagedResourceView_Etag_Deterministic(t *testing.T) {
	v := ManagedResourceView{
		ManagedResource: ManagedResource{
			ResourceType:   "api.kind.cluster",
			Name:           "test-cluster",
			UID:            "uid-mr",
			CurrentVersion: 2,
		},
		Intent: ResourceIntent{
			ResourceType: "api.kind.cluster",
			Name:         "test-cluster",
			Version:      2,
			Spec:         json.RawMessage(`{"replicas":3}`),
		},
		Fulfillment: Fulfillment{
			ID:         "f-2",
			Generation: 5,
			State:      FulfillmentStateActive,
		},
	}

	e1 := v.Etag()
	e2 := v.Etag()
	if e1 != e2 {
		t.Errorf("etag is not deterministic: %q != %q", e1, e2)
	}
}

func TestManagedResourceView_Etag_WeakPrefix(t *testing.T) {
	v := ManagedResourceView{
		ManagedResource: ManagedResource{
			ResourceType: "api.kind.cluster",
			Name:         "test-cluster",
		},
		Fulfillment: Fulfillment{ID: "f-2", Generation: 1, State: FulfillmentStateCreating},
	}
	etag := v.Etag()
	if !strings.HasPrefix(etag, `W/"`) {
		t.Errorf("etag should start with W/\", got %q", etag)
	}
	if !strings.HasSuffix(etag, `"`) {
		t.Errorf("etag should end with \", got %q", etag)
	}
}

func TestManagedResourceView_Etag_ChangesOnStateChange(t *testing.T) {
	base := ManagedResourceView{
		ManagedResource: ManagedResource{
			ResourceType:   "api.kind.cluster",
			Name:           "test-cluster",
			CurrentVersion: 1,
		},
		Intent: ResourceIntent{
			Version: 1,
			Spec:    json.RawMessage(`{"replicas":3}`),
		},
		Fulfillment: Fulfillment{
			ID:         "f-2",
			Generation: 5,
			State:      FulfillmentStateActive,
		},
	}
	baseEtag := base.Etag()

	t.Run("state change", func(t *testing.T) {
		v := base
		v.Fulfillment.State = FulfillmentStatePausedAuth
		if v.Etag() == baseEtag {
			t.Error("etag should change when state changes")
		}
	})

	t.Run("intent version change", func(t *testing.T) {
		v := base
		v.ManagedResource.CurrentVersion = 2
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
