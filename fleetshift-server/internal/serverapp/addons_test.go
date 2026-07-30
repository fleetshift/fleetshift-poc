package serverapp

import (
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestBuildTrustBundlePlacement(t *testing.T) {
	tests := []struct {
		name          string
		enabledAddons map[AddonName]bool
		gcphcpTarget  string
		want          domain.PlacementStrategySpec
	}{
		{
			name:          "no trust bundle consumers",
			enabledAddons: map[AddonName]bool{AddonKubernetes: true},
			want:          domain.PlacementStrategySpec{},
		},
		{
			name:          "kind only",
			enabledAddons: map[AddonName]bool{AddonKind: true},
			want: domain.PlacementStrategySpec{
				Type:    domain.PlacementStrategyStatic,
				Targets: []domain.TargetID{"kind-local"},
			},
		},
		{
			name:          "gcphcp only",
			enabledAddons: map[AddonName]bool{AddonGCPHCP: true},
			gcphcpTarget:  "gcphcp-example-us-central1",
			want: domain.PlacementStrategySpec{
				Type:    domain.PlacementStrategyStatic,
				Targets: []domain.TargetID{"gcphcp-example-us-central1"},
			},
		},
		{
			name: "kind and gcphcp",
			enabledAddons: map[AddonName]bool{
				AddonKind:   true,
				AddonGCPHCP: true,
			},
			gcphcpTarget: "gcphcp-example-us-central1",
			want: domain.PlacementStrategySpec{
				Type:    domain.PlacementStrategyStatic,
				Targets: []domain.TargetID{"kind-local", "gcphcp-example-us-central1"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildTrustBundlePlacement(tt.enabledAddons, tt.gcphcpTarget)
			if got.Type != tt.want.Type {
				t.Fatalf("buildTrustBundlePlacement() type = %q, want %q", got.Type, tt.want.Type)
			}
			if len(got.Targets) != len(tt.want.Targets) {
				t.Fatalf("buildTrustBundlePlacement() targets len = %d, want %d", len(got.Targets), len(tt.want.Targets))
			}
			for i := range tt.want.Targets {
				if got.Targets[i] != tt.want.Targets[i] {
					t.Fatalf("buildTrustBundlePlacement() target[%d] = %q, want %q", i, got.Targets[i], tt.want.Targets[i])
				}
			}
		})
	}
}
