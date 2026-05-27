package cli

import (
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestParseAddons(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  map[string]bool
	}{
		{
			name:  "all addons",
			input: "kind,ocp,kubernetes",
			want:  map[string]bool{"kind": true, "ocp": true, "kubernetes": true},
		},
		{
			name:  "production subset",
			input: "ocp,kubernetes",
			want:  map[string]bool{"ocp": true, "kubernetes": true},
		},
		{
			name:  "single addon",
			input: "kind",
			want:  map[string]bool{"kind": true},
		},
		{
			name:  "whitespace trimmed",
			input: " kind , ocp ",
			want:  map[string]bool{"kind": true, "ocp": true},
		},
		{
			name:  "empty string",
			input: "",
			want:  map[string]bool{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseAddons(tt.input)
			if len(got) != len(tt.want) {
				t.Fatalf("parseAddons(%q) returned %d entries, want %d", tt.input, len(got), len(tt.want))
			}
			for k, v := range tt.want {
				if got[k] != v {
					t.Errorf("parseAddons(%q)[%q] = %v, want %v", tt.input, k, got[k], v)
				}
			}
		})
	}
}

func TestBuildTrustBundlePlacement(t *testing.T) {
	tests := []struct {
		name          string
		enabledAddons map[string]bool
		gcphcpTarget  string
		want          domain.PlacementStrategySpec
	}{
		{
			name:          "no trust bundle consumers",
			enabledAddons: map[string]bool{"kubernetes": true},
			want:          domain.PlacementStrategySpec{},
		},
		{
			name:          "kind only",
			enabledAddons: map[string]bool{"kind": true},
			want: domain.PlacementStrategySpec{
				Type:    domain.PlacementStrategyStatic,
				Targets: []domain.TargetID{"kind-local"},
			},
		},
		{
			name:          "gcphcp only",
			enabledAddons: map[string]bool{"gcphcp": true},
			gcphcpTarget:  "gcphcp-example-us-central1",
			want: domain.PlacementStrategySpec{
				Type:    domain.PlacementStrategyStatic,
				Targets: []domain.TargetID{"gcphcp-example-us-central1"},
			},
		},
		{
			name:          "kind and gcphcp",
			enabledAddons: map[string]bool{"kind": true, "gcphcp": true},
			gcphcpTarget:  "gcphcp-example-us-central1",
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
