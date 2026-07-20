package cli

import (
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestDefaultAddons(t *testing.T) {
	t.Setenv("FLEETSHIFT_SERVER_ADDONS", "")
	if got := defaultAddons(); got != "kind,kubernetes" {
		t.Fatalf("defaultAddons() = %q, want kind,kubernetes", got)
	}

	t.Setenv("FLEETSHIFT_SERVER_ADDONS", "kubernetes,gcphcp")
	if got := defaultAddons(); got != "kubernetes,gcphcp" {
		t.Fatalf("defaultAddons() with env = %q, want kubernetes,gcphcp", got)
	}
}

func TestRequireGCPHCPConfig(t *testing.T) {
	if err := requireGCPHCPConfig("/tmp/gcphcp.yaml"); err != nil {
		t.Fatalf("requireGCPHCPConfig(path) unexpected error: %v", err)
	}

	err := requireGCPHCPConfig("")
	if err == nil {
		t.Fatal("requireGCPHCPConfig(\"\") = nil, want error")
	}
	if !strings.Contains(err.Error(), "GCPHCP_CONFIG") {
		t.Fatalf("requireGCPHCPConfig error %q does not mention GCPHCP_CONFIG", err)
	}
	if !strings.Contains(err.Error(), "--gcphcp-config") {
		t.Fatalf("requireGCPHCPConfig error %q does not mention --gcphcp-config", err)
	}
}

func TestResolveGCPHCPConfigPath(t *testing.T) {
	t.Setenv("GCPHCP_CONFIG", "/env/gcphcp.yaml")
	if got := resolveGCPHCPConfigPath("/flag/gcphcp.yaml"); got != "/flag/gcphcp.yaml" {
		t.Fatalf("flag path should win, got %q", got)
	}
	if got := resolveGCPHCPConfigPath(""); got != "/env/gcphcp.yaml" {
		t.Fatalf("env path = %q, want /env/gcphcp.yaml", got)
	}

	t.Setenv("GCPHCP_CONFIG", "")
	if got := resolveGCPHCPConfigPath(""); got != "" {
		t.Fatalf("empty path = %q, want empty", got)
	}
}

func TestParseAddons(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  map[string]bool
	}{
		{
			name:  "all addons",
			input: "kind,kubernetes,gcphcp",
			want:  map[string]bool{"kind": true, "kubernetes": true, "gcphcp": true},
		},
		{
			name:  "single addon",
			input: "kind",
			want:  map[string]bool{"kind": true},
		},
		{
			name:  "whitespace trimmed",
			input: " kind , kubernetes ",
			want:  map[string]bool{"kind": true, "kubernetes": true},
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
