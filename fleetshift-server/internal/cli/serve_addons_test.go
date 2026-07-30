package cli

import (
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/serverapp"
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
		want  map[serverapp.AddonName]bool
	}{
		{
			name:  "all addons",
			input: "kind,kubernetes,gcphcp",
			want: map[serverapp.AddonName]bool{
				serverapp.AddonKind:       true,
				serverapp.AddonKubernetes: true,
				serverapp.AddonGCPHCP:     true,
			},
		},
		{
			name:  "single addon",
			input: "kind",
			want:  map[serverapp.AddonName]bool{serverapp.AddonKind: true},
		},
		{
			name:  "whitespace trimmed",
			input: " kind , kubernetes ",
			want: map[serverapp.AddonName]bool{
				serverapp.AddonKind:       true,
				serverapp.AddonKubernetes: true,
			},
		},
		{
			name:  "empty string",
			input: "",
			want:  map[serverapp.AddonName]bool{},
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
