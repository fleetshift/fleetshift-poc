package cli

import (
	"testing"
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
