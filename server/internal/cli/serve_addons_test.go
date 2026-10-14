package cli

import (
	"testing"
)

func TestDefaultAddons(t *testing.T) {
	t.Setenv("FLEETSHIFT_SERVER_ADDONS", "")
	if got := defaultAddons(); got != "kind,kubernetes" {
		t.Fatalf("defaultAddons() = %q, want kind,kubernetes", got)
	}

	t.Setenv("FLEETSHIFT_SERVER_ADDONS", "kubernetes,scripted")
	if got := defaultAddons(); got != "kubernetes,scripted" {
		t.Fatalf("defaultAddons() with env = %q, want kubernetes,scripted", got)
	}
}
