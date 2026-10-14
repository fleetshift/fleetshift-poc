package aioinit

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func clearGCPHCPEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"GCPHCP_ENABLED", "GCPHCP_CONFIG", "GCPHCP_GATEWAY_URL",
		"GCPHCP_CONFIG_OUT", "RENDER_GCPHCP_CONFIG", "FLEETSHIFT_SERVER_ADDONS",
		"GCPHCP_GATEWAY_AUDIENCE", "GCPHCP_TARGET_ID", "GCPHCP_GCP_PROJECT",
		"GCPHCP_GCP_REGION", "GCPHCP_WORKFORCE_POOL", "GCPHCP_WORKFORCE_PROVIDER",
		"GCPHCP_BROKER_SA_EMAIL",
	} {
		t.Setenv(k, "")
	}
}

func TestResolveGCPHCP(t *testing.T) {
	t.Run("default addons", func(t *testing.T) {
		clearGCPHCPEnv(t)
		got, err := ResolveGCPHCP()
		if err != nil {
			t.Fatal(err)
		}
		if got.Addons != "kind,kubernetes" || got.GCPHCPConfig != "" {
			t.Fatalf("got %+v", got)
		}
	})

	t.Run("false disables gcphcp", func(t *testing.T) {
		clearGCPHCPEnv(t)
		t.Setenv("GCPHCP_ENABLED", "false")
		t.Setenv("GCPHCP_GATEWAY_URL", "https://cls.example")
		got, err := ResolveGCPHCP()
		if err != nil {
			t.Fatal(err)
		}
		if got.Addons != "kind,kubernetes" || got.GCPHCPConfig != "" {
			t.Fatalf("got %+v", got)
		}
	})

	t.Run("invalid enabled value", func(t *testing.T) {
		clearGCPHCPEnv(t)
		t.Setenv("GCPHCP_ENABLED", "yes")
		_, err := ResolveGCPHCP()
		if err == nil || !strings.Contains(err.Error(), "GCPHCP_ENABLED") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("explicit config", func(t *testing.T) {
		clearGCPHCPEnv(t)
		t.Setenv("GCPHCP_CONFIG", "/tmp/custom-gcphcp.yaml")
		got, err := ResolveGCPHCP()
		if err != nil {
			t.Fatal(err)
		}
		if got.Addons != "kind,kubernetes,gcphcp" || got.GCPHCPConfig != "/tmp/custom-gcphcp.yaml" {
			t.Fatalf("got %+v", got)
		}
	})

	t.Run("addons env override", func(t *testing.T) {
		clearGCPHCPEnv(t)
		t.Setenv("FLEETSHIFT_SERVER_ADDONS", "kind")
		got, err := ResolveGCPHCP()
		if err != nil {
			t.Fatal(err)
		}
		if got.Addons != "kind" {
			t.Fatalf("Addons = %q", got.Addons)
		}
	})

	t.Run("enabled without gateway", func(t *testing.T) {
		clearGCPHCPEnv(t)
		t.Setenv("GCPHCP_ENABLED", "true")
		_, err := ResolveGCPHCP()
		if err == nil || !strings.Contains(err.Error(), "GCPHCP_GATEWAY_URL") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("orphan overrides", func(t *testing.T) {
		clearGCPHCPEnv(t)
		t.Setenv("GCPHCP_GCP_PROJECT", "proj")
		_, err := ResolveGCPHCP()
		if err == nil || !strings.Contains(err.Error(), "optional overrides") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("gateway renders config", func(t *testing.T) {
		clearGCPHCPEnv(t)
		root := t.TempDir()
		out := filepath.Join(root, "gcphcp.yaml")
		renderer := filepath.Join(root, "render.sh")
		script := `#!/bin/sh
out=""
while [ $# -gt 0 ]; do
  if [ "$1" = --output ]; then out=$2; shift 2; continue; fi
  shift
done
echo rendered > "$out"
`
		if err := os.WriteFile(renderer, []byte(script), 0755); err != nil {
			t.Fatal(err)
		}
		t.Setenv("GCPHCP_GATEWAY_URL", "https://cls.example")
		t.Setenv("GCPHCP_CONFIG_OUT", out)
		t.Setenv("RENDER_GCPHCP_CONFIG", renderer)
		got, err := ResolveGCPHCP()
		if err != nil {
			t.Fatal(err)
		}
		if got.Addons != "kind,kubernetes,gcphcp" || got.GCPHCPConfig != out {
			t.Fatalf("got %+v", got)
		}
		raw, err := os.ReadFile(out)
		if err != nil {
			t.Fatal(err)
		}
		if strings.TrimSpace(string(raw)) != "rendered" {
			t.Fatalf("renderer output = %q", raw)
		}
		// Process env must not be mutated by ResolveGCPHCP.
		if os.Getenv("GCPHCP_ENABLED") != "" {
			t.Fatalf("GCPHCP_ENABLED leaked into process: %q", os.Getenv("GCPHCP_ENABLED"))
		}
	})

	t.Run("renderer timeout", func(t *testing.T) {
		clearGCPHCPEnv(t)
		root := t.TempDir()
		out := filepath.Join(root, "gcphcp.yaml")
		renderer := filepath.Join(root, "hang.sh")
		if err := os.WriteFile(renderer, []byte("#!/bin/sh\nexec sleep 60\n"), 0755); err != nil {
			t.Fatal(err)
		}
		t.Setenv("GCPHCP_GATEWAY_URL", "https://cls.example")
		t.Setenv("GCPHCP_CONFIG_OUT", out)
		t.Setenv("RENDER_GCPHCP_CONFIG", renderer)

		old := renderTimeout
		renderTimeout = 100 * time.Millisecond
		t.Cleanup(func() { renderTimeout = old })

		_, err := ResolveGCPHCP()
		if err == nil {
			t.Fatal("expected renderer timeout error")
		}
		if !strings.Contains(err.Error(), "render gcphcp config") {
			t.Fatalf("err = %v, want render gcphcp config", err)
		}
	})
}
