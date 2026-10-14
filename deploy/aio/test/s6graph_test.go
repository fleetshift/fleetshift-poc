package test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestS6ServiceGraph locks the s6-rc graph and run-script contracts under
// deploy/aio/s6. A failure is this image's packaging, not openshift/release.
//
// It requires aio-proxy / aio-init / dex / fleetshift / aio-welcome
// dependency files and the user bundle, then checks: aio-proxy runs as
// 1002:1002 (not FleetShift 1000); fleetshift drops to 1000, sources
// public.env, probes Dex with the sandbox CA, bypasses HTTP_PROXY for
// PUBLIC_HOST, does not use curl -k, and exits non-zero if discovery is
// not ready; aio-welcome is a oneshot that depends on fleetshift.
func TestS6ServiceGraph(t *testing.T) {
	root := findAIORoot(t)
	for _, rel := range []string{
		"s6/s6-rc.d/aio-proxy/type",
		"s6/s6-rc.d/aio-proxy/run",
		"s6/s6-rc.d/aio-proxy/finish",
		"s6/s6-rc.d/aio-proxy/dependencies.d/base",
		"s6/s6-rc.d/aio-proxy/dependencies.d/aio-init",
		"s6/s6-rc.d/fleetshift/dependencies.d/aio-init",
		"s6/s6-rc.d/fleetshift/dependencies.d/dex",
		"s6/s6-rc.d/fleetshift/dependencies.d/aio-proxy",
		"s6/s6-rc.d/dex/dependencies.d/aio-init",
		"s6/user-bundles.d/user/contents.d/aio-proxy",
		"s6/user-bundles.d/user/contents.d/dex",
		"s6/user-bundles.d/user/contents.d/fleetshift",
		"s6/s6-rc.d/aio-welcome/type",
		"s6/s6-rc.d/aio-welcome/up",
		"s6/s6-rc.d/aio-welcome/dependencies.d/fleetshift",
		"s6/s6-rc.d/aio-welcome/dependencies.d/base",
		"s6/scripts/aio-welcome",
		"s6/user-bundles.d/user/contents.d/aio-welcome",
	} {
		path := filepath.Join(root, rel)
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("missing %s: %v", rel, err)
		}
	}

	run, err := os.ReadFile(filepath.Join(root, "s6/s6-rc.d/aio-proxy/run"))
	if err != nil {
		t.Fatal(err)
	}
	body := string(run)
	if !strings.Contains(body, "s6-setuidgid 1002:1002") {
		t.Fatalf("aio-proxy must run as 1002:1002:\n%s", body)
	}
	if strings.Contains(body, "s6-setuidgid 1000:1000") {
		t.Fatalf("aio-proxy must not share the FleetShift uid:\n%s", body)
	}
	if !strings.Contains(body, "/usr/local/bin/aio-proxy") {
		t.Fatalf("aio-proxy run script missing binary:\n%s", body)
	}

	fsRun, err := os.ReadFile(filepath.Join(root, "s6/s6-rc.d/fleetshift/run"))
	if err != nil {
		t.Fatal(err)
	}
	fsBody := string(fsRun)
	if !strings.Contains(fsBody, "-u 1000") && !strings.Contains(fsBody, "s6-setuidgid 1000:1000") {
		t.Fatal("fleetshift run must drop to 1000:1000")
	}
	if strings.Contains(fsBody, "1002:1002") {
		t.Fatal("fleetshift run must not use the aio-proxy uid")
	}
	if !strings.Contains(fsBody, ". /run/fleetshift/public.env") {
		t.Fatal("fleetshift run must source public.env for Dex discovery")
	}
	if !strings.Contains(fsBody, "${DEX_DISCOVERY_URL}") {
		t.Fatal("fleetshift run must probe Dex discovery from public.env")
	}
	if !strings.Contains(fsBody, "--cacert /data/sandbox/pki/ca.crt") {
		t.Fatal("fleetshift run must trust the sandbox CA")
	}
	if !strings.Contains(fsBody, `--noproxy "${PUBLIC_HOST}"`) {
		t.Fatal("fleetshift run must bypass HTTP_PROXY for PUBLIC_HOST")
	}
	if strings.Contains(fsBody, "curl -k") || strings.Contains(fsBody, "curl -sk") {
		t.Fatal("fleetshift run must not use curl -k")
	}
	if !strings.Contains(fsBody, "exit 1") {
		t.Fatal("fleetshift run must fail if Dex discovery is not ready")
	}

	welcomeType, err := os.ReadFile(filepath.Join(root, "s6/s6-rc.d/aio-welcome/type"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(welcomeType)) != "oneshot" {
		t.Fatalf("aio-welcome type = %q, want oneshot", welcomeType)
	}
	welcomeUp, err := os.ReadFile(filepath.Join(root, "s6/s6-rc.d/aio-welcome/up"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(welcomeUp), "/etc/s6-overlay/scripts/aio-welcome") {
		t.Fatalf("aio-welcome up must invoke the AIO welcome script:\n%s", welcomeUp)
	}

	// Overlay scripts and longrun run files are git 0755; COPY preserves that
	// mode, so Dockerfile.fleetshift does not chmod them.
	for _, rel := range []string{
		"s6/scripts/aio-init",
		"s6/scripts/aio-entrypoint",
		"s6/scripts/aio-welcome",
		"s6/s6-rc.d/aio-proxy/run",
		"s6/s6-rc.d/dex/run",
		"s6/s6-rc.d/fleetshift/run",
	} {
		info, err := os.Stat(filepath.Join(root, rel))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode()&0o111 == 0 {
			t.Fatalf("%s must be executable (COPY preserves git mode)", rel)
		}
	}
}
