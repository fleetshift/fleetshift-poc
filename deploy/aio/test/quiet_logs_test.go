package test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestAIOWelcomeScript locks the readiness-gated welcome oneshot: it polls
// the public /readyz URL with the sandbox CA, suppresses curl noise, prints
// Dex-on credentials or Dex-off IdP text, and fails once on timeout.
func TestAIOWelcomeScript(t *testing.T) {
	root := findAIORoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "s6/scripts/aio-welcome"))
	if err != nil {
		t.Fatal(err)
	}
	body := string(raw)
	for _, want := range []string{
		"#!/command/with-contenv sh",
		"set -eu",
		". /run/fleetshift/public.env",
		"${PUBLIC_ORIGIN}/readyz",
		"--cacert /data/sandbox/pki/ca.crt",
		`--noproxy "${PUBLIC_HOST}"`,
		"--max-time 2",
		">/dev/null 2>&1",
		`[ "$i" -lt 120 ]`,
		"public readiness timed out",
		"/run/fleetshift/dex.enabled",
		"ops@fleetshift.local",
		"fleetshift-ops",
		"dev@fleetshift.local",
		"fleetshift-dev",
		"configured external identity provider",
		"Press Ctrl+C to stop FleetShift.",
		"exit 1",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("aio-welcome missing %q\n%s", want, body)
		}
	}
	if strings.Contains(body, "curl -k") || strings.Contains(body, "curl -sk") {
		t.Fatal("aio-welcome must not use curl -k")
	}
	if strings.Contains(body, "waiting") || strings.Contains(body, "Waiting") {
		t.Fatal("aio-welcome must not print waiting progress")
	}
}

// TestAIOEntrypointAndDockerfile locks the single LOG_LEVEL control: the
// wrapper maps error/warn/info/debug to s6 verbosity 0/1/2/3, rejects
// invalid values, execs /init, and the image defaults LOG_LEVEL=error.
func TestAIOEntrypointAndDockerfile(t *testing.T) {
	root := findAIORoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "s6/scripts/aio-entrypoint"))
	if err != nil {
		t.Fatal(err)
	}
	body := string(raw)
	for _, want := range []string{
		"LOG_LEVEL=${LOG_LEVEL:-error}",
		"error) S6_VERBOSITY=0",
		"warn)  S6_VERBOSITY=1",
		"info)  S6_VERBOSITY=2",
		"debug) S6_VERBOSITY=3",
		"invalid LOG_LEVEL",
		"exit 64",
		"export LOG_LEVEL S6_VERBOSITY",
		"exec /init",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("aio-entrypoint missing %q\n%s", want, body)
		}
	}

	dockerfile := readDockerfileFleetshift(t)
	if !strings.Contains(dockerfile, "LOG_LEVEL=error") {
		t.Fatal("Dockerfile.fleetshift must default LOG_LEVEL=error")
	}
	if !strings.Contains(dockerfile, `ENTRYPOINT ["/etc/s6-overlay/scripts/aio-entrypoint"]`) {
		t.Fatal("Dockerfile.fleetshift must use the AIO entrypoint")
	}
	if strings.Contains(dockerfile, `ENTRYPOINT ["/init"]`) {
		t.Fatal("Dockerfile.fleetshift must not use /init as ENTRYPOINT")
	}
}
