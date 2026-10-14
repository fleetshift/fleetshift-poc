package test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestAIOWelcomeScript locks the welcome oneshot contracts that the s6 graph
// test does not cover: public /readyz with the sandbox CA (no curl -k),
// HTTP_PROXY bypass, quiet curl, a per-request curl timeout, Dex-on
// credentials vs Dex-off IdP text, and a single failure on timeout. Poll
// duration is a tuning knob, not asserted.
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
		"--max-time",
		"--cacert /data/sandbox/pki/ca.crt",
		`--noproxy "${PUBLIC_HOST}"`,
		">/dev/null 2>&1",
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

// TestAIOEntrypointLogLevel matches aioinit.ResolveLogLevel: trim, empty or
// whitespace becomes error, padded values map, invalid values exit 64.
func TestAIOEntrypointLogLevel(t *testing.T) {
	script := stubAIOEntrypoint(t)
	cases := []struct {
		name       string
		set        bool
		raw        string
		wantLevel  string
		wantVerb   string
		wantStatus int
	}{
		{name: "unset", wantLevel: "error", wantVerb: "0"},
		{name: "empty", set: true, raw: "", wantLevel: "error", wantVerb: "0"},
		{name: "whitespace", set: true, raw: "  \t", wantLevel: "error", wantVerb: "0"},
		{name: "error", set: true, raw: "error", wantLevel: "error", wantVerb: "0"},
		{name: "warn", set: true, raw: "warn", wantLevel: "warn", wantVerb: "1"},
		{name: "info", set: true, raw: "info", wantLevel: "info", wantVerb: "2"},
		{name: "debug", set: true, raw: "debug", wantLevel: "debug", wantVerb: "3"},
		{name: "trimmed debug", set: true, raw: "  debug  ", wantLevel: "debug", wantVerb: "3"},
		{name: "invalid", set: true, raw: "verbose", wantStatus: 64},
		{name: "uppercase", set: true, raw: "ERROR", wantStatus: 64},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command("/bin/sh", script)
			cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
			if tc.set {
				cmd.Env = append(cmd.Env, "LOG_LEVEL="+tc.raw)
			}
			out, err := cmd.CombinedOutput()
			if tc.wantStatus != 0 {
				if err == nil {
					t.Fatalf("status 0, want %d; output:\n%s", tc.wantStatus, out)
				}
				if got := cmd.ProcessState.ExitCode(); got != tc.wantStatus {
					t.Fatalf("status %d, want %d; output:\n%s", got, tc.wantStatus, out)
				}
				if !strings.Contains(string(out), "invalid LOG_LEVEL") {
					t.Fatalf("output %q, want invalid LOG_LEVEL", out)
				}
				return
			}
			if err != nil {
				t.Fatalf("%v\n%s", err, out)
			}
			got := string(out)
			if !strings.Contains(got, "LOG_LEVEL="+tc.wantLevel+"\n") {
				t.Fatalf("output %q, want LOG_LEVEL=%s", got, tc.wantLevel)
			}
			if !strings.Contains(got, "S6_VERBOSITY="+tc.wantVerb+"\n") {
				t.Fatalf("output %q, want S6_VERBOSITY=%s", got, tc.wantVerb)
			}
		})
	}
}

// stubAIOEntrypoint copies aio-entrypoint and replaces exec /init with a
// print of the mapped env so the LOG_LEVEL contract can be run without s6.
func stubAIOEntrypoint(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(findAIORoot(t), "s6/scripts/aio-entrypoint"))
	if err != nil {
		t.Fatal(err)
	}
	const execInit = "export LOG_LEVEL S6_VERBOSITY\nexec /init\n"
	body := strings.Replace(string(raw), execInit, "export LOG_LEVEL S6_VERBOSITY\n"+
		`printf 'LOG_LEVEL=%s\nS6_VERBOSITY=%s\n' "$LOG_LEVEL" "$S6_VERBOSITY"`+"\n", 1)
	if body == string(raw) {
		t.Fatal("aio-entrypoint missing export + exec /init")
	}
	path := filepath.Join(t.TempDir(), "aio-entrypoint")
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
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
