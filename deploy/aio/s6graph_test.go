package aio_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestS6ProxyGraph(t *testing.T) {
	root := "."
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
	} {
		path := filepath.Join(root, rel)
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("missing %s: %v", rel, err)
		}
	}

	run, err := os.ReadFile("s6/s6-rc.d/aio-proxy/run")
	if err != nil {
		t.Fatal(err)
	}
	body := string(run)
	if !strings.Contains(body, "s6-setuidgid 1000:1000") {
		t.Fatalf("aio-proxy must run as 1000:1000:\n%s", body)
	}
	if !strings.Contains(body, "/usr/local/bin/aio-proxy") {
		t.Fatalf("aio-proxy run script missing binary:\n%s", body)
	}

	fsRun, err := os.ReadFile("s6/s6-rc.d/fleetshift/run")
	if err != nil {
		t.Fatal(err)
	}
	fsBody := string(fsRun)
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
}
