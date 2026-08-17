package kind

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIssuerHostname(t *testing.T) {
	got, err := issuerHostname("https://fleetshift-sandbox.localhost:8085/dex")
	if err != nil {
		t.Fatal(err)
	}
	if got != "fleetshift-sandbox.localhost" {
		t.Fatalf("issuerHostname = %q", got)
	}
	if _, err := issuerHostname("https:///dex"); err == nil {
		t.Fatal("expected empty host error")
	}
}

func TestIsLoopbackHostname(t *testing.T) {
	for _, host := range []string{"localhost", "127.0.0.1", "::1"} {
		if !isLoopbackHostname(host) {
			t.Fatalf("isLoopbackHostname(%q) = false", host)
		}
	}
	if isLoopbackHostname("fleetshift-sandbox.localhost") {
		t.Fatal("DNS .localhost name is not a loopback address")
	}
}

func TestApplyLoopbackIssuerHost_SkipsLoopback(t *testing.T) {
	cfg := toKindConfig(ClusterSpec{Name: "x", Nodes: []NodeSpec{{Role: "control-plane"}}})
	if err := applyLoopbackIssuerHost(&cfg, "https://127.0.0.1:8085/dex"); err != nil {
		t.Fatal(err)
	}
	if len(cfg.Nodes[0].ExtraMounts) != 0 || len(cfg.Nodes[0].KubeadmConfigPatches) != 0 {
		t.Fatalf("loopback issuer should not add hostAliases: %+v", cfg.Nodes[0])
	}
}

func TestApplyLoopbackIssuerHost_AddsPatchForDNSHost(t *testing.T) {
	cfg := toKindConfig(ClusterSpec{Name: "x", Nodes: []NodeSpec{{Role: "control-plane"}}})
	if err := applyLoopbackIssuerHost(&cfg, "https://fleetshift-sandbox.localhost:8085/dex"); err != nil {
		t.Fatal(err)
	}
	if len(cfg.Nodes[0].ExtraMounts) != 1 {
		t.Fatalf("mounts = %+v", cfg.Nodes[0].ExtraMounts)
	}
	t.Cleanup(func() { _ = os.RemoveAll(cfg.Nodes[0].ExtraMounts[0].HostPath) })
	raw, err := os.ReadFile(filepath.Join(cfg.Nodes[0].ExtraMounts[0].HostPath, kubeadmIssuerHostPatchFile))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "- fleetshift-sandbox.localhost") {
		t.Fatalf("patch missing hostname:\n%s", raw)
	}
}

func TestWriteIssuerHostPatch(t *testing.T) {
	dir, err := writeIssuerHostPatch("fleetshift-sandbox.localhost")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	raw, err := os.ReadFile(filepath.Join(dir, kubeadmIssuerHostPatchFile))
	if err != nil {
		t.Fatal(err)
	}
	body := string(raw)
	for _, want := range []string{
		"name: kube-apiserver",
		"namespace: kube-system",
		`ip: "127.0.0.1"`,
		"- fleetshift-sandbox.localhost",
		"hostAliases:",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("patch missing %q:\n%s", want, body)
		}
	}
}

func TestApplyIssuerHostOverlay_ControlPlaneOnly(t *testing.T) {
	cfg := toKindConfig(ClusterSpec{
		Name: "multi",
		Nodes: []NodeSpec{
			{Role: "control-plane"},
			{Role: "control-plane"},
			{Role: "worker"},
		},
	})
	applyIssuerHostOverlay(&cfg, "/tmp/kind-oidc-hostalias")

	if len(cfg.Nodes) != 3 {
		t.Fatalf("nodes = %d", len(cfg.Nodes))
	}
	for i, n := range cfg.Nodes {
		hasMount := false
		for _, m := range n.ExtraMounts {
			if m.ContainerPath == kubeadmIssuerHostPatchesDir && m.HostPath == "/tmp/kind-oidc-hostalias" && m.ReadOnly {
				hasMount = true
			}
		}
		hasInit := false
		hasJoin := false
		for _, p := range n.KubeadmConfigPatches {
			if strings.Contains(p, "kind: InitConfiguration") && strings.Contains(p, kubeadmIssuerHostPatchesDir) {
				hasInit = true
			}
			if strings.Contains(p, "kind: JoinConfiguration") && strings.Contains(p, kubeadmIssuerHostPatchesDir) {
				hasJoin = true
			}
		}
		if n.Role == "worker" {
			if hasMount || hasInit || hasJoin {
				t.Fatalf("worker node %d unexpectedly received issuer-host overlay: %+v", i, n)
			}
			continue
		}
		if !hasMount || !hasInit || !hasJoin {
			t.Fatalf("control-plane node %d missing overlay mount=%v init=%v join=%v patches=%v mounts=%v", i, hasMount, hasInit, hasJoin, n.KubeadmConfigPatches, n.ExtraMounts)
		}
	}
}

func TestApplyOIDCOverlay_CAMountCoexistsWithIssuerHost(t *testing.T) {
	cfg := toKindConfig(ClusterSpec{
		Name:  "oidc",
		Nodes: []NodeSpec{{Role: "control-plane"}, {Role: "worker"}},
	})
	applyOIDCOverlay(&cfg, &OIDCSpec{}, "https://fleetshift-sandbox.localhost:8085/dex", "fleetshift", "/tmp/oidc-ca.pem")
	applyIssuerHostOverlay(&cfg, "/tmp/kind-oidc-hostalias")

	if len(cfg.KubeadmConfigPatches) != 1 || !strings.Contains(cfg.KubeadmConfigPatches[0], "oidc-issuer-url") {
		t.Fatalf("cluster kubeadm patches = %v", cfg.KubeadmConfigPatches)
	}
	cp := cfg.Nodes[0]
	if len(cp.ExtraMounts) != 2 {
		t.Fatalf("control-plane mounts = %+v, want CA + patch dir", cp.ExtraMounts)
	}
	if cfg.Nodes[1].Role != "worker" || len(cfg.Nodes[1].ExtraMounts) != 0 {
		t.Fatalf("worker mounts = %+v", cfg.Nodes[1].ExtraMounts)
	}
}
