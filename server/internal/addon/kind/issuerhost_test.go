package kind

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateLoopbackIssuerHost(t *testing.T) {
	if err := validateLoopbackIssuerHost("https://fleetshift-sandbox.localhost:8085/dex", "fleetshift-sandbox.localhost"); err != nil {
		t.Fatal(err)
	}
	err := validateLoopbackIssuerHost("https://127.0.0.1:8085/dex", "fleetshift-sandbox.localhost")
	if err == nil || !strings.Contains(err.Error(), "does not match issuer host") {
		t.Fatalf("validateLoopbackIssuerHost() = %v, want mismatch", err)
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
