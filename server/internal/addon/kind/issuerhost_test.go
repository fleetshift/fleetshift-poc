package kind

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
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
	n := cfg.Nodes[0]
	if len(n.ExtraMounts) != 1 {
		t.Fatalf("mounts = %+v", n.ExtraMounts)
	}
	t.Cleanup(func() { _ = os.RemoveAll(n.ExtraMounts[0].HostPath) })
	raw, err := os.ReadFile(filepath.Join(n.ExtraMounts[0].HostPath, kubeadmIssuerHostPatchFile))
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
	assertIssuerHostKubeadmPatches(t, n)
}

type nopLoopbackForward struct{}

func (nopLoopbackForward) Ensure(context.Context, ClusterProvider, string) error { return nil }
func (nopLoopbackForward) Remove(context.Context, ClusterProvider, string) error { return nil }

func TestResolveConfig_LoopbackForwardAddsIssuerHost(t *testing.T) {
	spec := ClusterSpec{
		Name:  "oidc",
		Nodes: []NodeSpec{{Role: "control-plane"}, {Role: "worker"}},
	}
	auth := domain.DeliveryAuth{
		Caller: &domain.SubjectClaims{
			FederatedIdentity: domain.FederatedIdentity{
				Subject: "alice",
				Issuer:  "https://fleetshift-sandbox.localhost:8085/dex",
			},
		},
		Audience: []domain.Audience{"fleetshift"},
	}

	t.Run("with loopback forward", func(t *testing.T) {
		a := &Agent{loopbackForward: nopLoopbackForward{}}
		raw, source, err := a.resolveConfig(spec, auth)
		if err != nil {
			t.Fatal(err)
		}
		if source != ConfigSourceOIDC {
			t.Fatalf("source = %q, want %q", source, ConfigSourceOIDC)
		}
		cfg := unmarshalKindConfig(t, raw)
		cleanupIssuerHostMounts(t, cfg)
		if len(cfg.Nodes) != 2 {
			t.Fatalf("nodes = %d", len(cfg.Nodes))
		}
		assertIssuerHostKubeadmPatches(t, cfg.Nodes[0])
		if len(cfg.Nodes[0].ExtraMounts) != 1 {
			t.Fatalf("control-plane mounts = %+v", cfg.Nodes[0].ExtraMounts)
		}
		if cfg.Nodes[1].Role != "worker" || len(cfg.Nodes[1].ExtraMounts) != 0 || len(cfg.Nodes[1].KubeadmConfigPatches) != 0 {
			t.Fatalf("worker unexpectedly received issuer-host overlay: %+v", cfg.Nodes[1])
		}
	})

	t.Run("without loopback forward", func(t *testing.T) {
		a := &Agent{}
		raw, source, err := a.resolveConfig(spec, auth)
		if err != nil {
			t.Fatal(err)
		}
		if source != ConfigSourceOIDC {
			t.Fatalf("source = %q, want %q", source, ConfigSourceOIDC)
		}
		cfg := unmarshalKindConfig(t, raw)
		if len(cfg.Nodes[0].ExtraMounts) != 0 || len(cfg.Nodes[0].KubeadmConfigPatches) != 0 {
			t.Fatalf("nil loopbackForward should not add hostAliases: %+v", cfg.Nodes[0])
		}
	})
}

func unmarshalKindConfig(t *testing.T, raw []byte) kindConfig {
	t.Helper()
	var cfg kindConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func cleanupIssuerHostMounts(t *testing.T, cfg kindConfig) {
	t.Helper()
	for _, n := range cfg.Nodes {
		for _, m := range n.ExtraMounts {
			if m.ContainerPath == kubeadmIssuerHostPatchesDir {
				t.Cleanup(func() { _ = os.RemoveAll(m.HostPath) })
			}
		}
	}
}

func assertIssuerHostKubeadmPatches(t *testing.T, n kindNode) {
	t.Helper()
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
	if !hasInit || !hasJoin {
		t.Fatalf("missing issuer-host kubeadm patches init=%v join=%v patches=%v", hasInit, hasJoin, n.KubeadmConfigPatches)
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
