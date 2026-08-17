package kind

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
)

const (
	// LoopbackIssuerHostEnv names the peer-Dex issuer host injected into
	// kube-apiserver hostAliases.
	LoopbackIssuerHostEnv = "KIND_LOOPBACK_ISSUER_HOST"

	kubeadmIssuerHostPatchesDir = "/etc/kubernetes/patches/oidc-issuer-host"
	kubeadmIssuerHostPatchFile  = "kube-apiserver0+merge.yaml"
)

// validateLoopbackIssuerHost requires hostname to equal the issuer URL host.
func validateLoopbackIssuerHost(issuerURL, hostname string) error {
	u, err := url.Parse(issuerURL)
	if err != nil {
		return fmt.Errorf("%s: parse issuer: %w", LoopbackIssuerHostEnv, err)
	}
	if u.Hostname() != hostname {
		return fmt.Errorf("%s %q does not match issuer host %q", LoopbackIssuerHostEnv, hostname, u.Hostname())
	}
	return nil
}

// writeIssuerHostPatch writes a kubeadm strategic-merge patch that adds
// hostAliases for hostname and returns the host-visible patch directory.
func writeIssuerHostPatch(hostname string) (string, error) {
	dir, err := os.MkdirTemp("", "kind-oidc-hostalias-*")
	if err != nil {
		return "", fmt.Errorf("create issuer-host patch dir: %w", err)
	}
	body := fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: kube-apiserver
  namespace: kube-system
spec:
  hostAliases:
    - ip: "127.0.0.1"
      hostnames:
        - %s
`, hostname)
	path := filepath.Join(dir, kubeadmIssuerHostPatchFile)
	if err := os.WriteFile(path, []byte(body), 0644); err != nil {
		os.RemoveAll(dir)
		return "", fmt.Errorf("write issuer-host patch: %w", err)
	}
	return dir, nil
}

// applyIssuerHostOverlay extra-mounts the kubeadm patch directory into every
// control-plane node and points InitConfiguration/JoinConfiguration at it.
// Worker-only nodes are left unchanged.
func applyIssuerHostOverlay(config *kindConfig, patchHostPath string) {
	mount := kindMount{
		HostPath:      patchHostPath,
		ContainerPath: kubeadmIssuerHostPatchesDir,
		ReadOnly:      true,
	}
	patches := []string{
		fmt.Sprintf("kind: InitConfiguration\npatches:\n  directory: %q\n", kubeadmIssuerHostPatchesDir),
		fmt.Sprintf("kind: JoinConfiguration\npatches:\n  directory: %q\n", kubeadmIssuerHostPatchesDir),
	}
	for i := range config.Nodes {
		if config.Nodes[i].Role != "control-plane" {
			continue
		}
		config.Nodes[i].ExtraMounts = append(config.Nodes[i].ExtraMounts, mount)
		config.Nodes[i].KubeadmConfigPatches = append(config.Nodes[i].KubeadmConfigPatches, patches...)
	}
}
