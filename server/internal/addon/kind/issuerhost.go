package kind

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

const (
	kubeadmIssuerHostPatchesDir = "/etc/kubernetes/patches/oidc-issuer-host"
	kubeadmIssuerHostPatchFile  = "kube-apiserver0+merge.yaml"
)

// applyLoopbackIssuerHost extra-mounts a kubeadm hostAliases patch for the
// issuer hostname so kube-apiserver can reach a loopback-forwarded IdP.
// Already-loopback hosts (localhost / 127.0.0.0/8 / ::1) are left unchanged.
func applyLoopbackIssuerHost(config *kindConfig, issuerURL string) error {
	host, err := issuerHostname(issuerURL)
	if err != nil {
		return err
	}
	if isLoopbackHostname(host) {
		return nil
	}
	patchDir, err := writeIssuerHostPatch(host)
	if err != nil {
		return err
	}
	applyIssuerHostOverlay(config, patchDir)
	return nil
}

// issuerHostname returns the host (no port) from issuerURL.
func issuerHostname(issuerURL string) (string, error) {
	u, err := url.Parse(issuerURL)
	if err != nil {
		return "", fmt.Errorf("parse issuer: %w", err)
	}
	host := u.Hostname()
	if host == "" {
		return "", fmt.Errorf("issuer host is empty")
	}
	return host, nil
}

// isLoopbackHostname reports whether host is localhost or a loopback address.
func isLoopbackHostname(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
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
