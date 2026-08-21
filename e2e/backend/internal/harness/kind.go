package harness

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// HostKindClusterName is the host Kind/podman cluster name for a fleetctl
// Kind cluster id (fs--{id}).
func HostKindClusterName(id string) string {
	return kindClusterNamePrefix + id
}

// KindControlPlaneID returns the host podman id of the Kind control-plane node.
func KindControlPlaneID(hostName string) (string, error) {
	ids, err := kindContainerIDs(
		"--filter", "label="+kindClusterLabel+"="+hostName,
		"--filter", "label="+kindRoleLabel+"="+kindControlPlaneRole,
	)
	if err != nil {
		return "", fmt.Errorf("podman ps control-plane: %w", err)
	}
	if len(ids) == 0 {
		return "", fmt.Errorf("no kind control-plane container for %s", hostName)
	}
	return ids[0], nil
}

// KindNodeIDs returns host podman ids of Kind node containers for hostName.
func KindNodeIDs(hostName string) ([]string, error) {
	ids, err := kindContainerIDs("--filter", "label="+kindClusterLabel+"="+hostName)
	if err != nil {
		return nil, fmt.Errorf("podman ps kind nodes: %w", err)
	}
	return ids, nil
}

// kindContainerIDs runs `podman ps -a` with filters and returns container IDs.
func kindContainerIDs(filters ...string) ([]string, error) {
	args := make([]string, 0, 2+len(filters)+2)
	args = append(args, "ps", "-a")
	args = append(args, filters...)
	args = append(args, "--format", "{{.ID}}")
	cmd := exec.Command("podman", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("%w\n%s", err, trimOutput(out))
	}
	return strings.Fields(string(out)), nil
}

// resolveEngineSocket returns the host unix socket to mount into the AIO.
// PODMAN_SOCKET wins when set; otherwise $XDG_RUNTIME_DIR/podman/podman.sock.
// Missing or non-live sockets fail; this does not start podman system service.
func resolveEngineSocket() (string, error) {
	p := strings.TrimSpace(os.Getenv(engineSocketEnv))
	fromEnv := p != ""
	if !fromEnv {
		xdg := strings.TrimSpace(os.Getenv("XDG_RUNTIME_DIR"))
		if xdg == "" {
			return "", fmt.Errorf("%s is unset and XDG_RUNTIME_DIR is empty; export %s or run: systemctl --user enable --now podman.socket", engineSocketEnv, engineSocketEnv)
		}
		p = filepath.Join(xdg, "podman", "podman.sock")
	}
	if err := requireLiveUnixSocket(p); err != nil {
		if fromEnv {
			return "", fmt.Errorf("%s %s: %w", engineSocketEnv, p, err)
		}
		return "", fmt.Errorf("engine socket %s: %w (export %s or run: systemctl --user enable --now podman.socket)", p, err, engineSocketEnv)
	}
	return p, nil
}

// requireLiveUnixSocket reports an error unless path is a dialable unix socket.
func requireLiveUnixSocket(path string) error {
	st, err := os.Stat(path)
	if err != nil {
		return err
	}
	if st.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("not a unix socket")
	}
	c, err := net.DialTimeout("unix", path, engineSocketDialTimeout)
	if err != nil {
		return fmt.Errorf("not a live unix socket: %w", err)
	}
	_ = c.Close()
	return nil
}

// ensureKindNetwork creates the kind network when it does not exist.
func ensureKindNetwork() error {
	exists := exec.Command("podman", "network", "exists", kindNetwork)
	if err := exists.Run(); err == nil {
		return nil
	}
	cmd := exec.Command("podman", "network", "create", kindNetwork)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("podman network create %s: %w\n%s", kindNetwork, err, trimOutput(out))
	}
	return nil
}

// isSuiteKindCluster reports whether hostName uses this suite's host Kind name prefix.
func isSuiteKindCluster(hostName string) bool {
	return strings.HasPrefix(hostName, suiteHostKindPrefix)
}

// dumpKindEvidence writes host socket and Kind-node container facts to stderr
// so a socket failure can be told from a product failure.
func dumpKindEvidence(containerName, engineSocket string) {
	fmt.Fprintf(os.Stderr, "===== kind/podman evidence =====\n")
	if engineSocket != "" {
		fmt.Fprintf(os.Stderr, "engine socket: %s\n", engineSocket)
		runToStderr("ls", "-l", engineSocket)
	}
	fmt.Fprintf(os.Stderr, "----- host podman ps (kind nodes) -----\n")
	runToStderr("podman", "ps", "-a", "--filter", "label="+kindClusterLabel)
	if containerName == "" {
		return
	}
	fmt.Fprintf(os.Stderr, "----- in-AIO podman ps (kind nodes) -----\n")
	runToStderr("podman", "exec", containerName, "podman", "ps", "-a", "--filter", "label="+kindClusterLabel)
	fmt.Fprintf(os.Stderr, "----- in-AIO docker.sock -----\n")
	runToStderr("podman", "exec", containerName, "ls", "-l", "/var/run/docker.sock")
}

// removeLeftoverKindNodes deletes Kind node containers whose cluster name
// matches this suite's prefix. Best-effort; ignores errors.
func removeLeftoverKindNodes() {
	cmd := exec.Command("podman", "ps", "-a",
		"--filter", "label="+kindClusterLabel,
		"--format", "{{.ID}}\t{{.Label \""+kindClusterLabel+"\"}}",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		id, ok := leftoverKindNodeID(line)
		if !ok {
			continue
		}
		_ = exec.Command("podman", "rm", "-f", id).Run()
	}
}

// leftoverKindNodeID returns the container id from a podman ps tab line when
// the Kind cluster name uses this suite's host prefix.
func leftoverKindNodeID(line string) (id string, ok bool) {
	id, name, ok := parseKindPSLine(line)
	if !ok || !isSuiteKindCluster(name) {
		return "", false
	}
	return id, true
}

// parseKindPSLine splits a `podman ps --format '{{.ID}}\t{{.Label ...}}'` line.
// ok is false for blank lines, missing tabs, or empty id/name.
func parseKindPSLine(line string) (id, name string, ok bool) {
	if strings.TrimSpace(line) == "" {
		return "", "", false
	}
	id, name, ok = strings.Cut(line, "\t")
	if !ok {
		return "", "", false
	}
	id = strings.TrimSpace(id)
	name = strings.TrimSpace(name)
	if id == "" || name == "" {
		return "", "", false
	}
	return id, name, true
}
