package harness

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// linuxEngineHost is true on Linux: discover only $XDG_RUNTIME_DIR/podman/podman.sock,
// never /var/run/docker.sock (that path is Docker).
var linuxEngineHost = runtime.GOOS == "linux"

// lookupRemoteEngineSocket returns Host.RemoteSocket.Path from `podman info`.
var lookupRemoteEngineSocket = podmanRemoteSocketPath

// dockerCompatSocketPath is the Docker / podman-mac-helper well-known socket.
var dockerCompatSocketPath = dockerCompatSocket

// kindAPIContainerPort is the Kubernetes API port inside a Kind node.
const kindAPIContainerPort = "6443"

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

// KindHostAPI returns a host-reachable Kubernetes API URL (https://127.0.0.1:port)
// and the cluster CA PEM for the Kind cluster named hostName (fs--{id}).
func KindHostAPI(hostName string) (apiURL string, caPEM []byte, err error) {
	id, err := KindControlPlaneID(hostName)
	if err != nil {
		return "", nil, err
	}
	cmd := exec.Command("podman", "port", id, kindAPIContainerPort)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", nil, fmt.Errorf("podman port %s: %w\n%s", kindAPIContainerPort, err, trimOutput(out))
	}
	apiURL, err = parsePodmanPort(string(out))
	if err != nil {
		return "", nil, err
	}
	cmd = exec.Command("podman", "exec", id, "cat", "/etc/kubernetes/pki/ca.crt")
	caPEM, err = cmd.CombinedOutput()
	if err != nil {
		return "", nil, fmt.Errorf("read kind CA: %w\n%s", err, trimOutput(caPEM))
	}
	if !strings.Contains(string(caPEM), "BEGIN CERTIFICATE") {
		return "", nil, fmt.Errorf("kind CA is not a PEM certificate")
	}
	return apiURL, caPEM, nil
}

// parsePodmanPort turns `podman port` output into https://host:port.
// Unspecified bind addresses (0.0.0.0, ::) become 127.0.0.1. IPv4 loopback
// wins when both families are published.
func parsePodmanPort(raw string) (string, error) {
	var candidates []string
	for line := range strings.SplitSeq(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if _, after, ok := strings.Cut(line, "->"); ok {
			line = strings.TrimSpace(after)
		}
		host, port, err := net.SplitHostPort(line)
		if err != nil {
			continue
		}
		switch host {
		case "", "0.0.0.0", "*", "::":
			host = "127.0.0.1"
		}
		candidates = append(candidates, net.JoinHostPort(host, port))
	}
	if len(candidates) == 0 {
		return "", fmt.Errorf("parse podman port output %q", strings.TrimSpace(raw))
	}
	for _, c := range candidates {
		host, _, err := net.SplitHostPort(c)
		if err != nil {
			continue
		}
		if host == "127.0.0.1" {
			return "https://" + c, nil
		}
	}
	return "https://" + candidates[0], nil
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

// resolveEngineSocket returns the host unix socket to mount into the AIO at
// /var/run/docker.sock. The path must be a live unix socket on this filesystem
// so `podman run -v` can bind-mount it. PODMAN_SOCKET wins when set; otherwise
// $XDG_RUNTIME_DIR/podman/podman.sock. When both are empty on a non-Linux host,
// a live /var/run/docker.sock (podman-mac-helper symlink) wins, then a live
// `podman info` RemoteSocket.Path. This does not start podman system service
// or the machine.
func resolveEngineSocket() (string, error) {
	p := strings.TrimSpace(os.Getenv(engineSocketEnv))
	fromEnv := p != ""
	fromRemote := false
	if !fromEnv {
		xdg := strings.TrimSpace(os.Getenv("XDG_RUNTIME_DIR"))
		if xdg != "" {
			p = filepath.Join(xdg, "podman", "podman.sock")
		} else if linuxEngineHost {
			return "", fmt.Errorf("%s is unset and XDG_RUNTIME_DIR is empty; export %s or run: systemctl --user enable --now podman.socket", engineSocketEnv, engineSocketEnv)
		} else if sock, ok := liveDockerCompatSocket(); ok {
			return sock, nil
		} else {
			rp, err := lookupRemoteEngineSocket()
			if err != nil {
				return "", fmt.Errorf("%s is unset and podman did not report a remote socket: %w; export %s or run: podman machine start", engineSocketEnv, err, engineSocketEnv)
			}
			p = rp
			fromRemote = true
		}
	}
	if err := requireLiveUnixSocket(p); err != nil {
		if fromEnv {
			return "", fmt.Errorf("%s %s: %w", engineSocketEnv, p, err)
		}
		if fromRemote {
			return "", fmt.Errorf("engine socket %s: %w (export %s or run: podman machine start)", p, err, engineSocketEnv)
		}
		return "", fmt.Errorf("engine socket %s: %w (export %s or run: systemctl --user enable --now podman.socket)", p, err, engineSocketEnv)
	}
	return p, nil
}

// liveDockerCompatSocket returns dockerCompatSocketPath when it is a dialable unix socket.
func liveDockerCompatSocket() (string, bool) {
	if err := requireLiveUnixSocket(dockerCompatSocketPath); err != nil {
		return "", false
	}
	return dockerCompatSocketPath, true
}

// podmanRemoteSocketPath returns Host.RemoteSocket.Path from `podman info`
// without a unix:// prefix.
func podmanRemoteSocketPath() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), commandTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "podman", "info", "--format", "{{.Host.RemoteSocket.Path}}")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("podman info: %w\n%s", err, trimOutput(out))
	}
	return parseRemoteSocketPath(string(out))
}

// parseRemoteSocketPath strips unix:// and whitespace from a podman info
// RemoteSocket.Path value.
func parseRemoteSocketPath(raw string) (string, error) {
	p := strings.TrimSpace(raw)
	switch {
	case p == "":
		return "", fmt.Errorf("empty remote socket path")
	case strings.HasPrefix(p, "unix://"):
		p = strings.TrimPrefix(p, "unix://")
	case strings.Contains(p, "://"):
		return "", fmt.Errorf("remote socket %s is not a unix path", p)
	}
	if p == "" {
		return "", fmt.Errorf("empty remote socket path")
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

// dumpKindEvidence writes host vs AIO-via-mounted-socket Kind-node facts to
// stderr so a socket/engine mismatch can be told from a product failure.
func dumpKindEvidence(containerName, engineSocket string) {
	fmt.Fprintf(os.Stderr, "===== kind/podman evidence =====\n")
	if engineSocket != "" {
		fmt.Fprintf(os.Stderr, "engine socket: %s\n", engineSocket)
		runToStderr("ls", "-l", engineSocket)
	}
	fmt.Fprintf(os.Stderr, "----- host engine: podman ps (kind nodes) -----\n")
	runToStderr("podman", "ps", "-a", "--filter", "label="+kindClusterLabel)
	if containerName == "" {
		return
	}
	fmt.Fprintf(os.Stderr, "----- AIO via mounted socket (kind nodes) -----\n")
	runToStderr("podman", "exec", containerName, "podman", "ps", "-a", "--filter", "label="+kindClusterLabel)
	fmt.Fprintf(os.Stderr, "----- AIO %s -----\n", containerEngineSocket)
	runToStderr("podman", "exec", containerName, "ls", "-l", containerEngineSocket)
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
