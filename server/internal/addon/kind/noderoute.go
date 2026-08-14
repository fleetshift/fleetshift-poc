package kind

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"

	"sigs.k8s.io/kind/pkg/cluster/nodes"
	"sigs.k8s.io/kind/pkg/cluster/nodeutils"
)

// NodeRouteBackendEnv enables a create/delete-time loopback forward from
// kind control-plane nodes to a TCP destination (host:port). Packaging sets this
// for peer-Dex AIO launches; empty means no hook (no-op).
const NodeRouteBackendEnv = "KIND_NODE_ROUTE_BACKEND"

// loopbackForwardBin is the helper copied into kind nodes. Tests override it.
var loopbackForwardBin = "/usr/local/bin/kind-loopback-forward"

// NodeRoute installs or removes a node-local loopback forward on kind
// control-plane containers so 127.0.0.1:<listenPort> reaches a destination.
// The Kind addon stays IdP-agnostic: it only forwards bytes to the configured
// destination. Nil means no-op.
type NodeRoute interface {
	// Ensure idempotently installs the loopback forward on control-plane nodes
	// for kindClusterName using provider.ListNodes.
	Ensure(ctx context.Context, provider ClusterProvider, kindClusterName string) error
	// Remove best-effort deletes the loopback forward; missing state is ignored.
	Remove(ctx context.Context, provider ClusterProvider, kindClusterName string) error
}

// loopbackNodeRoute runs a TCP proxy on 127.0.0.1:<listenPort> inside each
// control-plane node, forwarding to destination via the kind [ClusterProvider]
// (docker or podman). systemd owns the process so it survives podman exec.
type loopbackNodeRoute struct {
	destination string // host:port
	listenPort  string
}

// NewNodeRoute builds a [NodeRoute] that forwards kind control-plane
// loopback traffic for destination's port to destination. destination must be host:port.
func NewNodeRoute(destination string) (NodeRoute, error) {
	host, portStr, err := net.SplitHostPort(strings.TrimSpace(destination))
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", NodeRouteBackendEnv, err)
	}
	if host == "" {
		return nil, fmt.Errorf("%s host is empty", NodeRouteBackendEnv)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return nil, fmt.Errorf("%s port %q invalid", NodeRouteBackendEnv, portStr)
	}
	return &loopbackNodeRoute{
		destination: net.JoinHostPort(host, portStr),
		listenPort:  portStr,
	}, nil
}

// Ensure idempotently installs the loopback TCP proxy on all control-plane nodes
// for the kind cluster.
func (h *loopbackNodeRoute) Ensure(ctx context.Context, provider ClusterProvider, kindClusterName string) error {
	cps, err := listControlPlaneNodes(provider, kindClusterName)
	if err != nil {
		return err
	}
	if len(cps) == 0 {
		return fmt.Errorf("no control-plane nodes for kind cluster %q", kindClusterName)
	}
	bin, err := os.ReadFile(loopbackForwardBin)
	if err != nil {
		return fmt.Errorf("read loopback forwarder: %w", err)
	}
	if len(bin) == 0 {
		return fmt.Errorf("loopback forwarder %s is empty", loopbackForwardBin)
	}
	for _, n := range cps {
		if err := h.execForwarder(ctx, n, bin); err != nil {
			return fmt.Errorf("ensure route on %s: %w", n.String(), err)
		}
	}
	return nil
}

// Remove best-effort deletes the loopback proxy unit. Missing state is ignored.
func (h *loopbackNodeRoute) Remove(ctx context.Context, provider ClusterProvider, kindClusterName string) error {
	cps, err := listControlPlaneNodes(provider, kindClusterName)
	if err != nil {
		return err
	}
	for _, n := range cps {
		_ = h.execForwarder(ctx, n, nil)
	}
	return nil
}

// listControlPlaneNodes returns kind control-plane nodes for the cluster.
func listControlPlaneNodes(provider ClusterProvider, kindClusterName string) ([]nodes.Node, error) {
	all, err := provider.ListNodes(kindClusterName)
	if err != nil {
		return nil, fmt.Errorf("list kind nodes: %w", err)
	}
	cps, err := nodeutils.ControlPlaneNodes(all)
	if err != nil {
		return nil, fmt.Errorf("select control-plane nodes: %w", err)
	}
	return cps, nil
}

// execForwarder installs (bin != nil) or removes the systemd unit inside node.
func (h *loopbackNodeRoute) execForwarder(ctx context.Context, node nodes.Node, bin []byte) error {
	mode := "remove"
	var stdin io.Reader
	if bin != nil {
		mode = "ensure"
		stdin = bytes.NewReader(bin)
	}
	const script = `
set -e
BIN=/usr/local/bin/kind-loopback-forward
UNIT=/etc/systemd/system/kind-loopback-forward.service
if [ "$MODE" = ensure ]; then
  cat > "$BIN"
  chmod 0755 "$BIN"
  cat > "$UNIT" <<EOF
[Unit]
Description=kind loopback TCP forward
After=network-online.target

[Service]
ExecStart=/usr/local/bin/kind-loopback-forward -listen 127.0.0.1:${LISTEN_PORT} -to ${DESTINATION}
Restart=always
RestartSec=1

[Install]
WantedBy=multi-user.target
EOF
  systemctl daemon-reload
  systemctl enable kind-loopback-forward.service
  systemctl restart kind-loopback-forward.service
else
  systemctl disable --now kind-loopback-forward.service 2>/dev/null || true
  rm -f "$UNIT" "$BIN"
  systemctl daemon-reload 2>/dev/null || true
fi
`
	var stderr bytes.Buffer
	cmd := node.CommandContext(ctx, "sh", "-c", script)
	cmd.SetEnv(
		"LISTEN_PORT="+h.listenPort,
		"DESTINATION="+h.destination,
		"MODE="+mode,
	)
	if stdin != nil {
		cmd.SetStdin(stdin)
	}
	cmd.SetStderr(&stderr)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}
