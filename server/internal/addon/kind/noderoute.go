package kind

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	"sigs.k8s.io/kind/pkg/cluster/nodes"
	"sigs.k8s.io/kind/pkg/cluster/nodeutils"
)

// NodeRouteBackendEnv enables a create/delete-time loopback forward from
// kind control-plane nodes to a TCP destination (host:port). Packaging sets this
// for peer-Dex AIO launches; empty means no hook (no-op).
const NodeRouteBackendEnv = "KIND_NODE_ROUTE_BACKEND"

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

// loopbackNodeRoute DNATs 127.0.0.1:<listenPort> to destination inside each
// control-plane node via the kind [ClusterProvider] (docker or podman).
type loopbackNodeRoute struct {
	destination string // host:port
	listenPort  string
}

// NewNodeRoute builds a [NodeRoute] that DNATs kind control-plane
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

// Ensure idempotently installs OUTPUT DNAT rules on all control-plane nodes
// for the kind cluster.
func (h *loopbackNodeRoute) Ensure(ctx context.Context, provider ClusterProvider, kindClusterName string) error {
	cps, err := listControlPlaneNodes(provider, kindClusterName)
	if err != nil {
		return err
	}
	if len(cps) == 0 {
		return fmt.Errorf("no control-plane nodes for kind cluster %q", kindClusterName)
	}
	for _, n := range cps {
		if err := h.execIptables(ctx, n, true); err != nil {
			return fmt.Errorf("ensure route on %s: %w", n.String(), err)
		}
	}
	return nil
}

// Remove best-effort deletes the DNAT rules. Missing rules or nodes are ignored.
func (h *loopbackNodeRoute) Remove(ctx context.Context, provider ClusterProvider, kindClusterName string) error {
	cps, err := listControlPlaneNodes(provider, kindClusterName)
	if err != nil {
		return err
	}
	for _, n := range cps {
		_ = h.execIptables(ctx, n, false)
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

// execIptables adds (ensure) or deletes (remove) the DNAT rule inside node.
func (h *loopbackNodeRoute) execIptables(ctx context.Context, node nodes.Node, ensure bool) error {
	mode := "remove"
	if ensure {
		mode = "ensure"
	}
	// Prefer iptables-legacy (kind nodes); fall back to iptables.
	const script = `
ipt() { iptables-legacy "$@" 2>/dev/null || iptables "$@"; }
if [ "$MODE" = ensure ]; then
  ipt -t nat -C OUTPUT -p tcp -d 127.0.0.1 --dport "$LISTEN_PORT" -j DNAT --to-destination "$DESTINATION" 2>/dev/null \
    || ipt -t nat -A OUTPUT -p tcp -d 127.0.0.1 --dport "$LISTEN_PORT" -j DNAT --to-destination "$DESTINATION"
else
  ipt -t nat -D OUTPUT -p tcp -d 127.0.0.1 --dport "$LISTEN_PORT" -j DNAT --to-destination "$DESTINATION" 2>/dev/null || true
fi
`
	var stderr bytes.Buffer
	cmd := node.CommandContext(ctx, "sh", "-c", script)
	cmd.SetEnv(
		"LISTEN_PORT="+h.listenPort,
		"DESTINATION="+h.destination,
		"MODE="+mode,
	)
	cmd.SetStderr(&stderr)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}
