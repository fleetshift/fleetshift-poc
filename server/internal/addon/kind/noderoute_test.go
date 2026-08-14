package kind_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"sigs.k8s.io/kind/pkg/cluster"
	"sigs.k8s.io/kind/pkg/cluster/nodes"
	"sigs.k8s.io/kind/pkg/exec"
)

func TestNewNodeRoute(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		destination string
		wantErr     bool
	}{
		{name: "valid", destination: "10.89.0.2:5556"},
		{name: "dns name", destination: "fleetshift:5556"},
		{name: "invalid hostport", destination: "not-a-hostport", wantErr: true},
		{name: "empty host", destination: ":5556", wantErr: true},
		{name: "invalid port", destination: "10.89.0.2:0", wantErr: true},
		{name: "non-numeric port", destination: "10.89.0.2:abc", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, err := kind.NewNodeRoute(tt.destination)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("NewNodeRoute: %v", err)
			}
			if h == nil {
				t.Fatal("route is nil")
			}
		})
	}
}

type recordingNodeRoute struct {
	ensured   []string
	removed   []string
	ensureErr error
}

func (r *recordingNodeRoute) Ensure(_ context.Context, _ kind.ClusterProvider, name string) error {
	r.ensured = append(r.ensured, name)
	return r.ensureErr
}

func (r *recordingNodeRoute) Remove(_ context.Context, _ kind.ClusterProvider, name string) error {
	r.removed = append(r.removed, name)
	return nil
}

func TestAgent_EnsureCluster_CallsNodeRoute(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()
	route := &recordingNodeRoute{}
	agent, _ := newTestAgent(reporter, provider, kind.WithNodeRoute(route))

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "k1", Type: kind.TargetType, Name: "local-kind"})
	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name":"route-me"}`),
	}}

	if err := agent.Deliver(context.Background(), target, "d1:t1", manifests, domain.DeliveryAuth{}, nil, 1); err != nil {
		t.Fatalf("first Deliver: %v", err)
	}
	first := awaitDone(t, reporter.done)
	if first.State != domain.DeliveryStateDelivered {
		t.Fatalf("first State = %q (%s)", first.State, first.Message)
	}
	if len(route.ensured) != 1 || route.ensured[0] != "fs--route-me" {
		t.Fatalf("after create ensured = %v, want [fs--route-me]", route.ensured)
	}

	route.ensured = nil
	if err := agent.Deliver(context.Background(), target, "d2:t1", manifests, domain.DeliveryAuth{}, nil, 1); err != nil {
		t.Fatalf("second Deliver: %v", err)
	}
	second := awaitDone(t, reporter.done)
	if second.State != domain.DeliveryStateDelivered {
		t.Fatalf("second State = %q (%s)", second.State, second.Message)
	}
	if len(route.ensured) != 1 || route.ensured[0] != "fs--route-me" {
		t.Fatalf("after ensure ensured = %v, want [fs--route-me]", route.ensured)
	}
}

func TestAgent_EnsureCluster_NodeRouteErrorFailsDelivery(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()
	route := &recordingNodeRoute{ensureErr: errors.New("proxy install failed")}
	agent, _ := newTestAgent(reporter, provider, kind.WithNodeRoute(route))

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "k1", Type: kind.TargetType, Name: "local-kind"})
	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name":"route-fail"}`),
	}}
	if err := agent.Deliver(context.Background(), target, "d1:t1", manifests, domain.DeliveryAuth{}, nil, 1); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	res := awaitDone(t, reporter.done)
	if res.State != domain.DeliveryStateFailed {
		t.Fatalf("state = %q, want failed (message=%s)", res.State, res.Message)
	}
	if !strings.Contains(res.Message, "node route") {
		t.Fatalf("message = %q, want node route error", res.Message)
	}
}

func TestAgent_Remove_CallsNodeRoute(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()
	route := &recordingNodeRoute{}
	agent, _ := newTestAgent(reporter, provider, kind.WithNodeRoute(route))

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "k1", Type: kind.TargetType, Name: "local-kind"})
	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name":"gone"}`),
	}}
	if err := agent.Deliver(context.Background(), target, "d1:t1", manifests, domain.DeliveryAuth{}, nil, 1); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if res := awaitDone(t, reporter.done); res.State != domain.DeliveryStateDelivered {
		t.Fatalf("deliver State = %q (%s)", res.State, res.Message)
	}

	if err := agent.Remove(context.Background(), target, "d2:t1", manifests, domain.DeliveryAuth{}, nil, 1); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if res := awaitDone(t, reporter.done); res.State != domain.DeliveryStateDelivered {
		t.Fatalf("remove State = %q (%s)", res.State, res.Message)
	}
	if len(route.removed) != 1 || route.removed[0] != "fs--gone" {
		t.Fatalf("removed = %v, want [fs--gone]", route.removed)
	}
}

func TestLoopbackNodeRoute_Ensure_InstallsSystemdProxy(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "kind-loopback-forward")
	payload := []byte("fake-forwarder-binary")
	if err := os.WriteFile(bin, payload, 0755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kind.OverrideLoopbackForwardBin(bin))

	node := &recordingKindNode{name: "cp-1", role: "control-plane"}
	route, err := kind.NewNodeRoute("fleetshift:5556")
	if err != nil {
		t.Fatal(err)
	}
	if err := route.Ensure(context.Background(), &staticNodeProvider{nodes: []nodes.Node{node}}, "fs--x"); err != nil {
		t.Fatal(err)
	}
	if len(node.cmds) != 1 {
		t.Fatalf("cmds = %d, want 1", len(node.cmds))
	}
	cmd := node.cmds[0]
	if cmd.name != "sh" || len(cmd.args) != 2 || cmd.args[0] != "-c" {
		t.Fatalf("command = %s %v", cmd.name, cmd.args)
	}
	script := cmd.args[1]
	if strings.Contains(script, "iptables") {
		t.Fatal("install script still uses iptables")
	}
	for _, want := range []string{
		"systemctl restart kind-loopback-forward.service",
		"-listen 127.0.0.1:${LISTEN_PORT}",
		"-to ${DESTINATION}",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("script missing %q:\n%s", want, script)
		}
	}
	if !bytes.Equal(cmd.stdin, payload) {
		t.Fatalf("stdin = %q, want helper binary", cmd.stdin)
	}
	if !envHas(cmd.env, "DESTINATION=fleetshift:5556") || !envHas(cmd.env, "LISTEN_PORT=5556") || !envHas(cmd.env, "MODE=ensure") {
		t.Fatalf("env = %v", cmd.env)
	}
}

func TestLoopbackNodeRoute_Ensure_MissingBinary(t *testing.T) {
	t.Cleanup(kind.OverrideLoopbackForwardBin(filepath.Join(t.TempDir(), "missing")))
	node := &recordingKindNode{name: "cp-1", role: "control-plane"}
	route, err := kind.NewNodeRoute("fleetshift:5556")
	if err != nil {
		t.Fatal(err)
	}
	err = route.Ensure(context.Background(), &staticNodeProvider{nodes: []nodes.Node{node}}, "fs--x")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "read loopback forwarder") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoopbackNodeRoute_Remove_StopsUnit(t *testing.T) {
	node := &recordingKindNode{name: "cp-1", role: "control-plane"}
	route, err := kind.NewNodeRoute("fleetshift:5556")
	if err != nil {
		t.Fatal(err)
	}
	if err := route.Remove(context.Background(), &staticNodeProvider{nodes: []nodes.Node{node}}, "fs--x"); err != nil {
		t.Fatal(err)
	}
	if len(node.cmds) != 1 {
		t.Fatalf("cmds = %d, want 1", len(node.cmds))
	}
	cmd := node.cmds[0]
	if !strings.Contains(cmd.args[1], "systemctl disable --now kind-loopback-forward.service") {
		t.Fatalf("script = %s", cmd.args[1])
	}
	if envHas(cmd.env, "MODE=ensure") || !envHas(cmd.env, "MODE=remove") {
		t.Fatalf("env = %v", cmd.env)
	}
	if len(cmd.stdin) != 0 {
		t.Fatalf("remove should not pipe a binary, stdin=%q", cmd.stdin)
	}
}

func TestLoopbackNodeRoute_Ensure_NoControlPlane(t *testing.T) {
	route, err := kind.NewNodeRoute("fleetshift:5556")
	if err != nil {
		t.Fatal(err)
	}
	worker := &recordingKindNode{name: "w-1", role: "worker"}
	err = route.Ensure(context.Background(), &staticNodeProvider{nodes: []nodes.Node{worker}}, "fs--x")
	if err == nil || !strings.Contains(err.Error(), "no control-plane nodes") {
		t.Fatalf("error = %v, want no control-plane nodes", err)
	}
}

func TestLoopbackNodeRoute_Ensure_EmptyBinary(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "kind-loopback-forward")
	if err := os.WriteFile(bin, nil, 0755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kind.OverrideLoopbackForwardBin(bin))
	node := &recordingKindNode{name: "cp-1", role: "control-plane"}
	route, err := kind.NewNodeRoute("fleetshift:5556")
	if err != nil {
		t.Fatal(err)
	}
	err = route.Ensure(context.Background(), &staticNodeProvider{nodes: []nodes.Node{node}}, "fs--x")
	if err == nil || !strings.Contains(err.Error(), "is empty") {
		t.Fatalf("error = %v, want empty helper", err)
	}
}

func TestLoopbackNodeRoute_Ensure_CommandError(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "kind-loopback-forward")
	if err := os.WriteFile(bin, []byte("fake"), 0755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kind.OverrideLoopbackForwardBin(bin))
	node := &recordingKindNode{name: "cp-1", role: "control-plane", cmdErr: errors.New("systemctl failed")}
	route, err := kind.NewNodeRoute("fleetshift:5556")
	if err != nil {
		t.Fatal(err)
	}
	err = route.Ensure(context.Background(), &staticNodeProvider{nodes: []nodes.Node{node}}, "fs--x")
	if err == nil || !strings.Contains(err.Error(), "ensure route on cp-1") || !strings.Contains(err.Error(), "systemctl failed") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoopbackNodeRoute_Ensure_DefaultBinaryPath(t *testing.T) {
	node := &recordingKindNode{name: "cp-1", role: "control-plane"}
	route, err := kind.NewNodeRoute("fleetshift:5556")
	if err != nil {
		t.Fatal(err)
	}
	err = route.Ensure(context.Background(), &staticNodeProvider{nodes: []nodes.Node{node}}, "fs--x")
	if err != nil {
		if !strings.Contains(err.Error(), "/usr/local/bin/kind-loopback-forward") {
			t.Fatalf("error = %v, want default helper path", err)
		}
		return
	}
	if len(node.cmds) != 1 {
		t.Fatal("default helper present: expected install command")
	}
}

func envHas(env []string, want string) bool {
	for _, e := range env {
		if e == want {
			return true
		}
	}
	return false
}

type staticNodeProvider struct {
	nodes []nodes.Node
}

func (p *staticNodeProvider) Create(string, ...cluster.CreateOption) error { return nil }
func (p *staticNodeProvider) Delete(string, string) error                  { return nil }
func (p *staticNodeProvider) List() ([]string, error)                      { return nil, nil }
func (p *staticNodeProvider) ListNodes(string) ([]nodes.Node, error)       { return p.nodes, nil }
func (p *staticNodeProvider) KubeConfig(string, bool) (string, error)      { return "", nil }

type recordingKindNode struct {
	name   string
	role   string
	cmdErr error
	cmds   []*recordingKindCmd
}

func (n *recordingKindNode) String() string              { return n.name }
func (n *recordingKindNode) Role() (string, error)       { return n.role, nil }
func (n *recordingKindNode) IP() (string, string, error) { return "10.89.0.4", "", nil }
func (n *recordingKindNode) SerialLogs(io.Writer) error  { return nil }
func (n *recordingKindNode) Command(command string, args ...string) exec.Cmd {
	return n.CommandContext(context.Background(), command, args...)
}
func (n *recordingKindNode) CommandContext(_ context.Context, command string, args ...string) exec.Cmd {
	c := &recordingKindCmd{name: command, args: append([]string(nil), args...), runErr: n.cmdErr}
	n.cmds = append(n.cmds, c)
	return c
}

type recordingKindCmd struct {
	name   string
	args   []string
	env    []string
	stdin  []byte
	runErr error
}

func (c *recordingKindCmd) Run() error { return c.runErr }
func (c *recordingKindCmd) SetEnv(env ...string) exec.Cmd {
	c.env = append([]string(nil), env...)
	return c
}
func (c *recordingKindCmd) SetStdin(r io.Reader) exec.Cmd {
	if r == nil {
		c.stdin = nil
		return c
	}
	b, err := io.ReadAll(r)
	if err != nil {
		panic(err)
	}
	c.stdin = b
	return c
}
func (c *recordingKindCmd) SetStdout(io.Writer) exec.Cmd { return c }
func (c *recordingKindCmd) SetStderr(io.Writer) exec.Cmd { return c }
