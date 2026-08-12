package kind_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestNewNodeRoute(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		destination string
		wantErr     bool
	}{
		{name: "valid", destination: "10.89.0.2:5556"},
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
	route := &recordingNodeRoute{ensureErr: errors.New("dnat failed")}
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
