package kind_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func callerAuth() domain.DeliveryAuth {
	return domain.DeliveryAuth{
		Caller: &domain.SubjectClaims{
			FederatedIdentity: domain.FederatedIdentity{
				Subject: "alice",
				Issuer:  "https://host.docker.internal:9443",
			},
		},
		Audience: []domain.Audience{"fleetshift"},
	}
}

func TestAgent_Deliver_OIDCWithCustomNodes(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()

	agentObs := &recordingAgentObserver{}
	agent := kind.NewAgent(reporter, fakeFactory(provider), kind.WithObserver(agentObs))

	spec := kind.ClusterSpec{
		Name: "multi-oidc",
		Nodes: []kind.NodeSpec{
			{Role: "control-plane"},
			{Role: "worker"},
		},
	}
	specBytes, _ := json.Marshal(spec)

	target := domain.TargetInfo{ID: "k1", Type: kind.TargetType, Name: "local-kind"}
	manifests := []domain.Manifest{{
		ResourceType: kind.ClusterResourceType,
		Raw:          json.RawMessage(specBytes),
	}}

	err := agent.Deliver(context.Background(), target, "d1:k1", manifests, callerAuth(), nil, 1)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	<-reporter.done

	agentObs.mu.Lock()
	defer agentObs.mu.Unlock()

	if len(agentObs.probes) != 1 {
		t.Fatalf("expected 1 probe, got %d", len(agentObs.probes))
	}
	if agentObs.probes[0].source != kind.ConfigSourceOIDC {
		t.Errorf("source = %q, want %q", agentObs.probes[0].source, kind.ConfigSourceOIDC)
	}
	if !provider.hasCluster("multi-oidc") {
		t.Error("cluster was not created")
	}
}

func TestAgent_Deliver_OIDC_EmptyAudience_FailsDelivery(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()

	agent := kind.NewAgent(reporter, fakeFactory(provider))

	auth := domain.DeliveryAuth{
		Caller: &domain.SubjectClaims{
			FederatedIdentity: domain.FederatedIdentity{
				Subject: "alice",
				Issuer:  "https://issuer.example.com",
			},
		},
		// Audience intentionally empty — should fail, not panic.
	}

	manifests := []domain.Manifest{{
		ResourceType: kind.ClusterResourceType,
		Raw:          json.RawMessage(`{"name": "empty-aud"}`),
	}}

	err := agent.Deliver(context.Background(), domain.TargetInfo{}, "d1:k1", manifests, auth, nil, 1)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	result := <-reporter.done
	if result.State != domain.DeliveryStateFailed {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateFailed)
	}
	if provider.hasCluster("empty-aud") {
		t.Error("cluster should not have been created")
	}
}
