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
	agent := kind.NewAgent(reporter, fakeFactory(provider), kind.WithGenerationStore(kind.NewMemoryGenerationStore()), kind.WithObserver(agentObs))

	spec := kind.ClusterSpec{
		Name: "multi-oidc",
		Nodes: []kind.NodeSpec{
			{Role: "control-plane"},
			{Role: "worker"},
		},
	}
	specBytes, _ := json.Marshal(spec)

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "k1", Type: kind.TargetType, Name: "local-kind"})
	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
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
	if !provider.hasCluster("fs--multi-oidc") {
		t.Error("cluster was not created")
	}
}

func TestAgent_Deliver_OIDC_EmptyAudience_FailsDelivery(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()

	agent := kind.NewAgent(reporter, fakeFactory(provider), kind.WithGenerationStore(kind.NewMemoryGenerationStore()))

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
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name": "empty-aud"}`),
	}}

	err := agent.Deliver(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}), "d1:k1", manifests, auth, nil, 1)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	result := <-reporter.done
	if result.State != domain.DeliveryStateFailed {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateFailed)
	}
	if provider.hasCluster("fs--empty-aud") {
		t.Error("cluster should not have been created")
	}
}

func TestAgent_Deliver_RecreateValidatesConfigBeforeDelete(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()
	agent, _ := newTestAgent(reporter, provider)
	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name":"demo"}`),
	}}
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{})

	if err := agent.Deliver(context.Background(), target, "d1:t1", manifests, domain.DeliveryAuth{}, nil, 1); err != nil {
		t.Fatalf("gen1: %v", err)
	}
	if r := awaitDone(t, reporter.done); r.State != domain.DeliveryStateDelivered {
		t.Fatalf("gen1 State = %q: %s", r.State, r.Message)
	}

	badAuth := domain.DeliveryAuth{
		Caller: &domain.SubjectClaims{
			FederatedIdentity: domain.FederatedIdentity{
				Subject: "alice",
				Issuer:  "https://issuer.example.com",
			},
		},
		// Empty audience fails resolveConfig — must not delete first.
	}
	if err := agent.Deliver(context.Background(), target, "d1:t1", manifests, badAuth, nil, 2); err != nil {
		t.Fatalf("gen2: %v", err)
	}
	r := awaitDone(t, reporter.done)
	if r.State != domain.DeliveryStateFailed {
		t.Fatalf("gen2 State = %q, want Failed: %s", r.State, r.Message)
	}
	if provider.deleteCount() != 0 {
		t.Fatalf("Delete count = %d, want 0 (config must be validated before delete)", provider.deleteCount())
	}
	if !provider.hasCluster("fs--demo") {
		t.Fatal("existing cluster must remain when replacement config is invalid")
	}
}

func TestAgent_Deliver_MissingCMValidatesConfigBeforeDelete(t *testing.T) {
	provider := newFakeProvider()
	provider.clusters["fs--demo"] = nil
	reporter := newChannelReporter()
	agent, _ := newTestAgent(reporter, provider)

	badAuth := domain.DeliveryAuth{
		Caller: &domain.SubjectClaims{
			FederatedIdentity: domain.FederatedIdentity{
				Subject: "alice",
				Issuer:  "https://issuer.example.com",
			},
		},
	}
	_ = agent.Deliver(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}), "d1:t1",
		[]domain.Manifest{{ManifestType: kind.ClusterManifestType, Raw: json.RawMessage(`{"name":"demo"}`)}},
		badAuth, nil, 1)
	r := awaitDone(t, reporter.done)
	if r.State != domain.DeliveryStateFailed {
		t.Fatalf("State = %q, want Failed: %s", r.State, r.Message)
	}
	if provider.deleteCount() != 0 {
		t.Fatalf("Delete count = %d, want 0", provider.deleteCount())
	}
	if !provider.hasCluster("fs--demo") {
		t.Fatal("existing cluster must remain when replacement config is invalid")
	}
}
