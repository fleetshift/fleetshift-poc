package kind_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestBuildKindOIDCConfig_BasicFlags(t *testing.T) {
	spec := &kind.OIDCSpec{}

	cfg, err := kind.BuildKindOIDCConfig("https://idp.example.com", "fleetshift", spec, "")
	if err != nil {
		t.Fatalf("BuildKindOIDCConfig: %v", err)
	}

	s := string(cfg)
	assertContains(t, s, "kind: Cluster")
	assertContains(t, s, "apiVersion: kind.x-k8s.io/v1alpha4")
	assertContains(t, s, `oidc-issuer-url: "https://idp.example.com"`)
	assertContains(t, s, `oidc-client-id: "fleetshift"`)
	assertContains(t, s, `oidc-username-claim: "sub"`)
	assertContains(t, s, `oidc-groups-claim: "groups"`)

	if strings.Contains(s, "oidc-ca-file") {
		t.Error("oidc-ca-file should not be present without CA bundle")
	}
	if strings.Contains(s, "extraMounts") {
		t.Error("extraMounts should not be present without CA bundle")
	}
}

func TestBuildKindOIDCConfig_WithCACertPath(t *testing.T) {
	spec := &kind.OIDCSpec{}

	cfg, err := kind.BuildKindOIDCConfig("https://host.docker.internal:9443", "fleetshift", spec, "/tmp/ca.pem")
	if err != nil {
		t.Fatalf("BuildKindOIDCConfig: %v", err)
	}

	s := string(cfg)
	assertContains(t, s, `oidc-ca-file: "/etc/kubernetes/pki/oidc-ca.pem"`)
	assertContains(t, s, "extraMounts:")
	assertContains(t, s, "hostPath: /tmp/ca.pem")
	assertContains(t, s, "containerPath: /etc/kubernetes/pki/oidc-ca.pem")
}

func TestBuildKindOIDCConfig_CustomClaims(t *testing.T) {
	spec := &kind.OIDCSpec{
		UsernameClaim: "email",
		GroupsClaim:   "roles",
	}

	cfg, err := kind.BuildKindOIDCConfig("https://idp.example.com", "my-app", spec, "")
	if err != nil {
		t.Fatalf("BuildKindOIDCConfig: %v", err)
	}

	s := string(cfg)
	assertContains(t, s, `oidc-username-claim: "email"`)
	assertContains(t, s, `oidc-groups-claim: "roles"`)
}

func TestBuildKindOIDCConfig_ValidationErrors(t *testing.T) {
	tests := []struct {
		name      string
		issuerURL domain.IssuerURL
		audience  domain.Audience
	}{
		{"missing issuer", "", "aud"},
		{"missing audience", "https://x", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := kind.BuildKindOIDCConfig(tt.issuerURL, tt.audience, &kind.OIDCSpec{}, "")
			if err == nil {
				t.Error("expected validation error")
			}
		})
	}
}

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
	obs := newChannelDeliveryObserver()
	signaler := newChannelSignaler(obs)

	agentObs := &recordingAgentObserver{}
	agent := kind.NewAgent(fakeFactory(provider), kind.WithObserver(agentObs))

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

	_, err := agent.Deliver(context.Background(), target, "d1:k1", manifests, callerAuth(), nil, signaler)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	<-obs.done

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
	obs := newChannelDeliveryObserver()
	signaler := newChannelSignaler(obs)

	agent := kind.NewAgent(fakeFactory(provider))

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

	_, err := agent.Deliver(context.Background(), domain.TargetInfo{}, "d1:k1", manifests, auth, nil, signaler)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	result := <-obs.done
	if result.State != domain.DeliveryStateFailed {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateFailed)
	}
	if provider.hasCluster("empty-aud") {
		t.Error("cluster should not have been created")
	}
}

func assertContains(t *testing.T, haystack, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Errorf("expected to contain %q, got:\n%s", needle, haystack)
	}
}
