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

func TestAgent_Deliver_ConfigWithCallerRejected(t *testing.T) {
	provider := newFakeProvider()
	agent := kind.NewAgent(fakeFactory(provider))

	spec := kind.ClusterSpec{
		Name:   "bad-cluster",
		Config: json.RawMessage(`{"kind":"Cluster"}`),
	}
	specBytes, _ := json.Marshal(spec)

	target := domain.TargetInfo{ID: "k1", Type: kind.TargetType, Name: "local-kind"}
	manifests := []domain.Manifest{{
		ResourceType: kind.ClusterResourceType,
		Raw:          json.RawMessage(specBytes),
	}}

	result, err := agent.Deliver(context.Background(), target, "d1:k1", manifests, callerAuth(), nil, nop)
	if err == nil {
		t.Fatal("expected error for config + authenticated caller")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument, got: %v", err)
	}
	if result.State != domain.DeliveryStateFailed {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateFailed)
	}
}

func assertContains(t *testing.T, haystack, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Errorf("expected to contain %q, got:\n%s", needle, haystack)
	}
}
