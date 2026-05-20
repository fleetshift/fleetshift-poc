package gcphcp_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/gcphcp"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestAgent_Deliver_RejectsMissingName(t *testing.T) {
	// Create agent with dummy gateway config
	agent := gcphcp.NewAgent(gcphcp.AgentDeps{
		Gateway: gcphcp.GatewayConfig{
			URL:      "https://test-gateway",
			Audience: "test-audience",
		},
	})

	// Create signaler
	signaler := &domain.DeliverySignaler{
		Signal: func(_ context.Context, _ domain.FulfillmentID, event domain.FulfillmentEvent) error {
			return nil
		},
	}

	// Send manifest with empty JSON {}
	manifest := domain.Manifest{
		ResourceType: gcphcp.ClusterResourceType,
		Raw:          json.RawMessage(`{}`),
	}

	result, err := agent.Deliver(
		context.Background(),
		domain.TargetInfo{},
		domain.DeliveryID("test-delivery"),
		[]domain.Manifest{manifest},
		domain.DeliveryAuth{Token: "test-token"},
		nil,
		signaler,
	)

	// Expect DeliveryStateFailed result (not error)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if result.State != domain.DeliveryStateFailed {
		t.Errorf("expected state %s, got %s", domain.DeliveryStateFailed, result.State)
	}
	if result.Message == "" {
		t.Error("expected non-empty error message")
	}
}

func TestAgent_Deliver_TrustBundleOnly(t *testing.T) {
	// Create agent
	agent := gcphcp.NewAgent(gcphcp.AgentDeps{
		Gateway: gcphcp.GatewayConfig{
			URL:      "https://test-gateway",
			Audience: "test-audience",
		},
	})

	// Create signaler with channel to capture Done signal
	done := make(chan domain.DeliveryResult, 1)
	signaler := &domain.DeliverySignaler{
		Signal: func(_ context.Context, _ domain.FulfillmentID, event domain.FulfillmentEvent) error {
			if event.DeliveryCompleted != nil {
				done <- event.DeliveryCompleted.Result
			}
			return nil
		},
	}

	// Create trust bundle manifest
	trustBundle := domain.TrustBundleEntry{
		IssuerURL:          "https://test-issuer",
		JWKSURI:            "https://test-jwks",
		EnrollmentAudience: "test-audience",
	}
	trustBundleJSON, err := json.Marshal(trustBundle)
	if err != nil {
		t.Fatalf("failed to marshal trust bundle: %v", err)
	}

	manifest := domain.Manifest{
		ResourceType: domain.TrustBundleResourceType,
		Raw:          json.RawMessage(trustBundleJSON),
	}

	// Send only a trust-bundle manifest
	result, err := agent.Deliver(
		context.Background(),
		domain.TargetInfo{},
		domain.DeliveryID("test-delivery"),
		[]domain.Manifest{manifest},
		domain.DeliveryAuth{},
		nil,
		signaler,
	)

	// Expect Accepted return
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if result.State != domain.DeliveryStateAccepted {
		t.Errorf("expected state %s, got %s", domain.DeliveryStateAccepted, result.State)
	}

	// Wait for Done(Delivered) via signaler
	select {
	case finalResult := <-done:
		if finalResult.State != domain.DeliveryStateDelivered {
			t.Errorf("expected final state %s, got %s", domain.DeliveryStateDelivered, finalResult.State)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Done signal")
	}

	// Verify trust bundle stored in agent
	bundles := agent.TrustBundles()
	if len(bundles) != 1 {
		t.Fatalf("expected 1 trust bundle, got %d", len(bundles))
	}
	if bundles[0].IssuerURL != trustBundle.IssuerURL {
		t.Errorf("expected issuer URL %s, got %s", trustBundle.IssuerURL, bundles[0].IssuerURL)
	}
}

func TestAgent_Deliver_TrustBundleOnly_CompletesEvenIfRequestContextCanceled(t *testing.T) {
	agent := gcphcp.NewAgent(gcphcp.AgentDeps{
		Gateway: gcphcp.GatewayConfig{
			URL:      "https://test-gateway",
			Audience: "test-audience",
		},
	})

	done := make(chan error, 1)
	signaler := &domain.DeliverySignaler{
		Signal: func(ctx context.Context, _ domain.FulfillmentID, event domain.FulfillmentEvent) error {
			if event.DeliveryCompleted != nil {
				done <- ctx.Err()
			}
			return nil
		},
	}

	trustBundle := domain.TrustBundleEntry{
		IssuerURL:          "https://test-issuer",
		JWKSURI:            "https://test-jwks",
		EnrollmentAudience: "test-audience",
	}
	trustBundleJSON, err := json.Marshal(trustBundle)
	if err != nil {
		t.Fatalf("failed to marshal trust bundle: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := agent.Deliver(
		ctx,
		domain.TargetInfo{},
		domain.DeliveryID("test-delivery"),
		[]domain.Manifest{{
			ResourceType: domain.TrustBundleResourceType,
			Raw:          json.RawMessage(trustBundleJSON),
		}},
		domain.DeliveryAuth{},
		nil,
		signaler,
	)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if result.State != domain.DeliveryStateAccepted {
		t.Fatalf("expected state %s, got %s", domain.DeliveryStateAccepted, result.State)
	}

	select {
	case signalCtxErr := <-done:
		if signalCtxErr != nil {
			t.Fatalf("expected completion signal to use uncanceled context, got %v", signalCtxErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for completion signal")
	}
}

func TestAgent_Deliver_TrustBundleOnly_ReplacesExistingIssuerEntry(t *testing.T) {
	agent := gcphcp.NewAgent(gcphcp.AgentDeps{
		Gateway: gcphcp.GatewayConfig{
			URL:      "https://test-gateway",
			Audience: "test-audience",
		},
	})

	deliverTrustBundle(t, agent, domain.TrustBundleEntry{
		IssuerURL:          "https://issuer.example.com",
		JWKSURI:            "https://issuer.example.com/jwks-1",
		EnrollmentAudience: "audience-1",
	})
	deliverTrustBundle(t, agent, domain.TrustBundleEntry{
		IssuerURL:          "https://issuer.example.com",
		JWKSURI:            "https://issuer.example.com/jwks-2",
		EnrollmentAudience: "audience-2",
	})

	bundles := agent.TrustBundles()
	if len(bundles) != 1 {
		t.Fatalf("expected 1 trust bundle after replacement, got %d", len(bundles))
	}
	if bundles[0].JWKSURI != "https://issuer.example.com/jwks-2" {
		t.Fatalf("JWKSURI = %q, want replacement value", bundles[0].JWKSURI)
	}
	if bundles[0].EnrollmentAudience != "audience-2" {
		t.Fatalf("EnrollmentAudience = %q, want replacement value", bundles[0].EnrollmentAudience)
	}
}

func TestAgent_TrustBundles_ReturnsEntriesSortedByIssuer(t *testing.T) {
	agent := gcphcp.NewAgent(gcphcp.AgentDeps{
		Gateway: gcphcp.GatewayConfig{
			URL:      "https://test-gateway",
			Audience: "test-audience",
		},
	})

	deliverTrustBundle(t, agent, domain.TrustBundleEntry{
		IssuerURL:          "https://issuer-b.example.com",
		JWKSURI:            "https://issuer-b.example.com/jwks",
		EnrollmentAudience: "audience-b",
	})
	deliverTrustBundle(t, agent, domain.TrustBundleEntry{
		IssuerURL:          "https://issuer-a.example.com",
		JWKSURI:            "https://issuer-a.example.com/jwks",
		EnrollmentAudience: "audience-a",
	})

	bundles := agent.TrustBundles()
	if len(bundles) != 2 {
		t.Fatalf("expected 2 trust bundles, got %d", len(bundles))
	}
	if bundles[0].IssuerURL != "https://issuer-a.example.com" {
		t.Fatalf("bundles[0].IssuerURL = %q, want issuer-a first", bundles[0].IssuerURL)
	}
	if bundles[1].IssuerURL != "https://issuer-b.example.com" {
		t.Fatalf("bundles[1].IssuerURL = %q, want issuer-b second", bundles[1].IssuerURL)
	}
}

func TestAgent_Remove_TrustBundle_RemovesStoredIssuerEntry(t *testing.T) {
	agent := gcphcp.NewAgent(gcphcp.AgentDeps{
		Gateway: gcphcp.GatewayConfig{
			URL:      "https://test-gateway",
			Audience: "test-audience",
		},
	})

	entry := domain.TrustBundleEntry{
		IssuerURL:          "https://issuer.example.com",
		JWKSURI:            "https://issuer.example.com/jwks",
		EnrollmentAudience: "audience-1",
	}
	deliverTrustBundle(t, agent, entry)

	err := agent.Remove(
		context.Background(),
		domain.TargetInfo{},
		domain.DeliveryID("test-delivery"),
		[]domain.Manifest{trustBundleManifest(t, entry)},
		domain.DeliveryAuth{},
		nil,
		&domain.DeliverySignaler{},
	)
	if err != nil {
		t.Fatalf("Remove() error = %v", err)
	}

	if bundles := agent.TrustBundles(); len(bundles) != 0 {
		t.Fatalf("expected trust bundle removal, got %#v", bundles)
	}
}

func deliverTrustBundle(t *testing.T, agent *gcphcp.Agent, entry domain.TrustBundleEntry) {
	t.Helper()

	done := make(chan domain.DeliveryResult, 1)
	signaler := &domain.DeliverySignaler{
		Signal: func(_ context.Context, _ domain.FulfillmentID, event domain.FulfillmentEvent) error {
			if event.DeliveryCompleted != nil {
				done <- event.DeliveryCompleted.Result
			}
			return nil
		},
	}

	result, err := agent.Deliver(
		context.Background(),
		domain.TargetInfo{},
		domain.DeliveryID("trust-delivery"),
		[]domain.Manifest{trustBundleManifest(t, entry)},
		domain.DeliveryAuth{},
		nil,
		signaler,
	)
	if err != nil {
		t.Fatalf("Deliver() error = %v", err)
	}
	if result.State != domain.DeliveryStateAccepted {
		t.Fatalf("Deliver() state = %q, want %q", result.State, domain.DeliveryStateAccepted)
	}

	select {
	case finalResult := <-done:
		if finalResult.State != domain.DeliveryStateDelivered {
			t.Fatalf("async state = %q, want %q", finalResult.State, domain.DeliveryStateDelivered)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for trust bundle delivery completion")
	}
}

func trustBundleManifest(t *testing.T, entry domain.TrustBundleEntry) domain.Manifest {
	t.Helper()

	raw, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return domain.Manifest{
		ResourceType: domain.TrustBundleResourceType,
		Raw:          json.RawMessage(raw),
	}
}
