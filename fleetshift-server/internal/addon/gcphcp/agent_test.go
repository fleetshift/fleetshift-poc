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
