package gcphcp_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/gcphcp"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

type recordingReporter struct {
	mu      sync.Mutex
	results map[domain.DeliveryID]domain.DeliveryResult
	done    chan domain.DeliveryResult
}

func newRecordingReporter() *recordingReporter {
	return &recordingReporter{
		results: make(map[domain.DeliveryID]domain.DeliveryResult),
		done:    make(chan domain.DeliveryResult, 10),
	}
}

func (r *recordingReporter) ReportEvent(_ context.Context, _ domain.DeliveryID, _ domain.DeliveryEvent) error {
	return nil
}

func (r *recordingReporter) ReportResult(_ context.Context, id domain.DeliveryID, result domain.DeliveryResult) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.results[id] = result
	r.done <- result
	return nil
}

func (r *recordingReporter) ListActiveDeliveries(_ context.Context, _ []domain.TargetID) ([]domain.ActiveDelivery, error) {
	return nil, nil
}

func TestAgent_Deliver_RejectsMissingName(t *testing.T) {
	reporter := newRecordingReporter()
	agent := gcphcp.NewAgent(gcphcp.AgentDeps{
		Gateway: gcphcp.GatewayConfig{
			URL:      "https://test-gateway",
			Audience: "test-audience",
		},
		Reporter: reporter,
	})

	manifest := domain.Manifest{
		ResourceType: gcphcp.ClusterResourceType,
		Raw:          json.RawMessage(`{}`),
	}

	err := agent.Deliver(
		context.Background(),
		domain.TargetInfo{},
		domain.DeliveryID("test-delivery"),
		[]domain.Manifest{manifest},
		domain.DeliveryAuth{Token: "test-token"},
		nil,
		0,
	)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	select {
	case result := <-reporter.done:
		if result.State != domain.DeliveryStateFailed {
			t.Errorf("expected state %s, got %s", domain.DeliveryStateFailed, result.State)
		}
		if result.Message == "" {
			t.Error("expected non-empty error message")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for delivery result")
	}
}

func TestAgent_Deliver_TrustBundleOnly(t *testing.T) {
	reporter := newRecordingReporter()
	agent := gcphcp.NewAgent(gcphcp.AgentDeps{
		Gateway: gcphcp.GatewayConfig{
			URL:      "https://test-gateway",
			Audience: "test-audience",
		},
		Reporter: reporter,
	})

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

	err = agent.Deliver(
		context.Background(),
		domain.TargetInfo{},
		domain.DeliveryID("test-delivery"),
		[]domain.Manifest{manifest},
		domain.DeliveryAuth{},
		nil,
		0,
	)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	select {
	case result := <-reporter.done:
		if result.State != domain.DeliveryStateDelivered {
			t.Errorf("expected state %s, got %s", domain.DeliveryStateDelivered, result.State)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for delivery result")
	}

	bundles := agent.TrustBundles()
	if len(bundles) != 1 {
		t.Fatalf("expected 1 trust bundle, got %d", len(bundles))
	}
	if bundles[0].IssuerURL != trustBundle.IssuerURL {
		t.Errorf("expected issuer URL %s, got %s", trustBundle.IssuerURL, bundles[0].IssuerURL)
	}
}

func TestAgent_Deliver_TrustBundleOnly_CompletesEvenIfRequestContextCanceled(t *testing.T) {
	reporter := newRecordingReporter()
	agent := gcphcp.NewAgent(gcphcp.AgentDeps{
		Gateway: gcphcp.GatewayConfig{
			URL:      "https://test-gateway",
			Audience: "test-audience",
		},
		Reporter: reporter,
	})

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

	err = agent.Deliver(
		ctx,
		domain.TargetInfo{},
		domain.DeliveryID("test-delivery"),
		[]domain.Manifest{{
			ResourceType: domain.TrustBundleResourceType,
			Raw:          json.RawMessage(trustBundleJSON),
		}},
		domain.DeliveryAuth{},
		nil,
		0,
	)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	select {
	case result := <-reporter.done:
		if result.State != domain.DeliveryStateDelivered {
			t.Fatalf("expected state %s, got %s", domain.DeliveryStateDelivered, result.State)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for completion signal")
	}
}

func TestAgent_Deliver_TrustBundleOnly_ReplacesExistingIssuerEntry(t *testing.T) {
	reporter := newRecordingReporter()
	agent := gcphcp.NewAgent(gcphcp.AgentDeps{
		Gateway: gcphcp.GatewayConfig{
			URL:      "https://test-gateway",
			Audience: "test-audience",
		},
		Reporter: reporter,
	})

	deliverTrustBundle(t, agent, reporter, domain.TrustBundleEntry{
		IssuerURL:          "https://issuer.example.com",
		JWKSURI:            "https://issuer.example.com/jwks-1",
		EnrollmentAudience: "audience-1",
	})
	deliverTrustBundle(t, agent, reporter, domain.TrustBundleEntry{
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
	reporter := newRecordingReporter()
	agent := gcphcp.NewAgent(gcphcp.AgentDeps{
		Gateway: gcphcp.GatewayConfig{
			URL:      "https://test-gateway",
			Audience: "test-audience",
		},
		Reporter: reporter,
	})

	deliverTrustBundle(t, agent, reporter, domain.TrustBundleEntry{
		IssuerURL:          "https://issuer-b.example.com",
		JWKSURI:            "https://issuer-b.example.com/jwks",
		EnrollmentAudience: "audience-b",
	})
	deliverTrustBundle(t, agent, reporter, domain.TrustBundleEntry{
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
	reporter := newRecordingReporter()
	agent := gcphcp.NewAgent(gcphcp.AgentDeps{
		Gateway: gcphcp.GatewayConfig{
			URL:      "https://test-gateway",
			Audience: "test-audience",
		},
		Reporter: reporter,
	})

	entry := domain.TrustBundleEntry{
		IssuerURL:          "https://issuer.example.com",
		JWKSURI:            "https://issuer.example.com/jwks",
		EnrollmentAudience: "audience-1",
	}
	deliverTrustBundle(t, agent, reporter, entry)

	err := agent.Remove(
		context.Background(),
		domain.TargetInfo{},
		domain.DeliveryID("test-delivery"),
		[]domain.Manifest{trustBundleManifest(t, entry)},
		domain.DeliveryAuth{},
		nil,
		0,
	)
	if err != nil {
		t.Fatalf("Remove() error = %v", err)
	}

	if bundles := agent.TrustBundles(); len(bundles) != 0 {
		t.Fatalf("expected trust bundle removal, got %#v", bundles)
	}
}

func deliverTrustBundle(t *testing.T, agent *gcphcp.Agent, reporter *recordingReporter, entry domain.TrustBundleEntry) {
	t.Helper()

	err := agent.Deliver(
		context.Background(),
		domain.TargetInfo{},
		domain.DeliveryID("trust-delivery"),
		[]domain.Manifest{trustBundleManifest(t, entry)},
		domain.DeliveryAuth{},
		nil,
		0,
	)
	if err != nil {
		t.Fatalf("Deliver() error = %v", err)
	}

	select {
	case result := <-reporter.done:
		if result.State != domain.DeliveryStateDelivered {
			t.Fatalf("async state = %q, want %q", result.State, domain.DeliveryStateDelivered)
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
