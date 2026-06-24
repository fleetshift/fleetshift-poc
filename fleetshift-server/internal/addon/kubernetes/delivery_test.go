package kubernetes_test

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	kubernetes "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/testutil"
)

// awaitDone drains one result from ch with a safety-net timeout so
// that a regression in the fake delivery pipeline hangs for at most
// [testutil.UnitTimeout] rather than the global go-test deadline.
func awaitDone(t *testing.T, ch <-chan domain.DeliveryResult) domain.DeliveryResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(testutil.UnitTimeout):
		t.Fatal("timed out waiting for delivery result")
		return domain.DeliveryResult{}
	}
}

func tlsServerCAPEM(ts *httptest.Server) string {
	return string(pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: ts.Certificate().Raw,
	}))
}

// channelReporter implements [domain.DeliveryReporter] for tests,
// routing events and results to channels for deterministic waits.
type channelReporter struct {
	mu     sync.Mutex
	events []domain.DeliveryEvent
	ch     chan domain.DeliveryEvent
	done   chan domain.DeliveryResult
}

func newChannelReporter() *channelReporter {
	return &channelReporter{
		ch:   make(chan domain.DeliveryEvent, 100),
		done: make(chan domain.DeliveryResult, 1),
	}
}

func (r *channelReporter) ReportEvent(_ context.Context, _ domain.DeliveryID, _ domain.Generation, event domain.DeliveryEvent) error {
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()
	r.ch <- event
	return nil
}

func (r *channelReporter) ReportResult(_ context.Context, _ domain.DeliveryID, _ domain.Generation, result domain.DeliveryResult) error {
	r.done <- result
	return nil
}

func (r *channelReporter) ListActiveDeliveries(_ context.Context, _ []domain.TargetID) ([]domain.ActiveDelivery, error) {
	return nil, nil
}

// nopReporter discards all reports; used when tests don't need to
// observe async results.
type nopReporter struct{}

func (nopReporter) ReportEvent(context.Context, domain.DeliveryID, domain.Generation, domain.DeliveryEvent) error {
	return nil
}
func (nopReporter) ReportResult(context.Context, domain.DeliveryID, domain.Generation, domain.DeliveryResult) error {
	return nil
}
func (nopReporter) ListActiveDeliveries(context.Context, []domain.TargetID) ([]domain.ActiveDelivery, error) {
	return nil, nil
}

// newTestManagerWithReporter creates a Manager with the given reporter
// for delivery tests that need to observe async results.
func newTestManagerWithReporter(t *testing.T, reporter domain.DeliveryReporter) *kubernetes.AgentPool {
	t.Helper()
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}
	vault := &fakeVault{secrets: make(map[domain.SecretRef][]byte)}
	logger := slog.Default()
	return kubernetes.NewAgentPool(context.Background(), store, vault, nil, reporter, nil, nil, logger)
}

// deliveryTarget builds a TargetInfo with the given properties for
// delivery tests.
func deliveryTarget(id string, props map[string]string) domain.TargetInfo {
	return domain.NewTargetInfo(
		domain.TargetID(id),
		kubernetes.TargetType,
		"test-cluster",
		domain.TargetStateReady,
		nil,
		props,
		nil,
	)
}

func TestDeliver_MissingAPIServer(t *testing.T) {
	mgr := newTestManagerWithReporter(t, nopReporter{})
	t.Cleanup(mgr.StopAll)

	ctx := context.Background()
	target := deliveryTarget("k8s-test", map[string]string{})

	// StartIndexing should fail because api_server is missing.
	err := mgr.StartIndexing(ctx, target)
	if err == nil {
		t.Fatal("expected error from StartIndexing for missing api_server")
	}
	if !strings.Contains(err.Error(), "api_server") {
		t.Errorf("expected error to mention api_server, got: %v", err)
	}

	// Deliver should also fail: no agent exists for this target.
	err = mgr.Deliver(ctx, target, "d1", nil, domain.DeliveryAuth{Token: "some-token"}, nil, 1)
	if err == nil {
		t.Fatal("expected error from Deliver for missing target agent")
	}
}

func TestDeliver_EmptyAPIServer(t *testing.T) {
	mgr := newTestManagerWithReporter(t, nopReporter{})
	t.Cleanup(mgr.StopAll)

	ctx := context.Background()
	target := deliveryTarget("k8s-test", map[string]string{"api_server": ""})

	// StartIndexing should fail because api_server is empty.
	err := mgr.StartIndexing(ctx, target)
	if err == nil {
		t.Fatal("expected error from StartIndexing for empty api_server")
	}
}

func TestRemove_EmptyAPIServer(t *testing.T) {
	mgr := newTestManagerWithReporter(t, nopReporter{})
	t.Cleanup(mgr.StopAll)

	ctx := context.Background()
	target := deliveryTarget("k8s-test", map[string]string{"api_server": ""})

	err := mgr.StartIndexing(ctx, target)
	if err == nil {
		t.Fatal("expected error from StartIndexing for empty api_server")
	}

	// Remove should fail: no agent.
	err = mgr.Remove(ctx, target, "d1", nil, domain.DeliveryAuth{Token: "some-token"}, nil, 1)
	if err == nil {
		t.Fatal("expected error from Remove for missing target agent")
	}
}

func TestDeliver_MissingToken(t *testing.T) {
	mgr := newTestManagerWithReporter(t, nopReporter{})
	t.Cleanup(mgr.StopAll)

	ctx := context.Background()
	target := deliveryTarget("k8s-test", map[string]string{
		"api_server": "https://127.0.0.1:6443",
	})

	if err := mgr.StartIndexing(ctx, target); err != nil {
		t.Fatalf("StartIndexing: %v", err)
	}

	// Deliver without a token should fail with ErrInvalidArgument.
	err := mgr.Deliver(ctx, target, "d1", nil, domain.DeliveryAuth{}, nil, 1)
	if err == nil {
		t.Fatal("expected error for missing token")
	}
	if !strings.Contains(err.Error(), "token") {
		t.Errorf("expected token-related error, got: %v", err)
	}
}

func TestDeliver_BadAPIServer(t *testing.T) {
	reporter := newChannelReporter()
	mgr := newTestManagerWithReporter(t, reporter)
	t.Cleanup(mgr.StopAll)

	ctx := context.Background()
	target := deliveryTarget("k8s-test", map[string]string{
		"api_server": "https://127.0.0.1:1",
	})

	if err := mgr.StartIndexing(ctx, target); err != nil {
		t.Fatalf("StartIndexing: %v", err)
	}

	manifests := []domain.Manifest{{
		ResourceType: "raw",
		Raw:          json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"test","namespace":"default"},"data":{"key":"value"}}`),
	}}

	auth := domain.DeliveryAuth{Token: "not-a-real-token"}
	err := mgr.Deliver(ctx, target, "d1", manifests, auth, nil, 1)
	if err != nil {
		t.Fatalf("Deliver should not return error: %v", err)
	}

	asyncResult := awaitDone(t, reporter.done)
	if asyncResult.State != domain.DeliveryStateFailed {
		t.Errorf("async State = %q, want %q", asyncResult.State, domain.DeliveryStateFailed)
	}
}

func TestRemove_MissingAPIServer(t *testing.T) {
	mgr := newTestManagerWithReporter(t, nopReporter{})
	t.Cleanup(mgr.StopAll)

	ctx := context.Background()
	target := deliveryTarget("k8s-test", map[string]string{})

	// StartIndexing should fail.
	if err := mgr.StartIndexing(ctx, target); err == nil {
		t.Fatal("expected error from StartIndexing for missing api_server")
	}

	// Remove should fail: no agent.
	err := mgr.Remove(ctx, target, "d1", nil, domain.DeliveryAuth{Token: "some-token"}, nil, 1)
	if err == nil {
		t.Fatal("expected error from Remove for missing target agent")
	}
}

func TestRemove_EmptyManifests(t *testing.T) {
	mgr := newTestManagerWithReporter(t, nopReporter{})
	t.Cleanup(mgr.StopAll)

	ctx := context.Background()
	target := deliveryTarget("k8s-test", map[string]string{
		"api_server": "https://127.0.0.1:6443",
	})

	if err := mgr.StartIndexing(ctx, target); err != nil {
		t.Fatalf("StartIndexing: %v", err)
	}

	if err := mgr.Remove(ctx, target, "d1", nil, domain.DeliveryAuth{Token: "some-token"}, nil, 1); err != nil {
		t.Fatalf("Remove with empty manifests: %v", err)
	}
}

func TestDeliver_Unauthorized_ReportsAuthFailed(t *testing.T) {
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"Unauthorized","reason":"Unauthorized","code":401}`)
	}))
	defer ts.Close()

	reporter := newChannelReporter()
	mgr := newTestManagerWithReporter(t, reporter)
	t.Cleanup(mgr.StopAll)

	ctx := context.Background()
	target := deliveryTarget("k8s-test", map[string]string{
		"api_server": ts.URL,
		"ca_cert":    tlsServerCAPEM(ts),
	})

	if err := mgr.StartIndexing(ctx, target); err != nil {
		t.Fatalf("StartIndexing: %v", err)
	}

	manifests := []domain.Manifest{{
		ResourceType: "raw",
		Raw:          json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"test","namespace":"default"},"data":{"key":"value"}}`),
	}}

	auth := domain.DeliveryAuth{Token: "expired-token"}
	err := mgr.Deliver(ctx, target, "d1", manifests, auth, nil, 1)
	if err != nil {
		t.Fatalf("Deliver should not return error: %v", err)
	}

	asyncResult := awaitDone(t, reporter.done)
	if asyncResult.State != domain.DeliveryStateAuthFailed {
		t.Errorf("async State = %q, want %q; message: %s", asyncResult.State, domain.DeliveryStateAuthFailed, asyncResult.Message)
	}
}

func TestDeliver_Forbidden_ReportsAuthFailed(t *testing.T) {
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprintf(w, `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"Forbidden","reason":"Forbidden","code":403}`)
	}))
	defer ts.Close()

	reporter := newChannelReporter()
	mgr := newTestManagerWithReporter(t, reporter)
	t.Cleanup(mgr.StopAll)

	ctx := context.Background()
	target := deliveryTarget("k8s-test", map[string]string{
		"api_server": ts.URL,
		"ca_cert":    tlsServerCAPEM(ts),
	})

	if err := mgr.StartIndexing(ctx, target); err != nil {
		t.Fatalf("StartIndexing: %v", err)
	}

	manifests := []domain.Manifest{{
		ResourceType: "raw",
		Raw:          json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"test","namespace":"default"},"data":{"key":"value"}}`),
	}}

	auth := domain.DeliveryAuth{Token: "some-token"}
	err := mgr.Deliver(ctx, target, "d1", manifests, auth, nil, 1)
	if err != nil {
		t.Fatalf("Deliver should not return error: %v", err)
	}

	asyncResult := awaitDone(t, reporter.done)
	if asyncResult.State != domain.DeliveryStateAuthFailed {
		t.Errorf("async State = %q, want %q; message: %s", asyncResult.State, domain.DeliveryStateAuthFailed, asyncResult.Message)
	}
}

func TestDeliver_AttestationFailure_ReturnsAuthFailed(t *testing.T) {
	reporter := newChannelReporter()
	mgr := newTestManagerWithReporter(t, reporter)
	t.Cleanup(mgr.StopAll)

	ctx := context.Background()
	trustBundle := `[{"issuer_url":"https://trusted.example.com","jwks_uri":"https://trusted.example.com/jwks","enrollment_audience":"enroll"}]`
	target := deliveryTarget("k8s-test", map[string]string{
		"api_server":   "https://127.0.0.1:6443",
		"trust_bundle": trustBundle,
	})

	if err := mgr.StartIndexing(ctx, target); err != nil {
		t.Fatalf("StartIndexing: %v", err)
	}

	att := &domain.Attestation{
		Input: domain.SignedInput{
			Provenance: domain.Provenance{
				Sig: domain.Signature{
					Signer: domain.FederatedIdentity{
						Issuer: "https://untrusted.example.com",
					},
				},
			},
		},
	}

	err := mgr.Deliver(ctx, target, "d1", nil, domain.DeliveryAuth{}, att, 1)
	if err != nil {
		t.Fatalf("Deliver should not return error: %v", err)
	}
	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateAuthFailed {
		t.Errorf("State = %q, want %q; message: %s", result.State, domain.DeliveryStateAuthFailed, result.Message)
	}
}

func TestDeliver_WithAttestation_NoTrustBundle_ReturnsAuthFailed(t *testing.T) {
	reporter := newChannelReporter()
	mgr := newTestManagerWithReporter(t, reporter)
	t.Cleanup(mgr.StopAll)

	ctx := context.Background()
	target := deliveryTarget("k8s-test", map[string]string{
		"api_server": "https://127.0.0.1:6443",
	})

	if err := mgr.StartIndexing(ctx, target); err != nil {
		t.Fatalf("StartIndexing: %v", err)
	}

	att := &domain.Attestation{
		Input: domain.SignedInput{
			Provenance: domain.Provenance{
				Sig: domain.Signature{
					Signer: domain.FederatedIdentity{
						Issuer: "https://untrusted.example.com",
					},
				},
			},
		},
	}

	err := mgr.Deliver(ctx, target, "d1", nil, domain.DeliveryAuth{}, att, 1)
	if err != nil {
		t.Fatalf("Deliver should not return error: %v", err)
	}
	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateAuthFailed {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateAuthFailed)
	}
}

func TestDeliver_VerifierCacheReuse(t *testing.T) {
	reporter := newChannelReporter()
	mgr := newTestManagerWithReporter(t, reporter)
	t.Cleanup(mgr.StopAll)

	ctx := context.Background()
	trustBundle := `[{"issuer_url":"https://trusted.example.com","jwks_uri":"https://trusted.example.com/jwks","enrollment_audience":"enroll"}]`
	target := deliveryTarget("k8s-test", map[string]string{
		"api_server":   "https://127.0.0.1:6443",
		"trust_bundle": trustBundle,
	})

	if err := mgr.StartIndexing(ctx, target); err != nil {
		t.Fatalf("StartIndexing: %v", err)
	}

	att := &domain.Attestation{
		Input: domain.SignedInput{
			Provenance: domain.Provenance{
				Sig: domain.Signature{
					Signer: domain.FederatedIdentity{
						Issuer: "https://untrusted.example.com",
					},
				},
			},
		},
	}

	_ = mgr.Deliver(ctx, target, "d1", nil, domain.DeliveryAuth{}, att, 1)
	result1 := awaitDone(t, reporter.done)
	if result1.State != domain.DeliveryStateAuthFailed {
		t.Errorf("first: State = %q, want AuthFailed", result1.State)
	}

	_ = mgr.Deliver(ctx, target, "d2", nil, domain.DeliveryAuth{}, att, 1)
	result2 := awaitDone(t, reporter.done)
	if result2.State != domain.DeliveryStateAuthFailed {
		t.Errorf("second: State = %q, want AuthFailed", result2.State)
	}
}

func TestDeliver_WithAttestation_NoTokenRequired(t *testing.T) {
	reporter := newChannelReporter()
	mgr := newTestManagerWithReporter(t, reporter)
	t.Cleanup(mgr.StopAll)

	ctx := context.Background()
	trustBundle := `[{"issuer_url":"https://trusted.example.com","jwks_uri":"https://trusted.example.com/jwks","enrollment_audience":"enroll"}]`
	target := deliveryTarget("k8s-test", map[string]string{
		"api_server":   "https://127.0.0.1:6443",
		"trust_bundle": trustBundle,
	})

	if err := mgr.StartIndexing(ctx, target); err != nil {
		t.Fatalf("StartIndexing: %v", err)
	}

	att := &domain.Attestation{
		Input: domain.SignedInput{
			Provenance: domain.Provenance{
				Sig: domain.Signature{
					Signer: domain.FederatedIdentity{
						Issuer: "https://untrusted.example.com",
					},
				},
			},
		},
	}

	err := mgr.Deliver(ctx, target, "d1", nil, domain.DeliveryAuth{}, att, 1)
	if err != nil {
		t.Fatalf("Deliver should not return error: %v", err)
	}
	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateAuthFailed {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateAuthFailed)
	}
}

func TestRemove_AttestationFailure_ReportsAuthFailed(t *testing.T) {
	reporter := newChannelReporter()
	mgr := newTestManagerWithReporter(t, reporter)
	t.Cleanup(mgr.StopAll)

	ctx := context.Background()
	trustBundle := `[{"issuer_url":"https://trusted.example.com","jwks_uri":"https://trusted.example.com/jwks","enrollment_audience":"enroll"}]`
	target := deliveryTarget("k8s-test", map[string]string{
		"api_server":   "https://127.0.0.1:6443",
		"trust_bundle": trustBundle,
	})

	if err := mgr.StartIndexing(ctx, target); err != nil {
		t.Fatalf("StartIndexing: %v", err)
	}

	att := &domain.Attestation{
		Input: domain.SignedInput{
			Provenance: domain.Provenance{
				Sig: domain.Signature{
					Signer: domain.FederatedIdentity{
						Issuer: "https://untrusted.example.com",
					},
				},
			},
		},
	}

	err := mgr.Remove(ctx, target, "d1", nil, domain.DeliveryAuth{}, att, 1)
	if err != nil {
		t.Fatalf("Remove should not return error: %v", err)
	}
	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateAuthFailed {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateAuthFailed)
	}
	if !strings.Contains(result.Message, "attestation verification failed") {
		t.Errorf("expected attestation verification error in message, got: %q", result.Message)
	}
}

func TestRemove_WithAttestation_NoTrustBundle_ReportsAuthFailed(t *testing.T) {
	reporter := newChannelReporter()
	mgr := newTestManagerWithReporter(t, reporter)
	t.Cleanup(mgr.StopAll)

	ctx := context.Background()
	target := deliveryTarget("k8s-test", map[string]string{
		"api_server": "https://127.0.0.1:6443",
	})

	if err := mgr.StartIndexing(ctx, target); err != nil {
		t.Fatalf("StartIndexing: %v", err)
	}

	att := &domain.Attestation{
		Input: domain.SignedInput{
			Provenance: domain.Provenance{
				Sig: domain.Signature{
					Signer: domain.FederatedIdentity{
						Issuer: "https://untrusted.example.com",
					},
				},
			},
		},
	}

	err := mgr.Remove(ctx, target, "d1", nil, domain.DeliveryAuth{}, att, 1)
	if err != nil {
		t.Fatalf("Remove should not return error: %v", err)
	}
	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateAuthFailed {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateAuthFailed)
	}
	if !strings.Contains(result.Message, "trust_bundle") {
		t.Errorf("expected trust_bundle error in message, got: %q", result.Message)
	}
}
