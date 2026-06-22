package kubernetes_test

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
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

func TestAgent_Deliver_MissingAPIServer(t *testing.T) {
	agent := kubernetes.NewAgent(nopReporter{})

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:         "k8s-test",
		Type:       kubernetes.TargetType,
		Name:       "test-cluster",
		Properties: map[string]string{},
	})

	auth := domain.DeliveryAuth{Token: "some-token"}
	err := agent.Deliver(context.Background(), target, "d1", nil, auth, nil, 1)
	if err == nil {
		t.Fatal("expected error for missing api_server")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument, got: %v", err)
	}
}

func TestAgent_Deliver_EmptyAPIServer(t *testing.T) {
	agent := kubernetes.NewAgent(nopReporter{})

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:         "k8s-test",
		Type:       kubernetes.TargetType,
		Name:       "test-cluster",
		Properties: map[string]string{"api_server": ""},
	})

	err := agent.Deliver(context.Background(), target, "d1", nil, domain.DeliveryAuth{Token: "some-token"}, nil, 1)
	if err == nil {
		t.Fatal("expected error for empty api_server")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument, got: %v", err)
	}
}

func TestAgent_Remove_EmptyAPIServer(t *testing.T) {
	agent := kubernetes.NewAgent(nopReporter{})

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:         "k8s-test",
		Type:       kubernetes.TargetType,
		Name:       "test-cluster",
		Properties: map[string]string{"api_server": ""},
	})

	err := agent.Remove(context.Background(), target, "d1", nil, domain.DeliveryAuth{Token: "some-token"}, nil, 1)
	if err == nil {
		t.Fatal("expected error for empty api_server")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument, got: %v", err)
	}
}

func TestAgent_Deliver_MissingToken(t *testing.T) {
	agent := kubernetes.NewAgent(nopReporter{})

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": "https://127.0.0.1:6443",
		},
	})

	err := agent.Deliver(context.Background(), target, "d1", nil, domain.DeliveryAuth{}, nil, 1)
	if err == nil {
		t.Fatal("expected error for missing token")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument, got: %v", err)
	}
}

func TestAgent_Deliver_BadAPIServer(t *testing.T) {
	reporter := newChannelReporter()
	agent := kubernetes.NewAgent(reporter)

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": "https://127.0.0.1:1",
		},
	})

	auth := domain.DeliveryAuth{Token: "not-a-real-token"}
	manifests := []domain.Manifest{{
		ManifestType: "raw",
		Raw:          json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"test","namespace":"default"},"data":{"key":"value"}}`),
	}}

	err := agent.Deliver(context.Background(), target, "d1", manifests, auth, nil, 1)
	if err != nil {
		t.Fatalf("Deliver should not return error: %v", err)
	}

	asyncResult := awaitDone(t, reporter.done)
	if asyncResult.State != domain.DeliveryStateFailed {
		t.Errorf("async State = %q, want %q", asyncResult.State, domain.DeliveryStateFailed)
	}
}

func TestAgent_Remove_MissingAPIServer(t *testing.T) {
	agent := kubernetes.NewAgent(nopReporter{})

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:         "k8s-test",
		Type:       kubernetes.TargetType,
		Name:       "test-cluster",
		Properties: map[string]string{},
	})

	err := agent.Remove(context.Background(), target, "d1", nil, domain.DeliveryAuth{Token: "some-token"}, nil, 1)
	if err == nil {
		t.Fatal("expected error for missing api_server")
	}
}

func TestAgent_Remove_EmptyManifests(t *testing.T) {
	agent := kubernetes.NewAgent(nopReporter{})

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": "https://127.0.0.1:6443",
		},
	})

	if err := agent.Remove(context.Background(), target, "d1", nil, domain.DeliveryAuth{Token: "some-token"}, nil, 1); err != nil {
		t.Fatalf("Remove with empty manifests: %v", err)
	}
}

func TestAgent_Deliver_Unauthorized_ReportsAuthFailed(t *testing.T) {
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"Unauthorized","reason":"Unauthorized","code":401}`)
	}))
	defer ts.Close()

	reporter := newChannelReporter()
	agent := kubernetes.NewAgent(reporter)

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": ts.URL,
			"ca_cert":    tlsServerCAPEM(ts),
		},
	})

	auth := domain.DeliveryAuth{Token: "expired-token"}
	manifests := []domain.Manifest{{
		ManifestType: "raw",
		Raw:          json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"test","namespace":"default"},"data":{"key":"value"}}`),
	}}

	err := agent.Deliver(context.Background(), target, "d1", manifests, auth, nil, 1)
	if err != nil {
		t.Fatalf("Deliver should not return error: %v", err)
	}

	asyncResult := awaitDone(t, reporter.done)
	if asyncResult.State != domain.DeliveryStateAuthFailed {
		t.Errorf("async State = %q, want %q; message: %s", asyncResult.State, domain.DeliveryStateAuthFailed, asyncResult.Message)
	}
}

func TestAgent_Deliver_Forbidden_ReportsAuthFailed(t *testing.T) {
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprintf(w, `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"Forbidden","reason":"Forbidden","code":403}`)
	}))
	defer ts.Close()

	reporter := newChannelReporter()
	agent := kubernetes.NewAgent(reporter)

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": ts.URL,
			"ca_cert":    tlsServerCAPEM(ts),
		},
	})

	auth := domain.DeliveryAuth{Token: "some-token"}
	manifests := []domain.Manifest{{
		ManifestType: "raw",
		Raw:          json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"test","namespace":"default"},"data":{"key":"value"}}`),
	}}

	err := agent.Deliver(context.Background(), target, "d1", manifests, auth, nil, 1)
	if err != nil {
		t.Fatalf("Deliver should not return error: %v", err)
	}

	asyncResult := awaitDone(t, reporter.done)
	if asyncResult.State != domain.DeliveryStateAuthFailed {
		t.Errorf("async State = %q, want %q; message: %s", asyncResult.State, domain.DeliveryStateAuthFailed, asyncResult.Message)
	}
}

func TestAgent_Deliver_AttestationFailure_ReturnsAuthFailed(t *testing.T) {
	reporter := newChannelReporter()
	agent := kubernetes.NewAgent(reporter)

	trustBundle := `[{"issuer_url":"https://trusted.example.com","jwks_uri":"https://trusted.example.com/jwks","enrollment_audience":"enroll"}]`
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server":   "https://127.0.0.1:6443",
			"trust_bundle": trustBundle,
		},
	})

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

	err := agent.Deliver(context.Background(), target, "d1", nil, domain.DeliveryAuth{}, att, 1)
	if err != nil {
		t.Fatalf("Deliver should not return error: %v", err)
	}
	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateAuthFailed {
		t.Errorf("State = %q, want %q; message: %s", result.State, domain.DeliveryStateAuthFailed, result.Message)
	}
}

func TestAgent_Deliver_WithAttestation_NoTrustBundle_ReturnsAuthFailed(t *testing.T) {
	reporter := newChannelReporter()
	agent := kubernetes.NewAgent(reporter)

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": "https://127.0.0.1:6443",
		},
	})

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

	err := agent.Deliver(context.Background(), target, "d1", nil, domain.DeliveryAuth{}, att, 1)
	if err != nil {
		t.Fatalf("Deliver should not return error: %v", err)
	}
	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateAuthFailed {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateAuthFailed)
	}
}

func TestAgent_Deliver_VerifierCacheReuse(t *testing.T) {
	trustBundle := `[{"issuer_url":"https://trusted.example.com","jwks_uri":"https://trusted.example.com/jwks","enrollment_audience":"enroll"}]`
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server":   "https://127.0.0.1:6443",
			"trust_bundle": trustBundle,
		},
	})

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

	reporter := newChannelReporter()
	agent := kubernetes.NewAgent(reporter)

	_ = agent.Deliver(context.Background(), target, "d1", nil, domain.DeliveryAuth{}, att, 1)
	result1 := awaitDone(t, reporter.done)
	if result1.State != domain.DeliveryStateAuthFailed {
		t.Errorf("first: State = %q, want AuthFailed", result1.State)
	}

	_ = agent.Deliver(context.Background(), target, "d2", nil, domain.DeliveryAuth{}, att, 1)
	result2 := awaitDone(t, reporter.done)
	if result2.State != domain.DeliveryStateAuthFailed {
		t.Errorf("second: State = %q, want AuthFailed", result2.State)
	}
}

func TestAgent_Deliver_WithAttestation_NoTokenRequired(t *testing.T) {
	reporter := newChannelReporter()
	agent := kubernetes.NewAgent(reporter)

	trustBundle := `[{"issuer_url":"https://trusted.example.com","jwks_uri":"https://trusted.example.com/jwks","enrollment_audience":"enroll"}]`
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server":   "https://127.0.0.1:6443",
			"trust_bundle": trustBundle,
		},
	})

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

	err := agent.Deliver(context.Background(), target, "d1", nil, domain.DeliveryAuth{}, att, 1)
	if err != nil {
		t.Fatalf("Deliver should not return error: %v", err)
	}
	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateAuthFailed {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateAuthFailed)
	}
}

func TestAgent_Remove_AttestationFailure_ReportsAuthFailed(t *testing.T) {
	reporter := newChannelReporter()
	agent := kubernetes.NewAgent(reporter)

	trustBundle := `[{"issuer_url":"https://trusted.example.com","jwks_uri":"https://trusted.example.com/jwks","enrollment_audience":"enroll"}]`
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server":   "https://127.0.0.1:6443",
			"trust_bundle": trustBundle,
		},
	})

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

	err := agent.Remove(context.Background(), target, "d1", nil, domain.DeliveryAuth{}, att, 1)
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

func TestAgent_Remove_WithAttestation_NoTrustBundle_ReportsAuthFailed(t *testing.T) {
	reporter := newChannelReporter()
	agent := kubernetes.NewAgent(reporter)

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": "https://127.0.0.1:6443",
		},
	})

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

	err := agent.Remove(context.Background(), target, "d1", nil, domain.DeliveryAuth{}, att, 1)
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
