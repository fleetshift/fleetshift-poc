package kubernetes_test

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func tlsServerCAPEM(ts *httptest.Server) string {
	return string(pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: ts.Certificate().Raw,
	}))
}

// channelDeliveryObserver collects events and completion results on
// channels, enabling deterministic waits in tests with async delivery.
type channelDeliveryObserver struct {
	mu     sync.Mutex
	events []domain.DeliveryEvent
	ch     chan domain.DeliveryEvent
	done   chan domain.DeliveryResult
}

func newChannelDeliveryObserver() *channelDeliveryObserver {
	return &channelDeliveryObserver{
		ch:   make(chan domain.DeliveryEvent, 100),
		done: make(chan domain.DeliveryResult, 1),
	}
}

func (o *channelDeliveryObserver) EventEmitted(ctx context.Context, _ domain.DeliveryID, _ domain.TargetInfo, e domain.DeliveryEvent) (context.Context, domain.EventEmittedProbe) {
	o.mu.Lock()
	o.events = append(o.events, e)
	o.mu.Unlock()
	o.ch <- e
	return ctx, domain.NoOpEventEmittedProbe{}
}

func (o *channelDeliveryObserver) Completed(ctx context.Context, _ domain.DeliveryID, _ domain.TargetInfo, result domain.DeliveryResult) (context.Context, domain.CompletedProbe) {
	o.done <- result
	return ctx, domain.NoOpCompletedProbe{}
}

func newChannelSignaler(obs *channelDeliveryObserver) *domain.DeliverySignaler {
	return domain.NewDeliverySignaler("", "", domain.TargetInfo{}, nil, nil, obs)
}

func TestAgent_Deliver_MissingAPIServer(t *testing.T) {
	agent := kubernetes.NewAgent()

	target := domain.TargetInfo{
		ID:         "k8s-test",
		Type:       kubernetes.TargetType,
		Name:       "test-cluster",
		Properties: map[string]string{},
	}

	auth := domain.DeliveryAuth{Token: "some-token"}
	result, err := agent.Deliver(context.Background(), target, "d1", nil, auth, nil, &domain.DeliverySignaler{})
	if err == nil {
		t.Fatal("expected error for missing api_server")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument, got: %v", err)
	}
	if result.State != domain.DeliveryStateFailed {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateFailed)
	}
}

func TestAgent_Deliver_MissingToken(t *testing.T) {
	agent := kubernetes.NewAgent()

	target := domain.TargetInfo{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": "https://127.0.0.1:6443",
		},
	}

	result, err := agent.Deliver(context.Background(), target, "d1", nil, domain.DeliveryAuth{}, nil, &domain.DeliverySignaler{})
	if err == nil {
		t.Fatal("expected error for missing token")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument, got: %v", err)
	}
	if result.State != domain.DeliveryStateFailed {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateFailed)
	}
}

func TestAgent_Deliver_BadAPIServer(t *testing.T) {
	agent := kubernetes.NewAgent()
	obs := newChannelDeliveryObserver()
	signaler := newChannelSignaler(obs)

	target := domain.TargetInfo{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": "https://127.0.0.1:1",
		},
	}

	auth := domain.DeliveryAuth{Token: "not-a-real-token"}
	manifests := []domain.Manifest{{
		ResourceType: "raw",
		Raw:          json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"test","namespace":"default"},"data":{"key":"value"}}`),
	}}

	result, err := agent.Deliver(context.Background(), target, "d1", manifests, auth, nil, signaler)
	if err != nil {
		t.Fatalf("Deliver should not return error after ack: %v", err)
	}
	if result.State != domain.DeliveryStateAccepted {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateAccepted)
	}

	asyncResult := <-obs.done
	if asyncResult.State != domain.DeliveryStateFailed {
		t.Errorf("async State = %q, want %q", asyncResult.State, domain.DeliveryStateFailed)
	}
}

func TestAgent_Remove_IsNoop(t *testing.T) {
	agent := kubernetes.NewAgent()

	target := domain.TargetInfo{ID: "k8s-test", Type: kubernetes.TargetType, Name: "test-cluster"}
	if err := agent.Remove(context.Background(), target, "d1", &domain.DeliverySignaler{}); err != nil {
		t.Fatalf("Remove: %v", err)
	}
}

func TestAgent_Deliver_Unauthorized_ReportsAuthFailed(t *testing.T) {
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"Unauthorized","reason":"Unauthorized","code":401}`)
	}))
	defer ts.Close()

	agent := kubernetes.NewAgent()
	obs := newChannelDeliveryObserver()
	signaler := newChannelSignaler(obs)

	target := domain.TargetInfo{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": ts.URL,
			"ca_cert":    tlsServerCAPEM(ts),
		},
	}

	auth := domain.DeliveryAuth{Token: "expired-token"}
	manifests := []domain.Manifest{{
		ResourceType: "raw",
		Raw:          json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"test","namespace":"default"},"data":{"key":"value"}}`),
	}}

	result, err := agent.Deliver(context.Background(), target, "d1", manifests, auth, nil, signaler)
	if err != nil {
		t.Fatalf("Deliver should not return error after ack: %v", err)
	}
	if result.State != domain.DeliveryStateAccepted {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateAccepted)
	}

	asyncResult := <-obs.done
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

	agent := kubernetes.NewAgent()
	obs := newChannelDeliveryObserver()
	signaler := newChannelSignaler(obs)

	target := domain.TargetInfo{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": ts.URL,
			"ca_cert":    tlsServerCAPEM(ts),
		},
	}

	auth := domain.DeliveryAuth{Token: "some-token"}
	manifests := []domain.Manifest{{
		ResourceType: "raw",
		Raw:          json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"test","namespace":"default"},"data":{"key":"value"}}`),
	}}

	result, err := agent.Deliver(context.Background(), target, "d1", manifests, auth, nil, signaler)
	if err != nil {
		t.Fatalf("Deliver should not return error after ack: %v", err)
	}
	if result.State != domain.DeliveryStateAccepted {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateAccepted)
	}

	asyncResult := <-obs.done
	if asyncResult.State != domain.DeliveryStateAuthFailed {
		t.Errorf("async State = %q, want %q; message: %s", asyncResult.State, domain.DeliveryStateAuthFailed, asyncResult.Message)
	}
}

func TestAgent_Deliver_AttestationFailure_ReturnsAuthFailed(t *testing.T) {
	agent := kubernetes.NewAgent()

	trustBundle := `[{"issuer_url":"https://trusted.example.com","jwks_uri":"https://trusted.example.com/jwks","enrollment_audience":"enroll"}]`
	target := domain.TargetInfo{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server":   "https://127.0.0.1:6443",
			"trust_bundle": trustBundle,
		},
	}

	att := &domain.Attestation{
		Input: domain.SignedInput{
			Sig: domain.Signature{
				Signer: domain.FederatedIdentity{
					Issuer: "https://untrusted.example.com",
				},
			},
		},
	}

	result, err := agent.Deliver(context.Background(), target, "d1", nil, domain.DeliveryAuth{}, att, &domain.DeliverySignaler{})
	if err != nil {
		t.Fatalf("Deliver should not return error: %v", err)
	}
	if result.State != domain.DeliveryStateAuthFailed {
		t.Errorf("State = %q, want %q; message: %s", result.State, domain.DeliveryStateAuthFailed, result.Message)
	}
}

func TestAgent_Deliver_WithAttestation_NoTrustBundle_ReturnsAuthFailed(t *testing.T) {
	agent := kubernetes.NewAgent()

	target := domain.TargetInfo{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": "https://127.0.0.1:6443",
		},
	}

	att := &domain.Attestation{
		Input: domain.SignedInput{
			Sig: domain.Signature{
				Signer: domain.FederatedIdentity{
					Issuer: "https://untrusted.example.com",
				},
			},
		},
	}

	result, err := agent.Deliver(context.Background(), target, "d1", nil, domain.DeliveryAuth{}, att, &domain.DeliverySignaler{})
	if err != nil {
		t.Fatalf("Deliver should not return error: %v", err)
	}
	if result.State != domain.DeliveryStateAuthFailed {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateAuthFailed)
	}
}

func TestAgent_Deliver_VerifierCacheReuse(t *testing.T) {
	agent := kubernetes.NewAgent()

	trustBundle := `[{"issuer_url":"https://trusted.example.com","jwks_uri":"https://trusted.example.com/jwks","enrollment_audience":"enroll"}]`
	target := domain.TargetInfo{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server":   "https://127.0.0.1:6443",
			"trust_bundle": trustBundle,
		},
	}

	att := &domain.Attestation{
		Input: domain.SignedInput{
			Sig: domain.Signature{
				Signer: domain.FederatedIdentity{
					Issuer: "https://untrusted.example.com",
				},
			},
		},
	}

	// First delivery builds and caches the verifier.
	result1, _ := agent.Deliver(context.Background(), target, "d1", nil, domain.DeliveryAuth{}, att, &domain.DeliverySignaler{})
	if result1.State != domain.DeliveryStateAuthFailed {
		t.Errorf("first: State = %q, want AuthFailed", result1.State)
	}

	// Second delivery with same trust bundle should reuse cached verifier.
	result2, _ := agent.Deliver(context.Background(), target, "d2", nil, domain.DeliveryAuth{}, att, &domain.DeliverySignaler{})
	if result2.State != domain.DeliveryStateAuthFailed {
		t.Errorf("second: State = %q, want AuthFailed", result2.State)
	}
}

func TestAgent_Deliver_WithAttestation_NoTokenRequired(t *testing.T) {
	agent := kubernetes.NewAgent()

	trustBundle := `[{"issuer_url":"https://trusted.example.com","jwks_uri":"https://trusted.example.com/jwks","enrollment_audience":"enroll"}]`
	target := domain.TargetInfo{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server":   "https://127.0.0.1:6443",
			"trust_bundle": trustBundle,
		},
	}

	att := &domain.Attestation{
		Input: domain.SignedInput{
			Sig: domain.Signature{
				Signer: domain.FederatedIdentity{
					Issuer: "https://untrusted.example.com",
				},
			},
		},
	}

	// No token — the attestation code path doesn't require one.
	// Verification will fail (untrusted issuer), proving we reached the
	// attestation path rather than the token-required check.
	result, err := agent.Deliver(context.Background(), target, "d1", nil, domain.DeliveryAuth{}, att, &domain.DeliverySignaler{})
	if err != nil {
		t.Fatalf("Deliver should not return error: %v", err)
	}
	if result.State != domain.DeliveryStateAuthFailed {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateAuthFailed)
	}
}

