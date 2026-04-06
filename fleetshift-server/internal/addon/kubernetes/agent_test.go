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
	result, err := agent.Deliver(context.Background(), target, "d1", nil, auth, &domain.DeliverySignaler{})
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

	result, err := agent.Deliver(context.Background(), target, "d1", nil, domain.DeliveryAuth{}, &domain.DeliverySignaler{})
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

	result, err := agent.Deliver(context.Background(), target, "d1", manifests, auth, signaler)
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

func TestRemove_EmptyManifests_ReturnsNil(t *testing.T) {
	agent := kubernetes.NewAgent()

	target := domain.TargetInfo{ID: "k8s-test", Type: kubernetes.TargetType, Name: "test-cluster"}
	if err := agent.Remove(context.Background(), target, "d1", nil, domain.DeliveryAuth{}, &domain.DeliverySignaler{}); err != nil {
		t.Fatalf("Remove with nil manifests: %v", err)
	}
}

// fakeKubeAPI returns a TLS test server that serves minimal Kubernetes
// discovery responses for core/v1 ConfigMaps and delegates DELETE
// requests to deleteHandler.
func fakeKubeAPI(t *testing.T, deleteHandler http.HandlerFunc) (*httptest.Server, domain.TargetInfo) {
	t.Helper()
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/api" && r.Method == http.MethodGet:
			fmt.Fprint(w, `{"versions":["v1"]}`)
		case r.URL.Path == "/apis" && r.Method == http.MethodGet:
			fmt.Fprint(w, `{"kind":"APIGroupList","apiVersion":"v1","groups":[]}`)
		case r.URL.Path == "/api/v1" && r.Method == http.MethodGet:
			fmt.Fprint(w, `{"groupVersion":"v1","resources":[{"name":"configmaps","namespaced":true,"kind":"ConfigMap","verbs":["create","delete","get","list","patch","update","watch"]}]}`)
		case strings.HasPrefix(r.URL.Path, "/api/v1/namespaces/") && r.Method == http.MethodDelete:
			deleteHandler(w, r)
		default:
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`)
		}
	}))
	t.Cleanup(ts.Close)

	target := domain.TargetInfo{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": ts.URL,
			"ca_cert":    tlsServerCAPEM(ts),
		},
	}
	return ts, target
}

var testConfigMap = json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"test","namespace":"default"},"data":{"key":"value"}}`)

func TestRemove_DeletesResources(t *testing.T) {
	var deleted bool
	_, target := fakeKubeAPI(t, func(w http.ResponseWriter, r *http.Request) {
		deleted = true
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","status":"Success"}`)
	})

	agent := kubernetes.NewAgent()
	manifests := []domain.Manifest{{ResourceType: "kubernetes", Raw: testConfigMap}}
	auth := domain.DeliveryAuth{Token: "valid-token"}

	err := agent.Remove(context.Background(), target, "d1", manifests, auth, &domain.DeliverySignaler{})
	if err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if !deleted {
		t.Error("expected DELETE request to be sent")
	}
}

func TestRemove_HandlesNotFound(t *testing.T) {
	_, target := fakeKubeAPI(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","message":"configmaps \"test\" not found","reason":"NotFound","code":404}`)
	})

	agent := kubernetes.NewAgent()
	manifests := []domain.Manifest{{ResourceType: "kubernetes", Raw: testConfigMap}}
	auth := domain.DeliveryAuth{Token: "valid-token"}

	err := agent.Remove(context.Background(), target, "d1", manifests, auth, &domain.DeliverySignaler{})
	if err != nil {
		t.Fatalf("Remove should succeed when resource is already gone: %v", err)
	}
}

func TestRemove_AuthError(t *testing.T) {
	// Return 401 for all requests (including discovery).
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","message":"Unauthorized","reason":"Unauthorized","code":401}`)
	}))
	defer ts.Close()

	target := domain.TargetInfo{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": ts.URL,
			"ca_cert":    tlsServerCAPEM(ts),
		},
	}

	agent := kubernetes.NewAgent()
	manifests := []domain.Manifest{{ResourceType: "kubernetes", Raw: testConfigMap}}
	auth := domain.DeliveryAuth{Token: "expired-token"}

	err := agent.Remove(context.Background(), target, "d1", manifests, auth, &domain.DeliverySignaler{})
	if err == nil {
		t.Fatal("expected error for unauthorized request")
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

	result, err := agent.Deliver(context.Background(), target, "d1", manifests, auth, signaler)
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

	result, err := agent.Deliver(context.Background(), target, "d1", manifests, auth, signaler)
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
