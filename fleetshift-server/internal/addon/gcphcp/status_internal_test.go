package gcphcp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

type fakeNodepoolStatusClient struct {
	listSeq    [][]map[string]any
	listCalls  int
	statusByID map[string][]map[string]any
	statusCall map[string]int
}

type recordingDeliveryObserver struct {
	domain.NoOpDeliveryObserver
	mu     sync.Mutex
	events []domain.DeliveryEvent
}

func (o *recordingDeliveryObserver) EventEmitted(
	ctx context.Context,
	_ domain.DeliveryID,
	_ domain.TargetInfo,
	event domain.DeliveryEvent,
) (context.Context, domain.EventEmittedProbe) {
	o.mu.Lock()
	o.events = append(o.events, event)
	o.mu.Unlock()
	return ctx, domain.NoOpEventEmittedProbe{}
}

func (o *recordingDeliveryObserver) snapshot() []domain.DeliveryEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	events := make([]domain.DeliveryEvent, len(o.events))
	copy(events, o.events)
	return events
}

func newRecordingSignaler(obs *recordingDeliveryObserver) *domain.DeliverySignaler {
	return domain.NewDeliverySignaler(
		"ful-1",
		"del-1",
		domain.TargetInfo{ID: "gcphcp-test", Type: "gcphcp"},
		nil,
		nil,
		obs,
	)
}

func (f *fakeNodepoolStatusClient) ListNodepools(_ context.Context, _ string) ([]map[string]any, error) {
	idx := f.listCalls
	if idx >= len(f.listSeq) {
		idx = len(f.listSeq) - 1
	}
	f.listCalls++
	return f.listSeq[idx], nil
}

func (f *fakeNodepoolStatusClient) GetNodepoolStatus(_ context.Context, nodepoolID string) (map[string]any, error) {
	if f.statusCall == nil {
		f.statusCall = make(map[string]int)
	}
	seq := f.statusByID[nodepoolID]
	idx := f.statusCall[nodepoolID]
	if idx >= len(seq) {
		idx = len(seq) - 1
	}
	f.statusCall[nodepoolID]++
	return seq[idx], nil
}

func TestParseNodepoolPhase(t *testing.T) {
	got := ParseNodepoolPhase(map[string]any{
		"status": map[string]any{"phase": "Ready"},
	})
	if got != "Ready" {
		t.Fatalf("ParseNodepoolPhase() = %q, want Ready", got)
	}
}

func TestPollDesiredNodepoolsHealthy_ReadyOnFirstCheck(t *testing.T) {
	obs := &recordingDeliveryObserver{}
	client := &fakeNodepoolStatusClient{
		listSeq: [][]map[string]any{{
			{"id": "np-1", "name": "test-wa"},
			{"id": "np-2", "name": "test-wb"},
		}},
		statusByID: map[string][]map[string]any{
			"np-1": {{
				"status": map[string]any{
					"phase":   "Ready",
					"reason":  "AllControllersReady",
					"message": "NodePool is ready with 1 controllers operational",
				},
			}},
			"np-2": {{
				"status": map[string]any{
					"phase":   "Ready",
					"reason":  "AllControllersReady",
					"message": "NodePool is ready with 1 controllers operational",
				},
			}},
		},
	}

	err := PollDesiredNodepoolsHealthy(context.Background(), client, "cluster-123", "test", []NodepoolSpec{
		{ID: "wa"},
		{ID: "wb"},
	}, newRecordingSignaler(obs))
	if err != nil {
		t.Fatalf("PollDesiredNodepoolsHealthy() error = %v", err)
	}

	events := obs.snapshot()
	if len(events) != 2 {
		t.Fatalf("expected 2 status events, got %d", len(events))
	}
	want := `Nodepool test-wa status: phase=Ready reason=AllControllersReady message="NodePool is ready with 1 controllers operational"`
	if events[0].Message != want {
		t.Fatalf("first message = %q, want %q", events[0].Message, want)
	}
	want = `Nodepool test-wb status: phase=Ready reason=AllControllersReady message="NodePool is ready with 1 controllers operational"`
	if events[1].Message != want {
		t.Fatalf("second message = %q, want %q", events[1].Message, want)
	}
}

func TestPollDesiredNodepoolsHealthy_FailedNodepool(t *testing.T) {
	client := &fakeNodepoolStatusClient{
		listSeq: [][]map[string]any{{
			{"id": "np-1", "name": "test-wa"},
		}},
		statusByID: map[string][]map[string]any{
			"np-1": {{
				"status": map[string]any{
					"phase":   "Failed",
					"message": "quota exceeded",
				},
			}},
		},
	}

	err := PollDesiredNodepoolsHealthy(context.Background(), client, "cluster-123", "test", []NodepoolSpec{
		{ID: "wa"},
	}, &domain.DeliverySignaler{})
	if err == nil {
		t.Fatal("expected failed nodepool error")
	}
	if !strings.Contains(err.Error(), "test-wa") || !strings.Contains(err.Error(), "quota exceeded") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPollDesiredNodepoolsHealthy_WaitsUntilReady(t *testing.T) {
	origInterval := nodepoolPollInterval
	origTimeout := nodepoolPollTimeout
	nodepoolPollInterval = 5 * time.Millisecond
	nodepoolPollTimeout = 100 * time.Millisecond
	defer func() {
		nodepoolPollInterval = origInterval
		nodepoolPollTimeout = origTimeout
	}()

	client := &fakeNodepoolStatusClient{
		listSeq: [][]map[string]any{
			{},
			{{"id": "np-1", "name": "test-wa"}},
		},
		statusByID: map[string][]map[string]any{
			"np-1": {
				{"status": map[string]any{"phase": "Progressing"}},
				{"status": map[string]any{"phase": "Ready"}},
			},
		},
	}

	err := PollDesiredNodepoolsHealthy(context.Background(), client, "cluster-123", "test", []NodepoolSpec{
		{ID: "wa"},
	}, &domain.DeliverySignaler{})
	if err != nil {
		t.Fatalf("PollDesiredNodepoolsHealthy() error = %v", err)
	}
	if client.listCalls < 2 {
		t.Fatalf("expected multiple polling iterations, got %d", client.listCalls)
	}
}

func TestPollClusterReady_UsesConfigurableTimeout(t *testing.T) {
	origInterval := clusterPollInterval
	origTimeout := clusterPollTimeout
	clusterPollInterval = 5 * time.Millisecond
	clusterPollTimeout = 20 * time.Millisecond
	defer func() {
		clusterPollInterval = origInterval
		clusterPollTimeout = origTimeout
	}()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/clusters/c-123" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		if err := json.NewEncoder(w).Encode(map[string]any{
			"status": map[string]any{
				"phase":   "Progressing",
				"reason":  "ControllersProvisioning",
				"message": "Controllers are provisioning cluster resources",
			},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	obs := &recordingDeliveryObserver{}
	client := NewCLSClient(server.URL, "token", "email@example.com", nil)
	err := PollClusterReady(context.Background(), client, "c-123", newRecordingSignaler(obs))
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !strings.Contains(err.Error(), "timeout waiting for cluster to become ready") {
		t.Fatalf("unexpected error: %v", err)
	}

	events := obs.snapshot()
	if len(events) == 0 {
		t.Fatal("expected cluster status events")
	}
	want := `Cluster status: phase=Progressing reason=ControllersProvisioning message="Controllers are provisioning cluster resources"`
	if events[0].Message != want {
		t.Fatalf("first message = %q, want %q", events[0].Message, want)
	}
}

func TestEmitClusterReadyTransition_EmitsProgressEvent(t *testing.T) {
	obs := &recordingDeliveryObserver{}

	emitClusterReadyTransition(context.Background(), newRecordingSignaler(obs))

	events := obs.snapshot()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	want := "Cluster readiness satisfied; proceeding with guest bootstrap and desired nodepool health checks"
	if events[0].Message != want {
		t.Fatalf("message = %q, want %q", events[0].Message, want)
	}
}

func TestPollClusterDeleted_ReturnsNon404Errors(t *testing.T) {
	origInterval := clusterPollInterval
	origTimeout := clusterPollTimeout
	clusterPollInterval = 5 * time.Millisecond
	clusterPollTimeout = 20 * time.Millisecond
	defer func() {
		clusterPollInterval = origInterval
		clusterPollTimeout = origTimeout
	}()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/clusters/c-123" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		http.Error(w, "backend unavailable", http.StatusBadGateway)
	}))
	defer server.Close()

	client := NewCLSClient(server.URL, "token", "email@example.com", nil)
	err := PollClusterDeleted(context.Background(), client, "c-123", &domain.DeliverySignaler{})
	if err == nil {
		t.Fatal("expected get cluster error")
	}
	if !strings.Contains(err.Error(), "get cluster") || !strings.Contains(err.Error(), "HTTP 502") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPollClusterDeleted_SucceedsOnHTTP404(t *testing.T) {
	origInterval := clusterPollInterval
	origTimeout := clusterPollTimeout
	clusterPollInterval = 5 * time.Millisecond
	clusterPollTimeout = 20 * time.Millisecond
	defer func() {
		clusterPollInterval = origInterval
		clusterPollTimeout = origTimeout
	}()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/clusters/c-123" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer server.Close()

	client := NewCLSClient(server.URL, "token", "email@example.com", nil)
	if err := PollClusterDeleted(context.Background(), client, "c-123", &domain.DeliverySignaler{}); err != nil {
		t.Fatalf("expected HTTP 404 to count as deleted, got %v", err)
	}
}

func TestPollClusterDeleted_DoesNotTreat404TextInBodyAsDeletion(t *testing.T) {
	origInterval := clusterPollInterval
	origTimeout := clusterPollTimeout
	clusterPollInterval = 5 * time.Millisecond
	clusterPollTimeout = 20 * time.Millisecond
	defer func() {
		clusterPollInterval = origInterval
		clusterPollTimeout = origTimeout
	}()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/clusters/c-123" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		http.Error(w, "upstream lookup mentioned stale 404 cache entry", http.StatusBadGateway)
	}))
	defer server.Close()

	client := NewCLSClient(server.URL, "token", "email@example.com", nil)
	err := PollClusterDeleted(context.Background(), client, "c-123", &domain.DeliverySignaler{})
	if err == nil {
		t.Fatal("expected non-404 status with 404 text in body to remain an error")
	}
	if !strings.Contains(err.Error(), "get cluster") || !strings.Contains(err.Error(), "HTTP 502") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEmitFailureStatusSnapshot_EmitsCuratedRedactedDetail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/clusters/c-123":
			if err := json.NewEncoder(w).Encode(map[string]any{
				"id":   "c-123",
				"name": "test-cluster",
				"spec": map[string]any{
					"serviceAccountSigningKey": "super-secret-signing-key",
					"platform": map[string]any{
						"gcp": map[string]any{
							"projectID": "project-123",
							"workloadIdentity": map[string]any{
								"projectNumber": "123456789",
								"serviceAccountsRef": map[string]any{
									"controlPlaneEmail": "broker@example.com",
								},
							},
						},
					},
					"release": map[string]any{
						"version": "4.22.0-ec.5",
					},
				},
			}); err != nil {
				t.Fatalf("encode cluster response: %v", err)
			}
		case "/api/v1/clusters/c-123/status":
			if err := json.NewEncoder(w).Encode(map[string]any{
				"status": map[string]any{
					"phase":   "Failed",
					"reason":  "InfrastructureNotReady",
					"message": "subnet quota exceeded",
				},
				"controller_status": []any{
					map[string]any{
						"controller_name": "cls-hypershift-client",
						"conditions": []any{
							map[string]any{
								"type":    "Available",
								"status":  "True",
								"reason":  "AsExpected",
								"message": "controller available",
							},
							map[string]any{
								"type":    "Degraded",
								"status":  "True",
								"reason":  "QuotaExceeded",
								"message": "quota exceeded",
							},
							map[string]any{
								"type":    "APIServer",
								"status":  "False",
								"reason":  "EndpointNotReady",
								"message": "endpoint still provisioning",
							},
						},
					},
				},
			}); err != nil {
				t.Fatalf("encode cluster status response: %v", err)
			}
		case "/api/v1/nodepools":
			if got := r.URL.Query().Get("clusterId"); got != "c-123" {
				t.Fatalf("clusterId = %q, want c-123", got)
			}
			if err := json.NewEncoder(w).Encode(map[string]any{
				"nodepools": []any{
					map[string]any{
						"id":   "np-1",
						"name": "worker-a",
						"spec": map[string]any{
							"platform": map[string]any{
								"gcp": map[string]any{
									"serviceAccountEmail": "nodepool@example.com",
								},
							},
						},
					},
				},
			}); err != nil {
				t.Fatalf("encode nodepool list response: %v", err)
			}
		case "/api/v1/nodepools/np-1/status":
			if err := json.NewEncoder(w).Encode(map[string]any{
				"status": map[string]any{
					"phase":   "Progressing",
					"reason":  "ControllersProvisioning",
					"message": "nodepool resources are still provisioning",
				},
				"controller_status": []any{
					map[string]any{
						"controller_name": "cls-nodepool-controller",
						"conditions": []any{
							map[string]any{
								"type":    "Ready",
								"status":  "False",
								"reason":  "MachinesNotReady",
								"message": "waiting for machines",
							},
						},
						"metadata": map[string]any{
							"resources": map[string]any{
								"nodepool": map[string]any{
									"resource_status": map[string]any{
										"kubeconfig": map[string]any{"name": "should-not-be-logged"},
									},
								},
							},
						},
					},
				},
			}); err != nil {
				t.Fatalf("encode nodepool status response: %v", err)
			}
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer server.Close()

	obs := &recordingDeliveryObserver{}
	client := NewCLSClient(server.URL, "token", "email@example.com", nil)

	if err := emitFailureStatusSnapshot(
		context.Background(),
		client,
		"c-123",
		"test-cluster",
		newRecordingSignaler(obs),
	); err != nil {
		t.Fatalf("emitFailureStatusSnapshot() error = %v", err)
	}

	events := obs.snapshot()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Kind != domain.DeliveryEventWarning {
		t.Fatalf("kind = %q, want %q", events[0].Kind, domain.DeliveryEventWarning)
	}
	if !strings.HasPrefix(events[0].Message, "Redacted failure snapshot: ") {
		t.Fatalf("message = %q, want redacted snapshot prefix", events[0].Message)
	}

	var snapshot map[string]any
	if err := json.Unmarshal(events[0].Detail, &snapshot); err != nil {
		t.Fatalf("unmarshal detail: %v", err)
	}
	if got := snapshot["cluster_id"]; got != "c-123" {
		t.Fatalf("cluster_id = %v, want c-123", got)
	}
	if got := snapshot["cluster_name"]; got != "test-cluster" {
		t.Fatalf("cluster_name = %v, want test-cluster", got)
	}
	if got := snapshot["release_version"]; got != "4.22.0-ec.5" {
		t.Fatalf("release_version = %v, want 4.22.0-ec.5", got)
	}

	cluster, ok := snapshot["cluster"].(map[string]any)
	if !ok {
		t.Fatal("cluster snapshot missing")
	}
	if got := cluster["phase"]; got != "Failed" {
		t.Fatalf("cluster phase = %v, want Failed", got)
	}
	if got := cluster["api_server_present"]; got != false {
		t.Fatalf("cluster api_server_present = %v, want false", got)
	}

	nodepools, ok := snapshot["nodepools"].([]any)
	if !ok || len(nodepools) != 1 {
		t.Fatalf("nodepools = %#v, want 1 entry", snapshot["nodepools"])
	}
	nodepool, ok := nodepools[0].(map[string]any)
	if !ok {
		t.Fatal("nodepool snapshot missing")
	}
	if got := nodepool["name"]; got != "worker-a" {
		t.Fatalf("nodepool name = %v, want worker-a", got)
	}
	if got := nodepool["phase"]; got != "Progressing" {
		t.Fatalf("nodepool phase = %v, want Progressing", got)
	}
}

func TestEmitFailureStatusSnapshot_RedactsSensitiveFields(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/clusters/c-123":
			if err := json.NewEncoder(w).Encode(map[string]any{
				"id":   "c-123",
				"name": "test-cluster",
				"spec": map[string]any{
					"serviceAccountSigningKey": "super-secret-signing-key",
					"platform": map[string]any{
						"gcp": map[string]any{
							"projectID": "project-123",
							"workloadIdentity": map[string]any{
								"projectNumber": "123456789",
								"serviceAccountsRef": map[string]any{
									"controlPlaneEmail": "broker@example.com",
								},
							},
						},
					},
				},
			}); err != nil {
				t.Fatalf("encode cluster response: %v", err)
			}
		case "/api/v1/clusters/c-123/status":
			if err := json.NewEncoder(w).Encode(map[string]any{
				"status": map[string]any{
					"phase":   "Failed",
					"reason":  "InfrastructureNotReady",
					"message": "subnet quota exceeded",
				},
				"controller_status": []any{
					map[string]any{
						"controller_name": "cls-hypershift-client",
						"conditions": []any{
							map[string]any{
								"type":    "Degraded",
								"status":  "True",
								"reason":  "QuotaExceeded",
								"message": "quota exceeded",
							},
						},
						"metadata": map[string]any{
							"resources": map[string]any{
								"signing-key-secret": map[string]any{
									"status": "Created",
								},
								"rbac-setup-job": map[string]any{
									"resource_status": map[string]any{
										"kubeconfig": map[string]any{"name": "cluster-admin-kubeconfig"},
									},
								},
							},
						},
					},
				},
			}); err != nil {
				t.Fatalf("encode cluster status response: %v", err)
			}
		case "/api/v1/nodepools":
			if err := json.NewEncoder(w).Encode(map[string]any{
				"nodepools": []any{
					map[string]any{
						"id":   "np-1",
						"name": "worker-a",
						"spec": map[string]any{
							"platform": map[string]any{
								"gcp": map[string]any{
									"serviceAccountEmail": "nodepool@example.com",
								},
							},
						},
					},
				},
			}); err != nil {
				t.Fatalf("encode nodepool list response: %v", err)
			}
		case "/api/v1/nodepools/np-1/status":
			if err := json.NewEncoder(w).Encode(map[string]any{
				"status": map[string]any{
					"phase":   "Failed",
					"reason":  "MachineProvisionFailed",
					"message": "machine provisioning failed",
				},
				"controller_status": []any{
					map[string]any{
						"controller_name": "cls-nodepool-controller",
						"conditions": []any{
							map[string]any{
								"type":    "Ready",
								"status":  "False",
								"reason":  "MachinesNotReady",
								"message": "waiting for machines",
							},
						},
					},
				},
			}); err != nil {
				t.Fatalf("encode nodepool status response: %v", err)
			}
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer server.Close()

	obs := &recordingDeliveryObserver{}
	client := NewCLSClient(server.URL, "token", "email@example.com", nil)

	if err := emitFailureStatusSnapshot(
		context.Background(),
		client,
		"c-123",
		"test-cluster",
		newRecordingSignaler(obs),
	); err != nil {
		t.Fatalf("emitFailureStatusSnapshot() error = %v", err)
	}

	events := obs.snapshot()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	payload := events[0].Message + string(events[0].Detail)
	for _, forbidden := range []string{
		"serviceAccountSigningKey",
		"super-secret-signing-key",
		"projectNumber",
		"123456789",
		"broker@example.com",
		"nodepool@example.com",
		"signing-key-secret",
		"cluster-admin-kubeconfig",
	} {
		if strings.Contains(payload, forbidden) {
			t.Fatalf("payload unexpectedly contains %q: %s", forbidden, payload)
		}
	}
}
