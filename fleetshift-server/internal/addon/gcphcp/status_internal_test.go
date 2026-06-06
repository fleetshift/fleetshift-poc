package gcphcp

import (
	"context"
	"encoding/json"
	"fmt"
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

type testEventRecorder struct {
	mu     sync.Mutex
	events []domain.DeliveryEvent
}

func (r *testEventRecorder) ReportEvent(_ context.Context, _ domain.DeliveryID, _ domain.Generation, event domain.DeliveryEvent) error {
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()
	return nil
}

func (r *testEventRecorder) ReportResult(_ context.Context, _ domain.DeliveryID, _ domain.Generation, _ domain.DeliveryResult) error {
	return nil
}

func (r *testEventRecorder) ListActiveDeliveries(_ context.Context, _ []domain.TargetID) ([]domain.ActiveDelivery, error) {
	return nil, nil
}

func (r *testEventRecorder) snapshot() []domain.DeliveryEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	events := make([]domain.DeliveryEvent, len(r.events))
	copy(events, r.events)
	return events
}

func newTestProgress(rec *testEventRecorder) *deliveryProgress {
	return newDeliveryProgress(rec, "del-1", 1)
}

func noopProgress() *deliveryProgress {
	return &deliveryProgress{}
}

func (f *fakeNodepoolStatusClient) ListNodepools(_ context.Context, _ string) ([]map[string]any, error) {
	if len(f.listSeq) == 0 {
		return nil, fmt.Errorf("fakeNodepoolStatusClient: no ListNodepools responses configured")
	}
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
	seq, ok := f.statusByID[nodepoolID]
	if !ok || len(seq) == 0 {
		return nil, fmt.Errorf("fakeNodepoolStatusClient: no GetNodepoolStatus responses configured for %q", nodepoolID)
	}
	idx := f.statusCall[nodepoolID]
	if idx >= len(seq) {
		idx = len(seq) - 1
	}
	f.statusCall[nodepoolID]++
	return seq[idx], nil
}

func TestPollDesiredNodepoolsHealthy_ReadyOnFirstCheck(t *testing.T) {
	obs := &testEventRecorder{}
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
	}, newTestProgress(obs))
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
	}, noopProgress())
	if err == nil {
		t.Fatal("expected failed nodepool error")
	}
	if !strings.Contains(err.Error(), "test-wa") || !strings.Contains(err.Error(), "quota exceeded") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// withFastClusterPollTimers saves and restores clusterPollInterval
// and clusterPollTimeout, setting fast defaults (5ms interval,
// 20ms timeout) for testing.
func withFastClusterPollTimers(t *testing.T) {
	t.Helper()
	origInterval := clusterPollInterval
	origTimeout := clusterPollTimeout
	clusterPollInterval = 5 * time.Millisecond
	clusterPollTimeout = 20 * time.Millisecond
	t.Cleanup(func() {
		clusterPollInterval = origInterval
		clusterPollTimeout = origTimeout
	})
}

// withFastNodepoolPollTimers saves and restores nodepoolPollInterval
// and nodepoolPollTimeout, setting fast defaults (5ms interval,
// 100ms timeout) for testing.
func withFastNodepoolPollTimers(t *testing.T) {
	t.Helper()
	origInterval := nodepoolPollInterval
	origTimeout := nodepoolPollTimeout
	nodepoolPollInterval = 5 * time.Millisecond
	nodepoolPollTimeout = 100 * time.Millisecond
	t.Cleanup(func() {
		nodepoolPollInterval = origInterval
		nodepoolPollTimeout = origTimeout
	})
}

func TestPollDesiredNodepoolsHealthy_WaitsUntilReady(t *testing.T) {
	withFastNodepoolPollTimers(t)

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
	}, noopProgress())
	if err != nil {
		t.Fatalf("PollDesiredNodepoolsHealthy() error = %v", err)
	}
	if client.listCalls < 2 {
		t.Fatalf("expected multiple polling iterations, got %d", client.listCalls)
	}
}

func TestPollClusterReady_UsesConfigurableTimeout(t *testing.T) {
	withFastClusterPollTimers(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/clusters/c-123" {
			t.Errorf("unexpected path %q", r.URL.Path)
			http.Error(w, "unexpected path", http.StatusInternalServerError)
			return
		}
		if err := json.NewEncoder(w).Encode(map[string]any{
			"status": map[string]any{
				"phase":   "Progressing",
				"reason":  "ControllersProvisioning",
				"message": "Controllers are provisioning cluster resources",
			},
		}); err != nil {
			t.Errorf("encode response: %v", err)
		}
	}))
	defer server.Close()

	obs := &testEventRecorder{}
	client := NewCLSClient(server.URL, "token", "email@example.com", nil)
	err := PollClusterReady(context.Background(), client, "c-123", newTestProgress(obs))
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

func TestPollClusterReady_WaitsForControllerGeneration(t *testing.T) {
	withFastClusterPollTimers(t)

	// Models the real recovery bug: UpdateCluster bumps generation to 2,
	// the cluster object immediately shows Ready (transient), but the
	// /status endpoint still has controllers at the old generation until
	// they re-report.
	var statusCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/clusters/c-123/status":
			statusCalls++
			gen1Controllers := []any{
				map[string]any{
					"controller_name":     "cls-version-resolution-controller",
					"observed_generation": float64(1),
				},
				map[string]any{
					"controller_name":     "cls-hypershift-client",
					"observed_generation": float64(1),
				},
			}
			gen2Controllers := []any{
				map[string]any{
					"controller_name":     "cls-version-resolution-controller",
					"observed_generation": float64(2),
				},
				map[string]any{
					"controller_name":     "cls-hypershift-client",
					"observed_generation": float64(2),
				},
			}
			controllers := gen1Controllers
			if statusCalls >= 3 {
				controllers = gen2Controllers
			}
			if err := json.NewEncoder(w).Encode(map[string]any{
				"controller_status": controllers,
				"status":            map[string]any{"phase": "Ready"},
			}); err != nil {
				t.Errorf("encode response: %v", err)
			}

		case r.URL.Path == "/api/v1/clusters/c-123":
			if err := json.NewEncoder(w).Encode(map[string]any{
				"generation": float64(2),
				"status": map[string]any{
					"phase":              "Ready",
					"reason":             "AllControllersReady",
					"message":            "Cluster is ready with 1 controllers operational",
					"observedGeneration": float64(2),
				},
			}); err != nil {
				t.Errorf("encode response: %v", err)
			}

		default:
			http.Error(w, "unexpected path", http.StatusNotFound)
		}
	}))
	defer server.Close()

	clusterPollTimeout = 500 * time.Millisecond

	obs := &testEventRecorder{}
	client := NewCLSClient(server.URL, "token", "email@example.com", nil)
	err := PollClusterReady(context.Background(), client, "c-123", newTestProgress(obs))
	if err != nil {
		t.Fatalf("PollClusterReady() error = %v", err)
	}
	if statusCalls < 3 {
		t.Fatalf("expected at least 3 status calls (waited for controllers to catch up), got %d", statusCalls)
	}
}

func TestPollClusterReady_RejectsReadyWhenNoControllers(t *testing.T) {
	withFastClusterPollTimers(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/status"):
			if err := json.NewEncoder(w).Encode(map[string]any{
				"controller_status": []any{},
				"status": map[string]any{
					"phase": "Ready",
				},
			}); err != nil {
				t.Errorf("encode response: %v", err)
			}
		default:
			if err := json.NewEncoder(w).Encode(map[string]any{
				"generation": float64(1),
				"status": map[string]any{
					"phase":              "Ready",
					"reason":             "AllControllersReady",
					"observedGeneration": float64(1),
				},
			}); err != nil {
				t.Errorf("encode response: %v", err)
			}
		}
	}))
	defer server.Close()

	client := NewCLSClient(server.URL, "token", "email@example.com", nil)
	err := PollClusterReady(context.Background(), client, "c-123", noopProgress())
	if err == nil {
		t.Fatal("expected timeout error when no controllers reported")
	}
	if !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAllControllersAtGeneration(t *testing.T) {
	tests := []struct {
		name string
		data map[string]any
		gen  float64
		want bool
	}{
		{
			name: "no controller_status field",
			data: map[string]any{"status": map[string]any{"phase": "Ready"}},
			gen:  1,
			want: false,
		},
		{
			name: "empty controller list",
			data: map[string]any{"controller_status": []any{}},
			gen:  1,
			want: false,
		},
		{
			name: "all controllers at generation",
			data: map[string]any{"controller_status": []any{
				map[string]any{"controller_name": "a", "observed_generation": float64(2)},
				map[string]any{"controller_name": "b", "observed_generation": float64(2)},
			}},
			gen:  2,
			want: true,
		},
		{
			name: "one controller behind",
			data: map[string]any{"controller_status": []any{
				map[string]any{"controller_name": "a", "observed_generation": float64(2)},
				map[string]any{"controller_name": "b", "observed_generation": float64(1)},
			}},
			gen:  2,
			want: false,
		},
		{
			name: "controller missing observed_generation",
			data: map[string]any{"controller_status": []any{
				map[string]any{"controller_name": "a"},
			}},
			gen:  1,
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := allControllersAtGeneration(tt.data, tt.gen)
			if got != tt.want {
				t.Errorf("allControllersAtGeneration() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPollClusterReady_ProgressingThenReady(t *testing.T) {
	withFastClusterPollTimers(t)

	var clusterCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/clusters/c-123/status":
			if err := json.NewEncoder(w).Encode(map[string]any{
				"controller_status": []any{
					map[string]any{"controller_name": "cls-version-resolution-controller", "observed_generation": float64(1)},
					map[string]any{"controller_name": "cls-hypershift-client", "observed_generation": float64(1)},
				},
				"status": map[string]any{"phase": "Ready"},
			}); err != nil {
				t.Errorf("encode response: %v", err)
			}

		case r.URL.Path == "/api/v1/clusters/c-123":
			clusterCalls++
			phase := "Progressing"
			if clusterCalls >= 3 {
				phase = "Ready"
			}
			if err := json.NewEncoder(w).Encode(map[string]any{
				"generation": float64(1),
				"status": map[string]any{
					"phase":              phase,
					"reason":             "AllControllersReady",
					"observedGeneration": float64(1),
				},
			}); err != nil {
				t.Errorf("encode response: %v", err)
			}

		default:
			http.Error(w, "unexpected path", http.StatusNotFound)
		}
	}))
	defer server.Close()

	clusterPollTimeout = 500 * time.Millisecond

	client := NewCLSClient(server.URL, "token", "email@example.com", nil)
	err := PollClusterReady(context.Background(), client, "c-123", noopProgress())
	if err != nil {
		t.Fatalf("PollClusterReady() error = %v", err)
	}
	if clusterCalls < 3 {
		t.Fatalf("expected at least 3 cluster polls before Ready, got %d", clusterCalls)
	}
}

func TestPollClusterReady_FailedReturnsImmediately(t *testing.T) {
	var statusCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/status"):
			statusCalls++
			http.Error(w, "should not be called", http.StatusInternalServerError)

		default:
			if err := json.NewEncoder(w).Encode(map[string]any{
				"generation": float64(1),
				"status": map[string]any{
					"phase":   "Failed",
					"message": "quota exceeded",
				},
			}); err != nil {
				t.Errorf("encode response: %v", err)
			}
		}
	}))
	defer server.Close()

	client := NewCLSClient(server.URL, "token", "email@example.com", nil)
	err := PollClusterReady(context.Background(), client, "c-123", noopProgress())
	if err == nil {
		t.Fatal("expected error for Failed phase")
	}
	if !strings.Contains(err.Error(), "quota exceeded") {
		t.Fatalf("unexpected error: %v", err)
	}
	if statusCalls != 0 {
		t.Fatalf("expected 0 /status calls for Failed phase, got %d", statusCalls)
	}
}

func TestPollClusterReady_StatusEndpointError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/status"):
			http.Error(w, "internal error", http.StatusInternalServerError)

		default:
			if err := json.NewEncoder(w).Encode(map[string]any{
				"generation": float64(1),
				"status": map[string]any{
					"phase":              "Ready",
					"reason":             "AllControllersReady",
					"observedGeneration": float64(1),
				},
			}); err != nil {
				t.Errorf("encode response: %v", err)
			}
		}
	}))
	defer server.Close()

	client := NewCLSClient(server.URL, "token", "email@example.com", nil)
	err := PollClusterReady(context.Background(), client, "c-123", noopProgress())
	if err == nil {
		t.Fatal("expected error when /status endpoint fails")
	}
	if !strings.Contains(err.Error(), "get cluster status") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEmitClusterReadyTransition_EmitsProgressEvent(t *testing.T) {
	obs := &testEventRecorder{}

	emitClusterReadyTransition(context.Background(), newTestProgress(obs))

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
	withFastClusterPollTimers(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/clusters/c-123" {
			t.Errorf("unexpected path %q", r.URL.Path)
			http.Error(w, "unexpected path", http.StatusInternalServerError)
			return
		}
		http.Error(w, "backend unavailable", http.StatusBadGateway)
	}))
	defer server.Close()

	client := NewCLSClient(server.URL, "token", "email@example.com", nil)
	err := PollClusterDeleted(context.Background(), client, "c-123", noopProgress())
	if err == nil {
		t.Fatal("expected get cluster error")
	}
	if !strings.Contains(err.Error(), "get cluster") || !strings.Contains(err.Error(), "HTTP 502") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPollClusterDeleted_SucceedsOnHTTP404(t *testing.T) {
	withFastClusterPollTimers(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/clusters/c-123" {
			t.Errorf("unexpected path %q", r.URL.Path)
			http.Error(w, "unexpected path", http.StatusInternalServerError)
			return
		}
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer server.Close()

	client := NewCLSClient(server.URL, "token", "email@example.com", nil)
	if err := PollClusterDeleted(context.Background(), client, "c-123", noopProgress()); err != nil {
		t.Fatalf("expected HTTP 404 to count as deleted, got %v", err)
	}
}

func TestPollClusterDeleted_DoesNotTreat404TextInBodyAsDeletion(t *testing.T) {
	withFastClusterPollTimers(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/clusters/c-123" {
			t.Errorf("unexpected path %q", r.URL.Path)
			http.Error(w, "unexpected path", http.StatusInternalServerError)
			return
		}
		http.Error(w, "upstream lookup mentioned stale 404 cache entry", http.StatusBadGateway)
	}))
	defer server.Close()

	client := NewCLSClient(server.URL, "token", "email@example.com", nil)
	err := PollClusterDeleted(context.Background(), client, "c-123", noopProgress())
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
				t.Errorf("encode cluster response: %v", err)
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
				t.Errorf("encode cluster status response: %v", err)
			}
		case "/api/v1/nodepools":
			if got := r.URL.Query().Get("clusterId"); got != "c-123" {
				t.Errorf("clusterId = %q, want c-123", got)
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
				t.Errorf("encode nodepool list response: %v", err)
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
				t.Errorf("encode nodepool status response: %v", err)
			}
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
			http.Error(w, "unexpected path", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	obs := &testEventRecorder{}
	client := NewCLSClient(server.URL, "token", "email@example.com", nil)

	if err := emitFailureStatusSnapshot(
		context.Background(),
		client,
		"c-123",
		"test-cluster",
		newTestProgress(obs),
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
				t.Errorf("encode cluster response: %v", err)
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
				t.Errorf("encode cluster status response: %v", err)
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
				t.Errorf("encode nodepool list response: %v", err)
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
				t.Errorf("encode nodepool status response: %v", err)
			}
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
			http.Error(w, "unexpected path", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	obs := &testEventRecorder{}
	client := NewCLSClient(server.URL, "token", "email@example.com", nil)

	if err := emitFailureStatusSnapshot(
		context.Background(),
		client,
		"c-123",
		"test-cluster",
		newTestProgress(obs),
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

func TestExtractProblemConditions_EmptyData(t *testing.T) {
	result := extractProblemConditions(map[string]any{})
	if result != nil {
		t.Fatalf("expected nil for empty data, got %v", result)
	}
}

func TestExtractProblemConditions_NoControllerStatusFallsBackToStatusConditions(t *testing.T) {
	data := map[string]any{
		"status": map[string]any{
			"conditions": []any{
				map[string]any{
					"type":    "Degraded",
					"status":  "True",
					"reason":  "QuotaExceeded",
					"message": "quota limit reached",
				},
			},
		},
	}

	result := extractProblemConditions(data)
	if len(result) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(result))
	}
	if result[0].Type != "Degraded" {
		t.Fatalf("type = %q, want Degraded", result[0].Type)
	}
	if result[0].Controller != "" {
		t.Fatalf("controller = %q, want empty for status-level fallback", result[0].Controller)
	}
}

func TestExtractProblemConditions_SkipsNonMapControllers(t *testing.T) {
	data := map[string]any{
		"controller_status": []any{
			"not-a-map",
			map[string]any{
				"controller_name": "valid-controller",
				"conditions": []any{
					map[string]any{
						"type":   "Failed",
						"status": "True",
						"reason": "Error",
					},
				},
			},
		},
	}

	result := extractProblemConditions(data)
	if len(result) != 1 {
		t.Fatalf("expected 1 condition (skipping non-map), got %d", len(result))
	}
	if result[0].Controller != "valid-controller" {
		t.Fatalf("controller = %q, want valid-controller", result[0].Controller)
	}
}

func TestExtractProblemConditions_SkipsNonMapConditions(t *testing.T) {
	data := map[string]any{
		"controller_status": []any{
			map[string]any{
				"controller_name": "test-controller",
				"conditions": []any{
					"not-a-map",
					map[string]any{
						"type":   "Degraded",
						"status": "True",
						"reason": "Error",
					},
				},
			},
		},
	}

	result := extractProblemConditions(data)
	if len(result) != 1 {
		t.Fatalf("expected 1 condition (skipping non-map), got %d", len(result))
	}
}

func TestExtractProblemConditions_FiltersHealthyConditions(t *testing.T) {
	data := map[string]any{
		"controller_status": []any{
			map[string]any{
				"controller_name": "test-controller",
				"conditions": []any{
					map[string]any{
						"type":   "Available",
						"status": "True",
						"reason": "AsExpected",
					},
					map[string]any{
						"type":   "Degraded",
						"status": "False",
						"reason": "AsExpected",
					},
				},
			},
		},
	}

	result := extractProblemConditions(data)
	if len(result) != 1 {
		t.Fatalf("expected 1 problem condition (Degraded=False), got %d", len(result))
	}
	if result[0].Type != "Degraded" {
		t.Fatalf("type = %q, want Degraded", result[0].Type)
	}
}

func TestExtractProblemConditions_MissingConditionFields(t *testing.T) {
	data := map[string]any{
		"controller_status": []any{
			map[string]any{
				"conditions": []any{
					map[string]any{
						"status": "Unknown",
					},
				},
			},
		},
	}

	result := extractProblemConditions(data)
	if len(result) != 1 {
		t.Fatalf("expected 1 condition for Unknown status, got %d", len(result))
	}
	if result[0].Type != "" {
		t.Fatalf("type = %q, want empty", result[0].Type)
	}
	if result[0].Controller != "" {
		t.Fatalf("controller = %q, want empty", result[0].Controller)
	}
}

func TestExtractProblemConditions_ControllerStatusTakesPrecedence(t *testing.T) {
	data := map[string]any{
		"controller_status": []any{
			map[string]any{
				"controller_name": "test-controller",
				"conditions": []any{
					map[string]any{
						"type":   "Progressing",
						"status": "True",
						"reason": "Deploying",
					},
				},
			},
		},
		"status": map[string]any{
			"conditions": []any{
				map[string]any{
					"type":    "Failed",
					"status":  "True",
					"reason":  "ShouldNotAppear",
					"message": "this should not be included",
				},
			},
		},
	}

	result := extractProblemConditions(data)
	if len(result) != 1 {
		t.Fatalf("expected 1 condition from controller_status, got %d", len(result))
	}
	if result[0].Type != "Progressing" {
		t.Fatalf("type = %q, want Progressing (from controller_status, not status fallback)", result[0].Type)
	}
}
