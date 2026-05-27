package gcphcp_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/gcphcp"
)

// newTestCLSClient creates an httptest.Server with the given handler and returns
// a CLSClient pointed at it. The caller must defer server.Close().
func newTestCLSClient(t *testing.T, handler http.HandlerFunc) (*gcphcp.CLSClient, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	return gcphcp.NewCLSClient(server.URL, "token", "email@example.com", nil), server
}

func TestCLSClient_CreateCluster(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/api/v1/clusters" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer broker-token" {
			t.Errorf("unexpected auth header %q", r.Header.Get("Authorization"))
		}
		if r.Header.Get("X-User-Email") != "broker@example.com" {
			t.Errorf("unexpected email header %q", r.Header.Get("X-User-Email"))
		}

		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["name"] != "test-cluster" {
			t.Errorf("unexpected cluster name %v", body["name"])
		}

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"id": "cluster-123", "name": "test-cluster"})
	}))
	defer server.Close()

	client := gcphcp.NewCLSClient(server.URL, "broker-token", "broker@example.com", nil)
	result, err := client.CreateCluster(context.Background(), map[string]any{"name": "test-cluster"})
	if err != nil {
		t.Fatalf("CreateCluster failed: %v", err)
	}
	if result["id"] != "cluster-123" {
		t.Errorf("cluster id = %v", result["id"])
	}
}

func TestCLSClient_GetCluster(t *testing.T) {
	client, server := newTestCLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		if r.URL.Path != "/api/v1/clusters/c-123" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		json.NewEncoder(w).Encode(map[string]any{
			"id":   "c-123",
			"name": "my-cluster",
			"status": map[string]any{
				"phase": "Ready",
			},
		})
	})
	defer server.Close()

	result, err := client.GetCluster(context.Background(), "c-123")
	if err != nil {
		t.Fatalf("GetCluster failed: %v", err)
	}
	status := result["status"].(map[string]any)
	if status["phase"] != "Ready" {
		t.Errorf("phase = %v", status["phase"])
	}
}

func TestCLSClient_UpdateCluster(t *testing.T) {
	client, server := newTestCLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("expected PUT, got %s", r.Method)
		}
		if r.URL.Path != "/api/v1/clusters/c-123" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["name"] != "test-cluster" {
			t.Errorf("unexpected cluster name %v", body["name"])
		}

		json.NewEncoder(w).Encode(map[string]string{"id": "c-123", "name": "test-cluster"})
	})
	defer server.Close()

	result, err := client.UpdateCluster(context.Background(), "c-123", map[string]any{"name": "test-cluster"})
	if err != nil {
		t.Fatalf("UpdateCluster failed: %v", err)
	}
	if result["id"] != "c-123" {
		t.Errorf("cluster id = %v", result["id"])
	}
}

func TestCLSClient_DeleteCluster(t *testing.T) {
	client, server := newTestCLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		if r.URL.Path != "/api/v1/clusters/c-123" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		if r.URL.Query().Get("force") != "true" {
			t.Error("expected force=true query param")
		}
		w.WriteHeader(http.StatusOK)
	})
	defer server.Close()

	err := client.DeleteCluster(context.Background(), "c-123")
	if err != nil {
		t.Fatalf("DeleteCluster failed: %v", err)
	}
}

func TestCLSClient_CreateNodepool(t *testing.T) {
	client, server := newTestCLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/nodepools" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"id": "np-1"})
	})
	defer server.Close()

	result, err := client.CreateNodepool(context.Background(), map[string]any{"name": "np"})
	if err != nil {
		t.Fatalf("CreateNodepool failed: %v", err)
	}
	if result["id"] != "np-1" {
		t.Errorf("nodepool id = %v", result["id"])
	}
}

func TestCLSClient_UpdateNodepool(t *testing.T) {
	client, server := newTestCLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("expected PUT, got %s", r.Method)
		}
		if r.URL.Path != "/api/v1/nodepools/np-123" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["name"] != "worker-a" {
			t.Errorf("unexpected nodepool name %v", body["name"])
		}

		json.NewEncoder(w).Encode(map[string]string{"id": "np-123"})
	})
	defer server.Close()

	result, err := client.UpdateNodepool(context.Background(), "np-123", map[string]any{"name": "worker-a"})
	if err != nil {
		t.Fatalf("UpdateNodepool failed: %v", err)
	}
	if result["id"] != "np-123" {
		t.Errorf("nodepool id = %v", result["id"])
	}
}

func TestCLSClient_DeleteNodepool(t *testing.T) {
	client, server := newTestCLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		if r.URL.Path != "/api/v1/nodepools/np-123" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	})
	defer server.Close()

	if err := client.DeleteNodepool(context.Background(), "np-123"); err != nil {
		t.Fatalf("DeleteNodepool failed: %v", err)
	}
}

func TestCLSClient_GetClusterStatus(t *testing.T) {
	client, server := newTestCLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/clusters/c-123/status" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		json.NewEncoder(w).Encode(map[string]any{
			"phase": "Ready",
			"controller_status": []any{
				map[string]any{
					"name": "cls-hypershift-client",
					"conditions": []any{
						map[string]any{
							"type":    "APIServer",
							"message": "https://guest-api.example.com:6443",
						},
					},
				},
			},
		})
	})
	defer server.Close()

	result, err := client.GetClusterStatus(context.Background(), "c-123")
	if err != nil {
		t.Fatalf("GetClusterStatus failed: %v", err)
	}
	if result["phase"] != "Ready" {
		t.Errorf("phase = %v", result["phase"])
	}
}

func TestCLSClient_GetNodepoolStatus(t *testing.T) {
	client, server := newTestCLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/nodepools/np-123/status" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		json.NewEncoder(w).Encode(map[string]any{
			"nodepool_id": "np-123",
			"status": map[string]any{
				"phase":   "Ready",
				"message": "all replicas available",
			},
		})
	})
	defer server.Close()

	result, err := client.GetNodepoolStatus(context.Background(), "np-123")
	if err != nil {
		t.Fatalf("GetNodepoolStatus failed: %v", err)
	}
	status := result["status"].(map[string]any)
	if status["phase"] != "Ready" {
		t.Errorf("phase = %v", status["phase"])
	}
}

func TestCLSClient_ListClusters(t *testing.T) {
	client, server := newTestCLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"clusters": []map[string]any{
				{"id": "c-1", "name": "cluster-a"},
			},
		})
	})
	defer server.Close()

	clusters, err := client.ListClusters(context.Background())
	if err != nil {
		t.Fatalf("ListClusters failed: %v", err)
	}
	if len(clusters) != 1 {
		t.Fatalf("count = %d", len(clusters))
	}
	if clusters[0]["name"] != "cluster-a" {
		t.Errorf("name = %v", clusters[0]["name"])
	}
}

func TestCLSClient_ListClusters_RejectsMissingClustersField(t *testing.T) {
	client, server := newTestCLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{
				{"id": "c-1", "name": "cluster-a"},
			},
		})
	})
	defer server.Close()

	_, err := client.ListClusters(context.Background())
	if err == nil {
		t.Fatal("expected missing clusters field error")
	}
	if !strings.Contains(err.Error(), "clusters") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCLSClient_ListNodepools(t *testing.T) {
	client, server := newTestCLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		if r.URL.Path != "/api/v1/nodepools" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		if r.URL.Query().Get("clusterId") != "c-123" {
			t.Errorf("unexpected clusterId query %q", r.URL.Query().Get("clusterId"))
		}
		json.NewEncoder(w).Encode(map[string]any{
			"nodepools": []map[string]any{
				{"id": "np-1", "name": "worker-a"},
			},
		})
	})
	defer server.Close()

	nodepools, err := client.ListNodepools(context.Background(), "c-123")
	if err != nil {
		t.Fatalf("ListNodepools failed: %v", err)
	}
	if len(nodepools) != 1 {
		t.Fatalf("count = %d", len(nodepools))
	}
	if nodepools[0]["name"] != "worker-a" {
		t.Errorf("name = %v", nodepools[0]["name"])
	}
}

func TestCLSClient_ListNodepools_TreatsNullAsEmptyList(t *testing.T) {
	client, server := newTestCLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("clusterId") != "c-123" {
			t.Errorf("unexpected clusterId query %q", r.URL.Query().Get("clusterId"))
		}
		json.NewEncoder(w).Encode(map[string]any{
			"nodepools": nil,
		})
	})
	defer server.Close()

	nodepools, err := client.ListNodepools(context.Background(), "c-123")
	if err != nil {
		t.Fatalf("ListNodepools failed: %v", err)
	}
	if len(nodepools) != 0 {
		t.Fatalf("count = %d, want 0", len(nodepools))
	}
}

func TestCLSClient_ListNodepools_RejectsMissingNodepoolsField(t *testing.T) {
	client, server := newTestCLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("clusterId") != "c-123" {
			t.Errorf("unexpected clusterId query %q", r.URL.Query().Get("clusterId"))
		}
		json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{
				{"id": "np-1", "name": "worker-a"},
			},
		})
	})
	defer server.Close()

	_, err := client.ListNodepools(context.Background(), "c-123")
	if err == nil {
		t.Fatal("expected missing nodepools field error")
	}
	if !strings.Contains(err.Error(), "nodepools") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCLSClient_AuthExpiredErrorClassification(t *testing.T) {
	tests := []struct {
		name           string
		statusCode     int
		body           string
		wantAuthExpired bool
	}{
		{"HTTP401_ReturnsAuthExpiredError", http.StatusUnauthorized, `{"error":"token expired"}`, true},
		{"HTTP403_DoesNotReturnAuthExpiredError", http.StatusForbidden, `{"error":"access denied"}`, false},
		{"HTTP500_DoesNotReturnAuthExpiredError", http.StatusInternalServerError, `internal error`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, server := newTestCLSClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)
				w.Write([]byte(tt.body))
			})
			defer server.Close()

			_, err := client.GetCluster(context.Background(), "c-123")
			if err == nil {
				t.Fatalf("expected error on %d", tt.statusCode)
			}
			if got := gcphcp.IsAuthExpiredError(err); got != tt.wantAuthExpired {
				t.Fatalf("IsAuthExpiredError() = %v, want %v (err: %v)", got, tt.wantAuthExpired, err)
			}
			var httpErr *gcphcp.CLSHTTPError
			if !errors.As(err, &httpErr) {
				t.Fatal("expected clsHTTPError to be unwrappable")
			}
			if httpErr.StatusCode != tt.statusCode {
				t.Fatalf("status code = %d, want %d", httpErr.StatusCode, tt.statusCode)
			}
		})
	}
}

func TestCLSClient_ResolveClusterID(t *testing.T) {
	client, server := newTestCLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"clusters": []map[string]any{
				{"id": "c-1", "name": "cluster-a"},
			},
		})
	})
	defer server.Close()

	clusterID, err := client.ResolveClusterID(context.Background(), "cluster-a")
	if err != nil {
		t.Fatalf("ResolveClusterID failed: %v", err)
	}
	if clusterID != "c-1" {
		t.Errorf("clusterID = %q", clusterID)
	}
}

func TestCLSClient_ResolveClusterID_NotFound(t *testing.T) {
	client, server := newTestCLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"clusters": []map[string]any{
				{"id": "c-1", "name": "cluster-a"},
			},
		})
	})
	defer server.Close()

	_, err := client.ResolveClusterID(context.Background(), "cluster-b")
	if err == nil {
		t.Fatal("expected not found error")
	}
	if !errors.Is(err, gcphcp.ErrClusterNotFound) {
		t.Fatalf("expected ErrClusterNotFound, got %v", err)
	}
}

func TestCLSClient_ResolveClusterID_DoesNotTreatMalformedListAsNotFound(t *testing.T) {
	client, server := newTestCLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{
				{"id": "c-1", "name": "cluster-a"},
			},
		})
	})
	defer server.Close()

	_, err := client.ResolveClusterID(context.Background(), "cluster-a")
	if err == nil {
		t.Fatal("expected malformed list error")
	}
	if errors.Is(err, gcphcp.ErrClusterNotFound) {
		t.Fatalf("expected protocol error, got ErrClusterNotFound: %v", err)
	}
	if !strings.Contains(err.Error(), "clusters") {
		t.Fatalf("unexpected error: %v", err)
	}
}
