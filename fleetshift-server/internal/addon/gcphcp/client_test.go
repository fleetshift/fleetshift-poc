package gcphcp_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/gcphcp"
)

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
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	}))
	defer server.Close()

	client := gcphcp.NewCLSClient(server.URL, "token", "email@example.com", nil)
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
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	}))
	defer server.Close()

	client := gcphcp.NewCLSClient(server.URL, "token", "email@example.com", nil)
	result, err := client.UpdateCluster(context.Background(), "c-123", map[string]any{"name": "test-cluster"})
	if err != nil {
		t.Fatalf("UpdateCluster failed: %v", err)
	}
	if result["id"] != "c-123" {
		t.Errorf("cluster id = %v", result["id"])
	}
}

func TestCLSClient_DeleteCluster(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	}))
	defer server.Close()

	client := gcphcp.NewCLSClient(server.URL, "token", "email@example.com", nil)
	err := client.DeleteCluster(context.Background(), "c-123")
	if err != nil {
		t.Fatalf("DeleteCluster failed: %v", err)
	}
}

func TestCLSClient_CreateNodepool(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/nodepools" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"id": "np-1"})
	}))
	defer server.Close()

	client := gcphcp.NewCLSClient(server.URL, "token", "email@example.com", nil)
	result, err := client.CreateNodepool(context.Background(), map[string]any{"name": "np"})
	if err != nil {
		t.Fatalf("CreateNodepool failed: %v", err)
	}
	if result["id"] != "np-1" {
		t.Errorf("nodepool id = %v", result["id"])
	}
}

func TestCLSClient_UpdateNodepool(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	}))
	defer server.Close()

	client := gcphcp.NewCLSClient(server.URL, "token", "email@example.com", nil)
	result, err := client.UpdateNodepool(context.Background(), "np-123", map[string]any{"name": "worker-a"})
	if err != nil {
		t.Fatalf("UpdateNodepool failed: %v", err)
	}
	if result["id"] != "np-123" {
		t.Errorf("nodepool id = %v", result["id"])
	}
}

func TestCLSClient_DeleteNodepool(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		if r.URL.Path != "/api/v1/nodepools/np-123" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := gcphcp.NewCLSClient(server.URL, "token", "email@example.com", nil)
	if err := client.DeleteNodepool(context.Background(), "np-123"); err != nil {
		t.Fatalf("DeleteNodepool failed: %v", err)
	}
}

func TestCLSClient_GetClusterStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	}))
	defer server.Close()

	client := gcphcp.NewCLSClient(server.URL, "token", "email@example.com", nil)
	result, err := client.GetClusterStatus(context.Background(), "c-123")
	if err != nil {
		t.Fatalf("GetClusterStatus failed: %v", err)
	}
	if result["phase"] != "Ready" {
		t.Errorf("phase = %v", result["phase"])
	}
}

func TestCLSClient_ListClusters(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"clusters": []map[string]any{
				{"id": "c-1", "name": "cluster-a"},
			},
		})
	}))
	defer server.Close()

	client := gcphcp.NewCLSClient(server.URL, "token", "email@example.com", nil)
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

func TestCLSClient_ListNodepools(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	}))
	defer server.Close()

	client := gcphcp.NewCLSClient(server.URL, "token", "email@example.com", nil)
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

func TestCLSClient_ResolveClusterID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"clusters": []map[string]any{
				{"id": "c-1", "name": "cluster-a"},
			},
		})
	}))
	defer server.Close()

	client := gcphcp.NewCLSClient(server.URL, "token", "email@example.com", nil)
	clusterID, err := client.ResolveClusterID(context.Background(), "cluster-a")
	if err != nil {
		t.Fatalf("ResolveClusterID failed: %v", err)
	}
	if clusterID != "c-1" {
		t.Errorf("clusterID = %q", clusterID)
	}
}

func TestCLSClient_ResolveClusterID_NotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"clusters": []map[string]any{
				{"id": "c-1", "name": "cluster-a"},
			},
		})
	}))
	defer server.Close()

	client := gcphcp.NewCLSClient(server.URL, "token", "email@example.com", nil)
	_, err := client.ResolveClusterID(context.Background(), "cluster-b")
	if err == nil {
		t.Fatal("expected not found error")
	}
	if !errors.Is(err, gcphcp.ErrClusterNotFound) {
		t.Fatalf("expected ErrClusterNotFound, got %v", err)
	}
}
