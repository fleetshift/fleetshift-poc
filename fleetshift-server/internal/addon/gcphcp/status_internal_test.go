package gcphcp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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
	client := &fakeNodepoolStatusClient{
		listSeq: [][]map[string]any{{
			{"id": "np-1", "name": "worker-a"},
			{"id": "np-2", "name": "worker-b"},
		}},
		statusByID: map[string][]map[string]any{
			"np-1": {{
				"status": map[string]any{"phase": "Ready"},
			}},
			"np-2": {{
				"status": map[string]any{"phase": "Ready"},
			}},
		},
	}

	err := PollDesiredNodepoolsHealthy(context.Background(), client, "cluster-123", []NodepoolSpec{
		{Name: "worker-a"},
		{Name: "worker-b"},
	}, &domain.DeliverySignaler{})
	if err != nil {
		t.Fatalf("PollDesiredNodepoolsHealthy() error = %v", err)
	}
}

func TestPollDesiredNodepoolsHealthy_FailedNodepool(t *testing.T) {
	client := &fakeNodepoolStatusClient{
		listSeq: [][]map[string]any{{
			{"id": "np-1", "name": "worker-a"},
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

	err := PollDesiredNodepoolsHealthy(context.Background(), client, "cluster-123", []NodepoolSpec{
		{Name: "worker-a"},
	}, &domain.DeliverySignaler{})
	if err == nil {
		t.Fatal("expected failed nodepool error")
	}
	if !strings.Contains(err.Error(), "worker-a") || !strings.Contains(err.Error(), "quota exceeded") {
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
			{{"id": "np-1", "name": "worker-a"}},
		},
		statusByID: map[string][]map[string]any{
			"np-1": {
				{"status": map[string]any{"phase": "Progressing"}},
				{"status": map[string]any{"phase": "Ready"}},
			},
		},
	}

	err := PollDesiredNodepoolsHealthy(context.Background(), client, "cluster-123", []NodepoolSpec{
		{Name: "worker-a"},
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
				"phase": "Progressing",
			},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	client := NewCLSClient(server.URL, "token", "email@example.com", nil)
	err := PollClusterReady(context.Background(), client, "c-123", &domain.DeliverySignaler{})
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !strings.Contains(err.Error(), "timeout waiting for cluster to become ready") {
		t.Fatalf("unexpected error: %v", err)
	}
}
