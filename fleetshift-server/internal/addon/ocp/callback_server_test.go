package ocp

import (
	"context"
	"sync"
	"testing"

	fleetshiftv1 "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
)

func TestCallbackServer_ReportCompletion(t *testing.T) {
	provisions := &sync.Map{}
	state := &provisionState{
		done: make(chan struct{}),
	}
	clusterID := "test-cluster-123"
	provisions.Store(clusterID, state)

	server := &callbackServer{provisions: provisions}

	req := &fleetshiftv1.OCPCompletionRequest{
		ClusterId:   clusterID,
		InfraId:     "infra-123",
		ClusterUuid: "uuid-456",
		ApiServer:   "https://api.test.example.com:6443",
		Kubeconfig:  []byte("fake-kubeconfig"),
	}

	ack, err := server.ReportCompletion(context.Background(), req)
	if err != nil {
		t.Fatalf("ReportCompletion failed: %v", err)
	}
	if ack == nil {
		t.Fatal("expected non-nil ACK")
	}

	// Verify completion was stored
	if state.completion == nil {
		t.Error("expected completion to be set")
	}
	if state.completion.ClusterId != clusterID {
		t.Errorf("expected cluster ID %s, got %s", clusterID, state.completion.ClusterId)
	}

	// Verify done channel was closed
	select {
	case <-state.done:
		// Success: channel is closed
	default:
		t.Error("expected done channel to be closed")
	}
}

func TestCallbackServer_ReportFailure(t *testing.T) {
	provisions := &sync.Map{}
	state := &provisionState{
		done: make(chan struct{}),
	}
	clusterID := "test-cluster-456"
	provisions.Store(clusterID, state)

	server := &callbackServer{provisions: provisions}

	req := &fleetshiftv1.OCPFailureRequest{
		ClusterId:      clusterID,
		Phase:          "bootstrap",
		FailureReason:  "timeout",
		FailureMessage: "Bootstrap timed out after 60 minutes",
		LogTail:        "last 100 lines...",
	}

	ack, err := server.ReportFailure(context.Background(), req)
	if err != nil {
		t.Fatalf("ReportFailure failed: %v", err)
	}
	if ack == nil {
		t.Fatal("expected non-nil ACK")
	}

	// Verify failure was stored
	if state.failure == nil {
		t.Error("expected failure to be set")
	}
	if state.failure.ClusterId != clusterID {
		t.Errorf("expected cluster ID %s, got %s", clusterID, state.failure.ClusterId)
	}
	if state.failure.FailureReason != "timeout" {
		t.Errorf("expected failure reason 'timeout', got %s", state.failure.FailureReason)
	}

	// Verify done channel was closed
	select {
	case <-state.done:
		// Success: channel is closed
	default:
		t.Error("expected done channel to be closed")
	}
}

func TestCallbackServer_UnknownCluster(t *testing.T) {
	provisions := &sync.Map{}
	server := &callbackServer{provisions: provisions}

	t.Run("ReportPhaseResult", func(t *testing.T) {
		req := &fleetshiftv1.OCPPhaseResultRequest{
			ClusterId: "unknown-cluster",
			Phase:     "bootstrap",
			Status:    "running",
		}
		_, err := server.ReportPhaseResult(context.Background(), req)
		if err == nil {
			t.Error("expected error for unknown cluster, got nil")
		}
	})

	t.Run("ReportMilestone", func(t *testing.T) {
		req := &fleetshiftv1.OCPMilestoneRequest{
			ClusterId: "unknown-cluster",
			Event:     "api_server_available",
		}
		_, err := server.ReportMilestone(context.Background(), req)
		if err == nil {
			t.Error("expected error for unknown cluster, got nil")
		}
	})

	t.Run("ReportCompletion", func(t *testing.T) {
		req := &fleetshiftv1.OCPCompletionRequest{
			ClusterId: "unknown-cluster",
		}
		_, err := server.ReportCompletion(context.Background(), req)
		if err == nil {
			t.Error("expected error for unknown cluster, got nil")
		}
	})

	t.Run("ReportFailure", func(t *testing.T) {
		req := &fleetshiftv1.OCPFailureRequest{
			ClusterId: "unknown-cluster",
		}
		_, err := server.ReportFailure(context.Background(), req)
		if err == nil {
			t.Error("expected error for unknown cluster, got nil")
		}
	})
}

func TestCallbackServer_ReportPhaseResult(t *testing.T) {
	provisions := &sync.Map{}
	state := &provisionState{
		done: make(chan struct{}),
	}
	clusterID := "test-cluster-789"
	provisions.Store(clusterID, state)

	server := &callbackServer{provisions: provisions}

	req := &fleetshiftv1.OCPPhaseResultRequest{
		ClusterId:      clusterID,
		Phase:          "infrastructure",
		Status:         "completed",
		ElapsedSeconds: 180,
		Attempt:        1,
	}

	ack, err := server.ReportPhaseResult(context.Background(), req)
	if err != nil {
		t.Fatalf("ReportPhaseResult failed: %v", err)
	}
	if ack == nil {
		t.Fatal("expected non-nil ACK")
	}

	// Verify state is unchanged (observability only)
	if state.completion != nil {
		t.Error("expected completion to remain nil")
	}
	if state.failure != nil {
		t.Error("expected failure to remain nil")
	}

	// Verify done channel is still open
	select {
	case <-state.done:
		t.Error("expected done channel to remain open")
	default:
		// Success: channel is still open
	}
}

func TestCallbackServer_ReportMilestone(t *testing.T) {
	provisions := &sync.Map{}
	state := &provisionState{
		done: make(chan struct{}),
	}
	clusterID := "test-cluster-101"
	provisions.Store(clusterID, state)

	server := &callbackServer{provisions: provisions}

	req := &fleetshiftv1.OCPMilestoneRequest{
		ClusterId:      clusterID,
		Event:          "control_plane_ready",
		ElapsedSeconds: 300,
		Attempt:        1,
	}

	ack, err := server.ReportMilestone(context.Background(), req)
	if err != nil {
		t.Fatalf("ReportMilestone failed: %v", err)
	}
	if ack == nil {
		t.Fatal("expected non-nil ACK")
	}

	// Verify state is unchanged (observability only)
	if state.completion != nil {
		t.Error("expected completion to remain nil")
	}
	if state.failure != nil {
		t.Error("expected failure to remain nil")
	}

	// Verify done channel is still open
	select {
	case <-state.done:
		t.Error("expected done channel to remain open")
	default:
		// Success: channel is still open
	}
}
