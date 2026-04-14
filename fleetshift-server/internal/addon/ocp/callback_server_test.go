package ocp

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	ocpv1 "github.com/fleetshift/fleetshift-poc/gen/ocp/v1"
)

func newTestCallbackServer(t *testing.T) (*callbackServer, *CallbackTokenSigner, *sync.Map) {
	t.Helper()
	signer, err := NewCallbackTokenSigner()
	if err != nil {
		t.Fatalf("NewCallbackTokenSigner: %v", err)
	}
	provisions := &sync.Map{}
	server := &callbackServer{
		provisions:    provisions,
		tokenVerifier: signer,
	}
	return server, signer, provisions
}

func ctxWithToken(token string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
}

func TestCallbackServer_ReportCompletion(t *testing.T) {
	server, signer, provisions := newTestCallbackServer(t)
	clusterID := "test-cluster-123"
	state := &provisionState{done: make(chan struct{})}
	provisions.Store(clusterID, state)

	token, err := signer.Sign(clusterID, 2*time.Hour)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	req := &ocpv1.CompletionRequest{
		ClusterId: clusterID,
		InfraId:   "infra-123",
		ApiServer: "https://api.test.example.com:6443",
	}

	ack, err := server.ReportCompletion(ctxWithToken(token), req)
	if err != nil {
		t.Fatalf("ReportCompletion: %v", err)
	}
	if ack == nil {
		t.Fatal("expected non-nil ACK")
	}
	if state.completion == nil {
		t.Error("expected completion to be set")
	}

	select {
	case <-state.done:
	default:
		t.Error("expected done channel to be closed")
	}
}

func TestCallbackServer_ReportFailure(t *testing.T) {
	server, signer, provisions := newTestCallbackServer(t)
	clusterID := "test-cluster-456"
	state := &provisionState{done: make(chan struct{})}
	provisions.Store(clusterID, state)

	token, err := signer.Sign(clusterID, 2*time.Hour)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	req := &ocpv1.FailureRequest{
		ClusterId:      clusterID,
		Phase:          "bootstrap",
		FailureReason:  "timeout",
		FailureMessage: "Bootstrap timed out",
	}

	ack, err := server.ReportFailure(ctxWithToken(token), req)
	if err != nil {
		t.Fatalf("ReportFailure: %v", err)
	}
	if ack == nil {
		t.Fatal("expected non-nil ACK")
	}
	if state.failure == nil {
		t.Error("expected failure to be set")
	}

	select {
	case <-state.done:
	default:
		t.Error("expected done channel to be closed")
	}
}

func TestCallbackServer_ReportPhaseResult(t *testing.T) {
	server, signer, provisions := newTestCallbackServer(t)
	clusterID := "test-cluster-789"
	state := &provisionState{done: make(chan struct{})}
	provisions.Store(clusterID, state)

	token, err := signer.Sign(clusterID, 2*time.Hour)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	req := &ocpv1.PhaseResultRequest{
		ClusterId: clusterID,
		Phase:     "infrastructure",
		Status:    "completed",
	}

	ack, err := server.ReportPhaseResult(ctxWithToken(token), req)
	if err != nil {
		t.Fatalf("ReportPhaseResult: %v", err)
	}
	if ack == nil {
		t.Fatal("expected non-nil ACK")
	}

	if state.completion != nil || state.failure != nil {
		t.Error("expected state to remain unchanged")
	}
}

func TestCallbackServer_ReportMilestone(t *testing.T) {
	server, signer, provisions := newTestCallbackServer(t)
	clusterID := "test-cluster-101"
	state := &provisionState{done: make(chan struct{})}
	provisions.Store(clusterID, state)

	token, err := signer.Sign(clusterID, 2*time.Hour)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	req := &ocpv1.MilestoneRequest{
		ClusterId: clusterID,
		Event:     "control_plane_ready",
	}

	ack, err := server.ReportMilestone(ctxWithToken(token), req)
	if err != nil {
		t.Fatalf("ReportMilestone: %v", err)
	}
	if ack == nil {
		t.Fatal("expected non-nil ACK")
	}
}

func TestCallbackServer_MissingToken(t *testing.T) {
	server, _, provisions := newTestCallbackServer(t)
	clusterID := "test-cluster"
	provisions.Store(clusterID, &provisionState{done: make(chan struct{})})

	_, err := server.ReportCompletion(context.Background(), &ocpv1.CompletionRequest{
		ClusterId: clusterID,
	})
	if err == nil {
		t.Fatal("expected error for missing token")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unauthenticated {
		t.Errorf("code = %v, want Unauthenticated", status.Code(err))
	}
}

func TestCallbackServer_InvalidToken(t *testing.T) {
	server, _, provisions := newTestCallbackServer(t)
	clusterID := "test-cluster"
	provisions.Store(clusterID, &provisionState{done: make(chan struct{})})

	_, err := server.ReportCompletion(ctxWithToken("not-a-jwt"), &ocpv1.CompletionRequest{
		ClusterId: clusterID,
	})
	if err == nil {
		t.Fatal("expected error for invalid token")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unauthenticated {
		t.Errorf("code = %v, want Unauthenticated", status.Code(err))
	}
}

func TestCallbackServer_WrongClusterID(t *testing.T) {
	server, signer, provisions := newTestCallbackServer(t)
	provisions.Store("cluster-a", &provisionState{done: make(chan struct{})})
	provisions.Store("cluster-b", &provisionState{done: make(chan struct{})})

	token, _ := signer.Sign("cluster-a", 2*time.Hour)
	_, err := server.ReportCompletion(ctxWithToken(token), &ocpv1.CompletionRequest{
		ClusterId: "cluster-b",
	})
	if err == nil {
		t.Fatal("expected error for wrong cluster ID")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.PermissionDenied {
		t.Errorf("code = %v, want PermissionDenied", status.Code(err))
	}
}

func TestCallbackServer_ConcurrentCompletionAndFailure(t *testing.T) {
	server, signer, provisions := newTestCallbackServer(t)
	clusterID := "concurrent-test"
	state := &provisionState{done: make(chan struct{})}
	provisions.Store(clusterID, state)

	token, err := signer.Sign(clusterID, 2*time.Hour)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	ctx := ctxWithToken(token)

	// Fire completion and failure concurrently — only one should close the
	// channel (no panic from double-close), and the state must be consistent.
	done := make(chan struct{}, 2)
	go func() {
		server.ReportCompletion(ctx, &ocpv1.CompletionRequest{
			ClusterId: clusterID,
			InfraId:   "infra-concurrent",
		})
		done <- struct{}{}
	}()
	go func() {
		server.ReportFailure(ctx, &ocpv1.FailureRequest{
			ClusterId:      clusterID,
			Phase:          "bootstrap",
			FailureMessage: "race condition test",
		})
		done <- struct{}{}
	}()

	<-done
	<-done

	// The done channel must be closed exactly once (no panic)
	select {
	case <-state.done:
	default:
		t.Error("expected done channel to be closed")
	}

	// At least one of completion or failure must be set
	state.mu.Lock()
	hasCompletion := state.completion != nil
	hasFailure := state.failure != nil
	state.mu.Unlock()

	if !hasCompletion && !hasFailure {
		t.Error("expected at least one of completion or failure to be set")
	}
}

func TestCallbackServer_UnknownCluster(t *testing.T) {
	server, signer, _ := newTestCallbackServer(t)
	token, _ := signer.Sign("unknown-cluster", 2*time.Hour)

	_, err := server.ReportCompletion(ctxWithToken(token), &ocpv1.CompletionRequest{
		ClusterId: "unknown-cluster",
	})
	if err == nil {
		t.Fatal("expected error for unknown cluster")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.NotFound {
		t.Errorf("code = %v, want NotFound", status.Code(err))
	}
}
