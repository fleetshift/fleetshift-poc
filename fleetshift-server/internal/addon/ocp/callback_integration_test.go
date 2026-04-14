package ocp

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	ocpv1 "github.com/fleetshift/fleetshift-poc/gen/ocp/v1"
)

// startCallbackGRPCServer starts a real gRPC server with the callback service
// registered and returns the listener address. This exercises the full gRPC
// stack (serialization, metadata propagation, interceptors) rather than
// calling server methods directly.
func startCallbackGRPCServer(t *testing.T, server ocpv1.CallbackServiceServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	ocpv1.RegisterCallbackServiceServer(srv, server)
	go srv.Serve(lis)
	t.Cleanup(srv.GracefulStop)
	return lis.Addr().String()
}

// dialCallback creates a gRPC client connection to the callback server.
func dialCallback(t *testing.T, addr string) ocpv1.CallbackServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return ocpv1.NewCallbackServiceClient(conn)
}

// outgoingToken attaches a bearer token to outgoing gRPC metadata,
// matching what the ocp-engine callback client does.
func outgoingToken(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

func TestCallbackIntegration_PhaseResult(t *testing.T) {
	signer, err := NewCallbackTokenSigner()
	if err != nil {
		t.Fatal(err)
	}
	provisions := &sync.Map{}
	clusterID := "integration-phase"
	provisions.Store(clusterID, &provisionState{done: make(chan struct{})})

	addr := startCallbackGRPCServer(t, &callbackServer{provisions: provisions, tokenVerifier: signer})
	client := dialCallback(t, addr)

	token, _ := signer.Sign(clusterID, 2*time.Hour)
	ctx := outgoingToken(context.Background(), token)

	_, err = client.ReportPhaseResult(ctx, &ocpv1.PhaseResultRequest{
		ClusterId:      clusterID,
		Phase:          "extract",
		Status:         "complete",
		ElapsedSeconds: 45,
		Attempt:        1,
	})
	if err != nil {
		t.Fatalf("ReportPhaseResult over gRPC: %v", err)
	}
}

func TestCallbackIntegration_Milestone(t *testing.T) {
	signer, _ := NewCallbackTokenSigner()
	provisions := &sync.Map{}
	clusterID := "integration-milestone"
	provisions.Store(clusterID, &provisionState{done: make(chan struct{})})

	addr := startCallbackGRPCServer(t, &callbackServer{provisions: provisions, tokenVerifier: signer})
	client := dialCallback(t, addr)

	token, _ := signer.Sign(clusterID, 2*time.Hour)
	ctx := outgoingToken(context.Background(), token)

	_, err := client.ReportMilestone(ctx, &ocpv1.MilestoneRequest{
		ClusterId:      clusterID,
		Event:          "bootstrap_complete",
		ElapsedSeconds: 300,
		Attempt:        1,
	})
	if err != nil {
		t.Fatalf("ReportMilestone over gRPC: %v", err)
	}
}

func TestCallbackIntegration_CompletionSignalsDone(t *testing.T) {
	signer, _ := NewCallbackTokenSigner()
	provisions := &sync.Map{}
	clusterID := "integration-completion"
	state := &provisionState{done: make(chan struct{})}
	provisions.Store(clusterID, state)

	addr := startCallbackGRPCServer(t, &callbackServer{provisions: provisions, tokenVerifier: signer})
	client := dialCallback(t, addr)

	token, _ := signer.Sign(clusterID, 2*time.Hour)
	ctx := outgoingToken(context.Background(), token)

	_, err := client.ReportCompletion(ctx, &ocpv1.CompletionRequest{
		ClusterId:   clusterID,
		InfraId:     "infra-abc",
		ClusterUuid: "uuid-123",
		ApiServer:   "https://api.test.example.com:6443",
		CaCert:      []byte("test-ca-cert"),
		Region:      "us-east-1",
	})
	if err != nil {
		t.Fatalf("ReportCompletion over gRPC: %v", err)
	}

	select {
	case <-state.done:
	case <-time.After(2 * time.Second):
		t.Fatal("done channel not closed after completion")
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.completion == nil {
		t.Fatal("completion not set")
	}
	if state.completion.InfraId != "infra-abc" {
		t.Errorf("InfraId = %q, want %q", state.completion.InfraId, "infra-abc")
	}
	if state.completion.ApiServer != "https://api.test.example.com:6443" {
		t.Errorf("ApiServer = %q, want %q", state.completion.ApiServer, "https://api.test.example.com:6443")
	}
	if string(state.completion.CaCert) != "test-ca-cert" {
		t.Errorf("CaCert = %q, want %q", state.completion.CaCert, "test-ca-cert")
	}
}

func TestCallbackIntegration_FailureSignalsDone(t *testing.T) {
	signer, _ := NewCallbackTokenSigner()
	provisions := &sync.Map{}
	clusterID := "integration-failure"
	state := &provisionState{done: make(chan struct{})}
	provisions.Store(clusterID, state)

	addr := startCallbackGRPCServer(t, &callbackServer{provisions: provisions, tokenVerifier: signer})
	client := dialCallback(t, addr)

	token, _ := signer.Sign(clusterID, 2*time.Hour)
	ctx := outgoingToken(context.Background(), token)

	_, err := client.ReportFailure(ctx, &ocpv1.FailureRequest{
		ClusterId:       clusterID,
		Phase:           "cluster",
		FailureReason:   "aws_quota",
		FailureMessage:  "vCPU limit exceeded",
		LogTail:         "Error: You have exceeded your vCPU limit",
		RequiresDestroy: true,
		Attempt:         1,
	})
	if err != nil {
		t.Fatalf("ReportFailure over gRPC: %v", err)
	}

	select {
	case <-state.done:
	case <-time.After(2 * time.Second):
		t.Fatal("done channel not closed after failure")
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.failure == nil {
		t.Fatal("failure not set")
	}
	if state.failure.FailureReason != "aws_quota" {
		t.Errorf("FailureReason = %q, want %q", state.failure.FailureReason, "aws_quota")
	}
	if !state.failure.RequiresDestroy {
		t.Error("RequiresDestroy = false, want true")
	}
}

