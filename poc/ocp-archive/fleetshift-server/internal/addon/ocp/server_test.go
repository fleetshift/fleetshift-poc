package ocp

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	ocpv1 "github.com/fleetshift/fleetshift-poc/gen/ocp/v1"
)

func TestServer_StartAndShutdown(t *testing.T) {
	agent := NewAgent(nopReporter{},
		WithTokenSigner(mustNewSigner(t)),
	)

	if err := agent.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("Start: %v", err)
	}

	addr := agent.CallbackAddr()
	if addr == "" {
		t.Fatal("CallbackAddr() returned empty after Start()")
	}

	// Verify server is accepting connections
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	agent.Shutdown(ctx)
}

func TestServer_AuthenticatedCallback(t *testing.T) {
	signer := mustNewSigner(t)
	agent := NewAgent(nopReporter{}, WithTokenSigner(signer))

	if err := agent.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer agent.Shutdown(context.Background())

	clusterID := "test-server-cluster"
	state := &provisionState{done: make(chan struct{})}
	agent.provisions.Store(clusterID, state)

	token, err := signer.Sign(clusterID, 5*time.Minute)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	conn, err := grpc.NewClient(agent.CallbackAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	client := ocpv1.NewCallbackServiceClient(conn)
	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+token)

	_, err = client.ReportCompletion(ctx, &ocpv1.CompletionRequest{
		ClusterId: clusterID,
		InfraId:   "infra-server-test",
		ApiServer: "https://api.test.example.com:6443",
	})
	if err != nil {
		t.Fatalf("ReportCompletion: %v", err)
	}

	select {
	case <-state.done:
	default:
		t.Error("expected done channel to be closed after completion")
	}
}

func TestServer_RejectsUnauthenticatedCallback(t *testing.T) {
	signer := mustNewSigner(t)
	agent := NewAgent(nopReporter{}, WithTokenSigner(signer))

	if err := agent.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer agent.Shutdown(context.Background())

	conn, err := grpc.NewClient(agent.CallbackAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	client := ocpv1.NewCallbackServiceClient(conn)

	// No auth token — should be rejected by callback handler
	_, err = client.ReportCompletion(context.Background(), &ocpv1.CompletionRequest{
		ClusterId: "some-cluster",
	})
	if err == nil {
		t.Fatal("expected error for unauthenticated callback, got nil")
	}
}

func mustNewSigner(t *testing.T) *CallbackTokenSigner {
	t.Helper()
	s, err := NewCallbackTokenSigner()
	if err != nil {
		t.Fatalf("NewCallbackTokenSigner: %v", err)
	}
	return s
}
