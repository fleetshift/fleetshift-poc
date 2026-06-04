package callback

import (
	"context"
	"net"
	"sync"
	"testing"

	ocpv1 "github.com/fleetshift/fleetshift-poc/gen/ocp/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// mockServer records every request it receives so tests can inspect them.
type mockServer struct {
	ocpv1.UnimplementedCallbackServiceServer

	mu           sync.Mutex
	phaseResults []*ocpv1.PhaseResultRequest
	milestones   []*ocpv1.MilestoneRequest
	completions  []*ocpv1.CompletionRequest
	failures     []*ocpv1.FailureRequest
	authTokens   []string // bearer tokens extracted from metadata
}

func (m *mockServer) extractToken(ctx context.Context) {
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		if vals := md.Get("authorization"); len(vals) > 0 {
			m.authTokens = append(m.authTokens, vals[0])
		}
	}
}

func (m *mockServer) ReportPhaseResult(ctx context.Context, req *ocpv1.PhaseResultRequest) (*ocpv1.Ack, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.extractToken(ctx)
	m.phaseResults = append(m.phaseResults, req)
	return &ocpv1.Ack{}, nil
}

func (m *mockServer) ReportMilestone(ctx context.Context, req *ocpv1.MilestoneRequest) (*ocpv1.Ack, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.extractToken(ctx)
	m.milestones = append(m.milestones, req)
	return &ocpv1.Ack{}, nil
}

func (m *mockServer) ReportCompletion(ctx context.Context, req *ocpv1.CompletionRequest) (*ocpv1.Ack, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.extractToken(ctx)
	m.completions = append(m.completions, req)
	return &ocpv1.Ack{}, nil
}

func (m *mockServer) ReportFailure(ctx context.Context, req *ocpv1.FailureRequest) (*ocpv1.Ack, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.extractToken(ctx)
	m.failures = append(m.failures, req)
	return &ocpv1.Ack{}, nil
}

// startMockServer starts a gRPC server on a random port and returns
// the mock, the listening address, and a cleanup function.
func startMockServer(t *testing.T) (*mockServer, string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	mock := &mockServer{}
	ocpv1.RegisterCallbackServiceServer(srv, mock)
	go func() { _ = srv.Serve(lis) }()
	return mock, lis.Addr().String(), func() { srv.Stop() }
}

// newTestClient creates a Client connected to the mock server.
func newTestClient(t *testing.T, addr, clusterID, token string) *Client {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return &Client{
		conn:      conn,
		client:    ocpv1.NewCallbackServiceClient(conn),
		clusterID: clusterID,
		token:     token,
	}
}

func TestClient_ReportPhaseResult(t *testing.T) {
	mock, addr, cleanup := startMockServer(t)
	defer cleanup()

	c := newTestClient(t, addr, "cluster-abc", "tok-123")
	defer c.Close()

	err := c.ReportPhaseResult(context.Background(), "install", "success", 42, "", 1)
	if err != nil {
		t.Fatalf("ReportPhaseResult: %v", err)
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()

	if len(mock.phaseResults) != 1 {
		t.Fatalf("expected 1 phase result, got %d", len(mock.phaseResults))
	}
	pr := mock.phaseResults[0]
	if pr.ClusterId != "cluster-abc" {
		t.Errorf("cluster_id = %q, want %q", pr.ClusterId, "cluster-abc")
	}
	if pr.Phase != "install" {
		t.Errorf("phase = %q, want %q", pr.Phase, "install")
	}
	if pr.Status != "success" {
		t.Errorf("status = %q, want %q", pr.Status, "success")
	}
	if pr.ElapsedSeconds != 42 {
		t.Errorf("elapsed = %d, want 42", pr.ElapsedSeconds)
	}
	if pr.Attempt != 1 {
		t.Errorf("attempt = %d, want 1", pr.Attempt)
	}

	// Verify bearer token was sent.
	if len(mock.authTokens) != 1 || mock.authTokens[0] != "Bearer tok-123" {
		t.Errorf("auth tokens = %v, want [Bearer tok-123]", mock.authTokens)
	}
}

func TestClient_ReportMilestone(t *testing.T) {
	mock, addr, cleanup := startMockServer(t)
	defer cleanup()

	c := newTestClient(t, addr, "cluster-xyz", "tok-456")
	defer c.Close()

	err := c.ReportMilestone(context.Background(), "bootstrap-complete", 120, 2)
	if err != nil {
		t.Fatalf("ReportMilestone: %v", err)
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()

	if len(mock.milestones) != 1 {
		t.Fatalf("expected 1 milestone, got %d", len(mock.milestones))
	}
	ms := mock.milestones[0]
	if ms.ClusterId != "cluster-xyz" {
		t.Errorf("cluster_id = %q, want %q", ms.ClusterId, "cluster-xyz")
	}
	if ms.Event != "bootstrap-complete" {
		t.Errorf("event = %q, want %q", ms.Event, "bootstrap-complete")
	}
	if ms.ElapsedSeconds != 120 {
		t.Errorf("elapsed = %d, want 120", ms.ElapsedSeconds)
	}
	if ms.Attempt != 2 {
		t.Errorf("attempt = %d, want 2", ms.Attempt)
	}
}

func TestClient_ReportCompletion(t *testing.T) {
	mock, addr, cleanup := startMockServer(t)
	defer cleanup()

	c := newTestClient(t, addr, "cluster-comp", "tok-789")
	defer c.Close()

	data := CompletionData{
		InfraID:           "infra-001",
		ClusterUUID:       "uuid-002",
		APIServer:         "https://api.example.com:6443",
		Region:            "us-east-1",
		Kubeconfig:        []byte("kubeconfig-data"),
		CACert:            []byte("ca-cert-data"),
		SSHPrivateKey:     []byte("ssh-priv"),
		SSHPublicKey:      []byte("ssh-pub"),
		MetadataJSON:      []byte(`{"key":"value"}`),
		RecoveryAttempted: true,
		ElapsedSeconds:    600,
		Attempt:           1,
	}

	err := c.ReportCompletion(context.Background(), data)
	if err != nil {
		t.Fatalf("ReportCompletion: %v", err)
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()

	if len(mock.completions) != 1 {
		t.Fatalf("expected 1 completion, got %d", len(mock.completions))
	}
	cr := mock.completions[0]
	if cr.ClusterId != "cluster-comp" {
		t.Errorf("cluster_id = %q, want %q", cr.ClusterId, "cluster-comp")
	}
	if cr.InfraId != "infra-001" {
		t.Errorf("infra_id = %q, want %q", cr.InfraId, "infra-001")
	}
	if cr.ClusterUuid != "uuid-002" {
		t.Errorf("cluster_uuid = %q, want %q", cr.ClusterUuid, "uuid-002")
	}
	if cr.ApiServer != "https://api.example.com:6443" {
		t.Errorf("api_server = %q, want %q", cr.ApiServer, "https://api.example.com:6443")
	}
	if string(cr.Kubeconfig) != "kubeconfig-data" {
		t.Errorf("kubeconfig = %q, want %q", cr.Kubeconfig, "kubeconfig-data")
	}
	if string(cr.CaCert) != "ca-cert-data" {
		t.Errorf("ca_cert = %q, want %q", cr.CaCert, "ca-cert-data")
	}
	if string(cr.SshPrivateKey) != "ssh-priv" {
		t.Errorf("ssh_private_key = %q, want %q", cr.SshPrivateKey, "ssh-priv")
	}
	if string(cr.SshPublicKey) != "ssh-pub" {
		t.Errorf("ssh_public_key = %q, want %q", cr.SshPublicKey, "ssh-pub")
	}
	if cr.Region != "us-east-1" {
		t.Errorf("region = %q, want %q", cr.Region, "us-east-1")
	}
	if string(cr.MetadataJson) != `{"key":"value"}` {
		t.Errorf("metadata_json = %q, want %q", cr.MetadataJson, `{"key":"value"}`)
	}
	if !cr.RecoveryAttempted {
		t.Error("recovery_attempted = false, want true")
	}
	if cr.ElapsedSeconds != 600 {
		t.Errorf("elapsed = %d, want 600", cr.ElapsedSeconds)
	}
	if cr.Attempt != 1 {
		t.Errorf("attempt = %d, want 1", cr.Attempt)
	}

	// Verify bearer token.
	if len(mock.authTokens) != 1 || mock.authTokens[0] != "Bearer tok-789" {
		t.Errorf("auth tokens = %v, want [Bearer tok-789]", mock.authTokens)
	}
}

func TestClient_ReportFailure(t *testing.T) {
	mock, addr, cleanup := startMockServer(t)
	defer cleanup()

	c := newTestClient(t, addr, "cluster-fail", "tok-fail")
	defer c.Close()

	data := FailureData{
		Phase:             "install",
		FailureReason:     "timeout",
		FailureMessage:    "cluster did not become ready within 30m",
		LogTail:           "last 500 bytes of logs...",
		RequiresDestroy:   true,
		RecoveryAttempted: false,
		Attempt:           3,
	}

	err := c.ReportFailure(context.Background(), data)
	if err != nil {
		t.Fatalf("ReportFailure: %v", err)
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()

	if len(mock.failures) != 1 {
		t.Fatalf("expected 1 failure, got %d", len(mock.failures))
	}
	fr := mock.failures[0]
	if fr.ClusterId != "cluster-fail" {
		t.Errorf("cluster_id = %q, want %q", fr.ClusterId, "cluster-fail")
	}
	if fr.Phase != "install" {
		t.Errorf("phase = %q, want %q", fr.Phase, "install")
	}
	if fr.FailureReason != "timeout" {
		t.Errorf("failure_reason = %q, want %q", fr.FailureReason, "timeout")
	}
	if fr.FailureMessage != "cluster did not become ready within 30m" {
		t.Errorf("failure_message = %q, want %q", fr.FailureMessage, "cluster did not become ready within 30m")
	}
	if fr.LogTail != "last 500 bytes of logs..." {
		t.Errorf("log_tail = %q, want %q", fr.LogTail, "last 500 bytes of logs...")
	}
	if !fr.RequiresDestroy {
		t.Error("requires_destroy = false, want true")
	}
	if fr.RecoveryAttempted {
		t.Error("recovery_attempted = true, want false")
	}
	if fr.Attempt != 3 {
		t.Errorf("attempt = %d, want 3", fr.Attempt)
	}

	// Verify bearer token.
	if len(mock.authTokens) != 1 || mock.authTokens[0] != "Bearer tok-fail" {
		t.Errorf("auth tokens = %v, want [Bearer tok-fail]", mock.authTokens)
	}
}
