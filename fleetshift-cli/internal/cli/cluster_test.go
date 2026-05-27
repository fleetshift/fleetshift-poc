package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	clientauthv1 "k8s.io/client-go/pkg/apis/clientauthentication/v1"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/cli"
)

type fakeClusterServer struct {
	pb.UnimplementedClusterServiceServer
}

func (s *fakeClusterServer) GetClusterConnectionInfo(_ context.Context, req *pb.GetClusterConnectionInfoRequest) (*pb.GetClusterConnectionInfoResponse, error) {
	if req.GetResourceId() == "" {
		return nil, status.Error(codes.InvalidArgument, "resource_id is required")
	}
	if req.GetResourceId() == "nonexistent" {
		return nil, status.Error(codes.NotFound, "cluster not found")
	}
	return &pb.GetClusterConnectionInfoResponse{
		Endpoint: "https://api." + req.GetResourceId() + ".example.com:6443",
		CaCert:   "FAKE-CA-DATA",
	}, nil
}

func (s *fakeClusterServer) GetClusterCredential(_ context.Context, req *pb.GetClusterCredentialRequest) (*pb.GetClusterCredentialResponse, error) {
	if req.GetResourceId() == "" {
		return nil, status.Error(codes.InvalidArgument, "resource_id is required")
	}

	return &pb.GetClusterCredentialResponse{
		Token:      "minted-token-for-" + req.GetResourceId(),
		Expiration: timestamppb.New(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)),
	}, nil
}

func startFakeClusterServer(t *testing.T) string {
	t.Helper()

	srv := grpc.NewServer()
	pb.RegisterClusterServiceServer(srv, &fakeClusterServer{})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	go srv.Serve(lis)
	t.Cleanup(func() { srv.GracefulStop() })

	return lis.Addr().String()
}

func runClusterCLI(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var buf bytes.Buffer
	cmd := cli.New()
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs(args)

	err := cmd.Execute()
	return buf.String(), err
}

func TestClusterLogin_WritesKubeconfig(t *testing.T) {
	addr := startFakeClusterServer(t)

	kcPath := filepath.Join(t.TempDir(), "kubeconfig")

	out, err := runClusterCLI(t,
		"--server", addr,
		"cluster", "login", "test-cluster",
		"--kubeconfig", kcPath,
	)
	if err != nil {
		t.Fatalf("cluster login failed: %v\noutput: %s", err, out)
	}

	if !strings.Contains(out, "Logged into cluster") {
		t.Errorf("expected success message, got: %s", out)
	}

	config, err := clientcmd.LoadFromFile(kcPath)
	if err != nil {
		t.Fatalf("load kubeconfig: %v", err)
	}

	contextName := "fleetshift/test-cluster"

	cluster, ok := config.Clusters[contextName]
	if !ok {
		t.Fatalf("cluster %q not found in kubeconfig", contextName)
	}
	if cluster.Server != "https://api.test-cluster.example.com:6443" {
		t.Errorf("server = %q, want https://api.test-cluster.example.com:6443", cluster.Server)
	}
	if string(cluster.CertificateAuthorityData) != "FAKE-CA-DATA" {
		t.Errorf("ca data = %q, want FAKE-CA-DATA", string(cluster.CertificateAuthorityData))
	}

	authInfo, ok := config.AuthInfos[contextName]
	if !ok {
		t.Fatalf("authinfo %q not found in kubeconfig", contextName)
	}
	if authInfo.Exec == nil {
		t.Fatal("expected exec-based auth, got nil")
	}
	if authInfo.Exec.Command != "fleetctl" {
		t.Errorf("exec command = %q, want fleetctl", authInfo.Exec.Command)
	}
	if len(authInfo.Exec.Args) < 3 || authInfo.Exec.Args[0] != "cluster" || authInfo.Exec.Args[1] != "token" || authInfo.Exec.Args[2] != "test-cluster" {
		t.Errorf("exec args = %v, want [cluster token test-cluster ...]", authInfo.Exec.Args)
	}

	if config.CurrentContext != contextName {
		t.Errorf("current context = %q, want %q", config.CurrentContext, contextName)
	}
}

func TestClusterLogin_NoSetCurrentContext(t *testing.T) {
	addr := startFakeClusterServer(t)

	kcPath := filepath.Join(t.TempDir(), "kubeconfig")

	out, err := runClusterCLI(t,
		"--server", addr,
		"cluster", "login", "test-cluster",
		"--kubeconfig", kcPath,
		"--set-current-context=false",
	)
	if err != nil {
		t.Fatalf("cluster login failed: %v\noutput: %s", err, out)
	}

	config, err := clientcmd.LoadFromFile(kcPath)
	if err != nil {
		t.Fatalf("load kubeconfig: %v", err)
	}

	if config.CurrentContext != "" {
		t.Errorf("current context = %q, want empty", config.CurrentContext)
	}
}

func TestClusterLogin_AppendsToExistingKubeconfig(t *testing.T) {
	addr := startFakeClusterServer(t)

	kcPath := filepath.Join(t.TempDir(), "kubeconfig")

	// Create initial entry
	if _, err := runClusterCLI(t,
		"--server", addr,
		"cluster", "login", "cluster-a",
		"--kubeconfig", kcPath,
	); err != nil {
		t.Fatalf("first login failed: %v", err)
	}

	// Add a second
	if _, err := runClusterCLI(t,
		"--server", addr,
		"cluster", "login", "cluster-b",
		"--kubeconfig", kcPath,
	); err != nil {
		t.Fatalf("second login failed: %v", err)
	}

	config, err := clientcmd.LoadFromFile(kcPath)
	if err != nil {
		t.Fatalf("load kubeconfig: %v", err)
	}

	if _, ok := config.Clusters["fleetshift/cluster-a"]; !ok {
		t.Error("cluster-a should still be in kubeconfig")
	}
	if _, ok := config.Clusters["fleetshift/cluster-b"]; !ok {
		t.Error("cluster-b should be added to kubeconfig")
	}
}

func TestClusterLogin_UsesKUBECONFIGEnv(t *testing.T) {
	addr := startFakeClusterServer(t)

	kcPath := filepath.Join(t.TempDir(), "custom-kubeconfig")
	t.Setenv("KUBECONFIG", kcPath)

	out, err := runClusterCLI(t,
		"--server", addr,
		"cluster", "login", "test-cluster",
	)
	if err != nil {
		t.Fatalf("cluster login failed: %v\noutput: %s", err, out)
	}

	if _, err := os.Stat(kcPath); os.IsNotExist(err) {
		t.Fatal("expected kubeconfig to be created at KUBECONFIG path")
	}
}

func TestClusterLogin_MissingResourceID(t *testing.T) {
	addr := startFakeClusterServer(t)

	_, err := runClusterCLI(t,
		"--server", addr,
		"cluster", "login",
	)
	if err == nil {
		t.Fatal("expected error when resource-id not provided")
	}
}

func TestClusterLogin_PassesServerFlags(t *testing.T) {
	addr := startFakeClusterServer(t)
	kcPath := filepath.Join(t.TempDir(), "kubeconfig")

	out, err := runClusterCLI(t,
		"--server", addr,
		"cluster", "login", "test-cluster",
		"--kubeconfig", kcPath,
	)
	if err != nil {
		t.Fatalf("cluster login failed: %v\noutput: %s", err, out)
	}

	config, err := clientcmd.LoadFromFile(kcPath)
	if err != nil {
		t.Fatalf("load kubeconfig: %v", err)
	}

	authInfo := config.AuthInfos["fleetshift/test-cluster"]
	if authInfo == nil || authInfo.Exec == nil {
		t.Fatal("expected exec auth info")
	}

	argsStr := strings.Join(authInfo.Exec.Args, " ")
	if !strings.Contains(argsStr, "--server") {
		t.Errorf("exec args should contain --server, got: %v", authInfo.Exec.Args)
	}
	if !strings.Contains(argsStr, addr) {
		t.Errorf("exec args should contain server address %q, got: %v", addr, authInfo.Exec.Args)
	}
}

func TestClusterToken_OutputsExecCredential(t *testing.T) {
	addr := startFakeClusterServer(t)

	out, err := runClusterCLI(t,
		"--server", addr,
		"cluster", "token", "test-cluster",
	)
	if err != nil {
		t.Fatalf("cluster token failed: %v\noutput: %s", err, out)
	}

	trimmed := strings.TrimSpace(out)
	var execCred clientauthv1.ExecCredential
	if err := json.Unmarshal([]byte(trimmed), &execCred); err != nil {
		t.Fatalf("output is not valid ExecCredential JSON: %v\noutput: %s", err, out)
	}

	if execCred.APIVersion != "client.authentication.k8s.io/v1" {
		t.Errorf("apiVersion = %q, want client.authentication.k8s.io/v1", execCred.APIVersion)
	}
	if execCred.Status == nil {
		t.Fatal("status should not be nil")
	}
	if !strings.HasPrefix(execCred.Status.Token, "minted-token-for-") {
		t.Errorf("token = %q, want prefix minted-token-for-", execCred.Status.Token)
	}
}

func TestClusterToken_MissingResourceID(t *testing.T) {
	addr := startFakeClusterServer(t)

	_, err := runClusterCLI(t,
		"--server", addr,
		"cluster", "token",
	)
	if err == nil {
		t.Fatal("expected error when resource-id not provided")
	}
}
