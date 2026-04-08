package cli_test

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/cli"
)

// fakeDeploymentServer implements DeploymentServiceServer with in-memory
// storage, avoiding any dependency on server internals.
type fakeDeploymentServer struct {
	pb.UnimplementedDeploymentServiceServer

	mu          sync.Mutex
	deployments map[string]*pb.Deployment
}

func newFakeDeploymentServer() *fakeDeploymentServer {
	return &fakeDeploymentServer{deployments: make(map[string]*pb.Deployment)}
}

func (s *fakeDeploymentServer) CreateDeployment(_ context.Context, req *pb.CreateDeploymentRequest) (*pb.Deployment, error) {
	if req.GetDeploymentId() == "" {
		return nil, status.Error(codes.InvalidArgument, "deployment_id is required")
	}

	name := "deployments/" + req.GetDeploymentId()

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.deployments[name]; exists {
		return nil, status.Errorf(codes.AlreadyExists, "deployment %q already exists", name)
	}

	dep := &pb.Deployment{
		Name:              name,
		Uid:               "test-uid-" + req.GetDeploymentId(),
		State:             pb.Deployment_STATE_ACTIVE,
		Reconciling:       false,
		ManifestStrategy:  req.GetDeployment().GetManifestStrategy(),
		PlacementStrategy: req.GetDeployment().GetPlacementStrategy(),
		RolloutStrategy:   req.GetDeployment().GetRolloutStrategy(),
		CreateTime:        timestamppb.Now(),
		UpdateTime:        timestamppb.Now(),
	}

	if dep.PlacementStrategy.GetType() == pb.PlacementStrategy_TYPE_STATIC {
		dep.ResolvedTargetIds = dep.PlacementStrategy.GetTargetIds()
	}

	s.deployments[name] = dep
	return dep, nil
}

func (s *fakeDeploymentServer) GetDeployment(_ context.Context, req *pb.GetDeploymentRequest) (*pb.Deployment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dep, ok := s.deployments[req.GetName()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "deployment %q not found", req.GetName())
	}
	return dep, nil
}

func (s *fakeDeploymentServer) ListDeployments(_ context.Context, _ *pb.ListDeploymentsRequest) (*pb.ListDeploymentsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	deps := make([]*pb.Deployment, 0, len(s.deployments))
	for _, d := range s.deployments {
		deps = append(deps, d)
	}
	return &pb.ListDeploymentsResponse{Deployments: deps}, nil
}

func (s *fakeDeploymentServer) DeleteDeployment(_ context.Context, req *pb.DeleteDeploymentRequest) (*pb.Deployment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dep, ok := s.deployments[req.GetName()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "deployment %q not found", req.GetName())
	}

	dep.State = pb.Deployment_STATE_DELETING
	dep.Reconciling = true
	delete(s.deployments, req.GetName())
	return dep, nil
}

func (s *fakeDeploymentServer) ResumeDeployment(_ context.Context, req *pb.ResumeDeploymentRequest) (*pb.Deployment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dep, ok := s.deployments[req.GetName()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "deployment %q not found", req.GetName())
	}
	if dep.GetState() != pb.Deployment_STATE_PAUSED_AUTH {
		return nil, status.Errorf(codes.FailedPrecondition, "deployment %q is not paused for auth", req.GetName())
	}

	dep.State = pb.Deployment_STATE_CREATING
	dep.Reconciling = true
	return dep, nil
}

// startFakeServer launches an in-process gRPC server with the fake
// deployment service and returns its address.
func startFakeServer(t *testing.T) string {
	t.Helper()

	srv := grpc.NewServer()
	pb.RegisterDeploymentServiceServer(srv, newFakeDeploymentServer())

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	go srv.Serve(lis)
	t.Cleanup(func() { srv.GracefulStop() })

	return lis.Addr().String()
}

func runCLI(t *testing.T, args ...string) string {
	t.Helper()
	var buf bytes.Buffer
	cmd := cli.New()
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs(args)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("fleetctl %s failed: %v\noutput: %s", strings.Join(args, " "), err, buf.String())
	}
	return buf.String()
}

func runCLIErr(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var buf bytes.Buffer
	cmd := cli.New()
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return buf.String(), err
}

func writeManifestFile(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write manifest file: %v", err)
	}
	return path
}

func TestDeploymentCreateGetList_Table(t *testing.T) {
	addr := startFakeServer(t)
	manifestFile := writeManifestFile(t, `{"kind":"ConfigMap","name":"test"}`)

	out := runCLI(t,
		"--server", addr, "--output", "table",
		"deployment", "create",
		"--id", "alpha",
		"--manifest-file", manifestFile,
		"--resource-type", "test.resource",
		"--placement-type", "all",
	)
	if !strings.Contains(out, "deployments/alpha") {
		t.Errorf("create output should contain deployment name, got:\n%s", out)
	}
	if !strings.Contains(out, "NAME") {
		t.Errorf("create table output should contain header, got:\n%s", out)
	}

	out = runCLI(t, "--server", addr, "deployment", "get", "alpha")
	if !strings.Contains(out, "deployments/alpha") {
		t.Errorf("get output should contain deployment name, got:\n%s", out)
	}

	out = runCLI(t, "--server", addr, "deployment", "get", "deployments/alpha")
	if !strings.Contains(out, "deployments/alpha") {
		t.Errorf("get with full name should work, got:\n%s", out)
	}

	out = runCLI(t, "--server", addr, "deployment", "list")
	if !strings.Contains(out, "deployments/alpha") {
		t.Errorf("list should contain alpha, got:\n%s", out)
	}
}

func TestDeploymentCreateGetList_JSON(t *testing.T) {
	addr := startFakeServer(t)
	manifestFile := writeManifestFile(t, `{"kind":"ConfigMap","name":"test"}`)

	out := runCLI(t,
		"--server", addr, "--output", "json",
		"deployment", "create",
		"--id", "beta",
		"--manifest-file", manifestFile,
		"--resource-type", "test.resource",
		"--placement-type", "all",
	)
	if !strings.Contains(out, `"deployments/beta"`) {
		t.Errorf("create JSON should contain name field, got:\n%s", out)
	}
	if !strings.Contains(out, `"state"`) {
		t.Errorf("create JSON should contain state field, got:\n%s", out)
	}

	out = runCLI(t, "--server", addr, "-o", "json", "deployment", "get", "beta")
	if !strings.Contains(out, `"deployments/beta"`) {
		t.Errorf("get JSON should contain name, got:\n%s", out)
	}

	out = runCLI(t, "--server", addr, "-o", "json", "deployment", "list")
	if !strings.HasPrefix(strings.TrimSpace(out), "[") {
		t.Errorf("list JSON should be an array, got:\n%s", out)
	}
	if !strings.Contains(out, `"deployments/beta"`) {
		t.Errorf("list JSON should contain beta, got:\n%s", out)
	}
}

func TestDeploymentList_Empty(t *testing.T) {
	addr := startFakeServer(t)

	out := runCLI(t, "--server", addr, "deployment", "list")
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) != 1 {
		t.Errorf("empty list table should have header only, got %d lines:\n%s", len(lines), out)
	}

	out = runCLI(t, "--server", addr, "-o", "json", "deployment", "list")
	if strings.TrimSpace(out) != "[]" {
		t.Errorf("empty list JSON should be [], got: %q", strings.TrimSpace(out))
	}
}

func TestDeploymentGet_NotFound(t *testing.T) {
	addr := startFakeServer(t)
	_, err := runCLIErr(t, "--server", addr, "deployment", "get", "nonexistent")
	if err == nil {
		t.Error("get nonexistent should fail")
	}
}

func TestDeploymentCreate_StaticPlacement(t *testing.T) {
	addr := startFakeServer(t)
	manifestFile := writeManifestFile(t, `{"data":"x"}`)

	out := runCLI(t,
		"--server", addr,
		"deployment", "create",
		"--id", "static-dep",
		"--manifest-file", manifestFile,
		"--resource-type", "test.resource",
		"--placement-type", "static",
		"--target-ids", "t1,t2",
	)
	if !strings.Contains(out, "deployments/static-dep") {
		t.Errorf("create with static placement should work, got:\n%s", out)
	}
}

func TestDeploymentCreate_StaticPlacement_ResolvedTargets(t *testing.T) {
	addr := startFakeServer(t)
	manifestFile := writeManifestFile(t, `{"data":"x"}`)

	out := runCLI(t,
		"--server", addr,
		"deployment", "create",
		"--id", "tgt-dep",
		"--manifest-file", manifestFile,
		"--resource-type", "test.resource",
		"--placement-type", "static",
		"--target-ids", "node-a,node-b",
	)
	if !strings.Contains(out, "node-a") {
		t.Errorf("static placement should show resolved targets, got:\n%s", out)
	}
}

func TestInvalidOutputFormat(t *testing.T) {
	_, err := runCLIErr(t, "--output", "yaml", "deployment", "list")
	if err == nil {
		t.Error("unsupported output format should fail")
	}
}

func TestDeploymentAlias(t *testing.T) {
	addr := startFakeServer(t)

	out := runCLI(t, "--server", addr, "dep", "list")
	if !strings.Contains(out, "NAME") {
		t.Errorf("dep alias should work, got:\n%s", out)
	}
}

func TestDeploymentResume(t *testing.T) {
	fakeSrv := newFakeDeploymentServer()

	fakeSrv.deployments["deployments/paused-dep"] = &pb.Deployment{
		Name:        "deployments/paused-dep",
		State:       pb.Deployment_STATE_PAUSED_AUTH,
		Reconciling: true,
		CreateTime:  timestamppb.Now(),
		UpdateTime:  timestamppb.Now(),
		ManifestStrategy: &pb.ManifestStrategy{
			Type: pb.ManifestStrategy_TYPE_INLINE,
		},
		PlacementStrategy: &pb.PlacementStrategy{
			Type: pb.PlacementStrategy_TYPE_ALL,
		},
	}

	srv := grpc.NewServer()
	pb.RegisterDeploymentServiceServer(srv, fakeSrv)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.Serve(lis)
	t.Cleanup(func() { srv.GracefulStop() })
	addr := lis.Addr().String()

	out := runCLI(t, "--server", addr, "deployment", "resume", "paused-dep")
	if !strings.Contains(out, "deployments/paused-dep") {
		t.Errorf("resume output should contain deployment name, got:\n%s", out)
	}
	if !strings.Contains(out, "Creating") {
		t.Errorf("resume output should show resumed state, got:\n%s", out)
	}
}

func TestDeploymentResume_NotFound(t *testing.T) {
	addr := startFakeServer(t)
	_, err := runCLIErr(t, "--server", addr, "deployment", "resume", "nonexistent")
	if err == nil {
		t.Error("resume nonexistent should fail")
	}
}

func TestDeploymentDelete(t *testing.T) {
	addr := startFakeServer(t)
	manifestFile := writeManifestFile(t, `{"data":"x"}`)

	runCLI(t,
		"--server", addr,
		"deployment", "create",
		"--id", "to-delete",
		"--manifest-file", manifestFile,
		"--resource-type", "test.resource",
		"--placement-type", "all",
	)

	out := runCLI(t, "--server", addr, "deployment", "delete", "to-delete")
	if !strings.Contains(out, "deployments/to-delete") {
		t.Errorf("delete output should contain deployment name, got:\n%s", out)
	}
	if !strings.Contains(out, "Deleting") {
		t.Errorf("delete output should show Deleting state, got:\n%s", out)
	}

	// Deployment should be gone.
	_, err := runCLIErr(t, "--server", addr, "deployment", "get", "to-delete")
	if err == nil {
		t.Error("get after delete should fail")
	}
}

func TestDeploymentDelete_NotFound(t *testing.T) {
	addr := startFakeServer(t)
	_, err := runCLIErr(t, "--server", addr, "deployment", "delete", "nonexistent")
	if err == nil {
		t.Error("delete nonexistent should fail")
	}
}
