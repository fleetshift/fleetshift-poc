package cli_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/zalando/go-keyring"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/pkg/canonical"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
)

// capturingDeploymentServer records signing fields from requests so tests
// can verify the CLI populated them correctly.
type capturingDeploymentServer struct {
	pb.UnimplementedDeploymentServiceServer

	mu          sync.Mutex
	deployments map[string]*pb.Deployment

	lastCreateReq *pb.CreateDeploymentRequest
	lastResumeReq *pb.ResumeDeploymentRequest
}

func newCapturingDeploymentServer() *capturingDeploymentServer {
	return &capturingDeploymentServer{deployments: make(map[string]*pb.Deployment)}
}

func (s *capturingDeploymentServer) CreateDeployment(_ context.Context, req *pb.CreateDeploymentRequest) (*pb.Deployment, error) {
	if req.GetDeploymentId() == "" {
		return nil, status.Error(codes.InvalidArgument, "deployment_id is required")
	}
	name := "deployments/" + req.GetDeploymentId()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.lastCreateReq = req

	if _, exists := s.deployments[name]; exists {
		return nil, status.Errorf(codes.AlreadyExists, "deployment %q already exists", name)
	}

	dep := &pb.Deployment{
		Name:              name,
		Uid:               "uid-" + req.GetDeploymentId(),
		State:             pb.Deployment_STATE_ACTIVE,
		ManifestStrategy:  req.GetDeployment().GetManifestStrategy(),
		PlacementStrategy: req.GetDeployment().GetPlacementStrategy(),
		RolloutStrategy:   req.GetDeployment().GetRolloutStrategy(),
		CreateTime:        timestamppb.Now(),
		UpdateTime:        timestamppb.Now(),
	}
	s.deployments[name] = dep
	return dep, nil
}

func (s *capturingDeploymentServer) GetDeployment(_ context.Context, req *pb.GetDeploymentRequest) (*pb.Deployment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dep, ok := s.deployments[req.GetName()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "deployment %q not found", req.GetName())
	}
	return dep, nil
}

func (s *capturingDeploymentServer) ResumeDeployment(_ context.Context, req *pb.ResumeDeploymentRequest) (*pb.Deployment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lastResumeReq = req

	dep, ok := s.deployments[req.GetName()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "deployment %q not found", req.GetName())
	}
	if dep.GetState() != pb.Deployment_STATE_PAUSED_AUTH {
		return nil, status.Errorf(codes.FailedPrecondition, "deployment is not paused for auth")
	}
	dep.State = pb.Deployment_STATE_CREATING
	return dep, nil
}

func startCapturingServer(t *testing.T, fake *capturingDeploymentServer) string {
	t.Helper()
	srv := grpc.NewServer()
	pb.RegisterDeploymentServiceServer(srv, fake)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.Serve(lis)
	t.Cleanup(func() { srv.GracefulStop() })
	return lis.Addr().String()
}

func generateAndStoreTestKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()

	keyring.MockInit()

	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	der, err := x509.MarshalECPrivateKey(privKey)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	pemBlock := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der})
	if err := auth.SaveSigningKey(string(pemBlock)); err != nil {
		t.Fatalf("save signing key: %v", err)
	}

	return privKey
}

func TestDeploymentCreate_Sign_PopulatesSignatureFields(t *testing.T) {
	privKey := generateAndStoreTestKey(t)
	fake := newCapturingDeploymentServer()
	addr := startCapturingServer(t, fake)
	manifestFile := writeManifestFile(t, `{"kind":"ConfigMap","name":"test"}`)

	out := runCLI(t,
		"--server", addr,
		"deployment", "create",
		"--id", "signed-dep",
		"--manifest-file", manifestFile,
		"--resource-type", "test.resource",
		"--placement-type", "all",
		"--sign",
	)

	if !strings.Contains(out, "deployments/signed-dep") {
		t.Fatalf("expected deployment name in output, got:\n%s", out)
	}

	fake.mu.Lock()
	req := fake.lastCreateReq
	fake.mu.Unlock()

	if req == nil {
		t.Fatal("no create request captured")
	}
	if len(req.GetUserSignature()) == 0 {
		t.Error("user_signature should be non-empty")
	}
	if req.GetValidUntil() == nil {
		t.Error("valid_until should be set")
	}
	if req.GetExpectedGeneration() != 1 {
		t.Errorf("expected_generation = %d, want 1", req.GetExpectedGeneration())
	}

	ms, ps := canonicalStrategiesFromReq(req)
	envelopeBytes, err := canonical.BuildSignedInputEnvelope(
		req.GetDeploymentId(),
		ms, ps,
		req.GetValidUntil().AsTime(),
		nil,
		req.GetExpectedGeneration(),
	)
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}
	hash := sha256.Sum256(envelopeBytes)
	if !ecdsa.VerifyASN1(&privKey.PublicKey, hash[:], req.GetUserSignature()) {
		t.Error("signature verification failed — CLI-produced signature does not match envelope")
	}
}

func TestDeploymentResume_Sign_PopulatesSignatureFields(t *testing.T) {
	privKey := generateAndStoreTestKey(t)
	fake := newCapturingDeploymentServer()

	fake.deployments["deployments/paused-signed"] = &pb.Deployment{
		Name:  "deployments/paused-signed",
		State: pb.Deployment_STATE_PAUSED_AUTH,
		ManifestStrategy: &pb.ManifestStrategy{
			Type: pb.ManifestStrategy_TYPE_INLINE,
			Manifests: []*pb.Manifest{{
				ResourceType: "test.resource",
				Raw:          []byte(`{"kind":"ConfigMap"}`),
			}},
		},
		PlacementStrategy: &pb.PlacementStrategy{
			Type: pb.PlacementStrategy_TYPE_ALL,
		},
		Provenance: &pb.Provenance{
			ExpectedGeneration: 1,
		},
		CreateTime: timestamppb.Now(),
		UpdateTime: timestamppb.Now(),
	}

	addr := startCapturingServer(t, fake)

	out := runCLI(t,
		"--server", addr,
		"deployment", "resume", "paused-signed",
		"--sign",
	)

	if !strings.Contains(out, "deployments/paused-signed") {
		t.Fatalf("expected deployment name in output, got:\n%s", out)
	}

	fake.mu.Lock()
	req := fake.lastResumeReq
	fake.mu.Unlock()

	if req == nil {
		t.Fatal("no resume request captured")
	}
	if len(req.GetUserSignature()) == 0 {
		t.Error("user_signature should be non-empty")
	}
	if req.GetValidUntil() == nil {
		t.Error("valid_until should be set")
	}

	dep := fake.deployments["deployments/paused-signed"]
	ms, ps := canonicalStrategiesFromDeployment(dep)
	depID := "paused-signed"
	expectedGen := dep.GetProvenance().GetExpectedGeneration() + 1

	envelopeBytes, err := canonical.BuildSignedInputEnvelope(
		depID, ms, ps,
		req.GetValidUntil().AsTime(),
		nil,
		expectedGen,
	)
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}
	hash := sha256.Sum256(envelopeBytes)
	if !ecdsa.VerifyASN1(&privKey.PublicKey, hash[:], req.GetUserSignature()) {
		t.Error("signature verification failed — CLI-produced resume signature does not match envelope")
	}
}

func TestDeploymentCreate_Sign_NoKey_Fails(t *testing.T) {
	keyring.MockInit()

	fake := newCapturingDeploymentServer()
	addr := startCapturingServer(t, fake)
	manifestFile := writeManifestFile(t, `{"kind":"ConfigMap"}`)

	_, err := runCLIErr(t,
		"--server", addr,
		"deployment", "create",
		"--id", "no-key-dep",
		"--manifest-file", manifestFile,
		"--resource-type", "test.resource",
		"--placement-type", "all",
		"--sign",
	)
	if err == nil {
		t.Error("expected error when signing key is not enrolled")
	}
}

func canonicalStrategiesFromReq(req *pb.CreateDeploymentRequest) (canonical.ManifestStrategy, canonical.PlacementStrategy) {
	return canonicalStrategiesFromDeployment(req.GetDeployment())
}

func canonicalStrategiesFromDeployment(dep *pb.Deployment) (canonical.ManifestStrategy, canonical.PlacementStrategy) {
	var ms canonical.ManifestStrategy
	if p := dep.GetManifestStrategy(); p != nil {
		switch p.GetType() {
		case pb.ManifestStrategy_TYPE_INLINE:
			ms.Type = "inline"
			for _, m := range p.GetManifests() {
				ms.Manifests = append(ms.Manifests, canonical.Manifest{
					ResourceType: m.GetResourceType(),
					Raw:          m.GetRaw(),
				})
			}
		}
	}

	var ps canonical.PlacementStrategy
	if p := dep.GetPlacementStrategy(); p != nil {
		switch p.GetType() {
		case pb.PlacementStrategy_TYPE_STATIC:
			ps.Type = "static"
			ps.Targets = p.GetTargetIds()
		case pb.PlacementStrategy_TYPE_ALL:
			ps.Type = "all"
		case pb.PlacementStrategy_TYPE_SELECTOR:
			ps.Type = "selector"
			if sel := p.GetTargetSelector(); sel != nil {
				ps.MatchLabels = sel.GetMatchLabels()
			}
		}
	}

	return ms, ps
}
