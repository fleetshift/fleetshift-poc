package grpc_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
	transportgrpc "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/grpc"
)

type testClusterAccessProvider struct{}

func (p *testClusterAccessProvider) MintCredential(_ context.Context, callerToken string, target domain.TargetInfo) (*domain.ClusterCredential, error) {
	return &domain.ClusterCredential{
		Token:      "minted-for-" + string(target.ID),
		Expiration: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
	}, nil
}

func setupClusterServer(t *testing.T) pb.ClusterServiceClient {
	t.Helper()

	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}
	targetSvc := &application.TargetService{Store: store}
	reg := application.NewClusterAccessRegistry()
	reg.Register("gcphcp", &testClusterAccessProvider{})

	ctx := context.Background()

	if err := targetSvc.Register(ctx, domain.TargetInfo{
		ID:   "seeded-gcp",
		Type: "gcphcp",
		Name: "GCP Target",
	}); err != nil {
		t.Fatalf("register seeded target: %v", err)
	}

	if err := targetSvc.Register(ctx, domain.TargetInfo{
		ID:   "k8s-test-cluster",
		Type: "kubernetes",
		Name: "test-cluster",
		Properties: map[string]string{
			"api_server": "https://api.test.example.com:6443",
			"ca_cert":    "TEST-CA",
		},
		ProvisioningTargetID: "seeded-gcp",
	}); err != nil {
		t.Fatalf("register k8s target: %v", err)
	}

	clusterSvc := &application.ClusterService{
		Targets:   targetSvc,
		Providers: reg,
	}

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterClusterServiceServer(srv, &transportgrpc.ClusterServer{
		Clusters: clusterSvc,
	})

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return pb.NewClusterServiceClient(conn)
}

func TestClusterServer_GetConnectionInfo_Success(t *testing.T) {
	client := setupClusterServer(t)

	resp, err := client.GetClusterConnectionInfo(context.Background(), &pb.GetClusterConnectionInfoRequest{
		ResourceId: "test-cluster",
	})
	if err != nil {
		t.Fatalf("GetClusterConnectionInfo: %v", err)
	}
	if resp.GetEndpoint() != "https://api.test.example.com:6443" {
		t.Errorf("endpoint = %q, want https://api.test.example.com:6443", resp.GetEndpoint())
	}
	if resp.GetCaCert() != "TEST-CA" {
		t.Errorf("ca_cert = %q, want TEST-CA", resp.GetCaCert())
	}
}

func TestClusterServer_GetConnectionInfo_EmptyResourceID(t *testing.T) {
	client := setupClusterServer(t)

	_, err := client.GetClusterConnectionInfo(context.Background(), &pb.GetClusterConnectionInfoRequest{
		ResourceId: "",
	})
	if err == nil {
		t.Fatal("expected error for empty resource_id")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestClusterServer_GetConnectionInfo_NotFound(t *testing.T) {
	client := setupClusterServer(t)

	_, err := client.GetClusterConnectionInfo(context.Background(), &pb.GetClusterConnectionInfoRequest{
		ResourceId: "nonexistent",
	})
	if err == nil {
		t.Fatal("expected error for nonexistent cluster")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.NotFound {
		t.Errorf("code = %v, want NotFound", status.Code(err))
	}
}

func TestClusterServer_GetCredential_Success(t *testing.T) {
	client := setupClusterServer(t)

	ctx := metadata.AppendToOutgoingContext(context.Background(),
		"authorization", "Bearer my-test-token",
	)

	resp, err := client.GetClusterCredential(ctx, &pb.GetClusterCredentialRequest{
		ResourceId: "test-cluster",
	})
	if err != nil {
		t.Fatalf("GetClusterCredential: %v", err)
	}
	if resp.GetToken() != "minted-for-seeded-gcp" {
		t.Errorf("token = %q, want minted-for-seeded-gcp", resp.GetToken())
	}
	if resp.GetExpiration() == nil {
		t.Error("expiration should not be nil")
	}
}

func TestClusterServer_GetCredential_EmptyResourceID(t *testing.T) {
	client := setupClusterServer(t)

	ctx := metadata.AppendToOutgoingContext(context.Background(),
		"authorization", "Bearer token",
	)

	_, err := client.GetClusterCredential(ctx, &pb.GetClusterCredentialRequest{
		ResourceId: "",
	})
	if err == nil {
		t.Fatal("expected error for empty resource_id")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestClusterServer_GetCredential_MissingBearerToken(t *testing.T) {
	client := setupClusterServer(t)

	_, err := client.GetClusterCredential(context.Background(), &pb.GetClusterCredentialRequest{
		ResourceId: "test-cluster",
	})
	if err == nil {
		t.Fatal("expected error for missing bearer token")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.Unauthenticated {
		t.Errorf("code = %v, want Unauthenticated", status.Code(err))
	}
}

func TestClusterServer_GetCredential_NotFound(t *testing.T) {
	client := setupClusterServer(t)

	ctx := metadata.AppendToOutgoingContext(context.Background(),
		"authorization", "Bearer token",
	)

	_, err := client.GetClusterCredential(ctx, &pb.GetClusterCredentialRequest{
		ResourceId: "nonexistent",
	})
	if err == nil {
		t.Fatal("expected error for nonexistent cluster")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.NotFound {
		t.Errorf("code = %v, want NotFound", status.Code(err))
	}
}
