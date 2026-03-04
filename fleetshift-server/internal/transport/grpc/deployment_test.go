package grpc_test

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/syncworkflow"
	transportgrpc "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/grpc"
)

const testTargetType domain.TargetType = "test"

func setup(t *testing.T) pb.DeploymentServiceClient {
	t.Helper()

	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	recordingAgent := &sqlite.RecordingDeliveryService{
		Store: store,
		Now:   func() time.Time { return time.Date(2026, 3, 2, 12, 0, 0, 0, time.UTC) },
	}
	router := delivery.NewRoutingDeliveryService()
	router.Register(testTargetType, recordingAgent)

	owf := &domain.OrchestrationWorkflow{
		Store:      store,
		Delivery:   router,
		Strategies: domain.DefaultStrategyFactory{},
	}
	cwf := &domain.CreateDeploymentWorkflow{Store: store}

	engine := &syncworkflow.Engine{}
	runners, err := engine.Register(owf, cwf)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	deploymentSvc := &application.DeploymentService{
		Store:    store,
		CreateWF: runners.CreateDeployment,
	}

	// Register a test target so placements resolve.
	targetSvc := &application.TargetService{Store: store}
	if err := targetSvc.Register(context.Background(), domain.TargetInfo{
		ID: "t1", Type: testTargetType, Name: "test-target",
	}); err != nil {
		t.Fatalf("register target: %v", err)
	}

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterDeploymentServiceServer(srv, &transportgrpc.DeploymentServer{
		Deployments: deploymentSvc,
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

	return pb.NewDeploymentServiceClient(conn)
}

func TestCreateThenGet(t *testing.T) {
	client := setup(t)
	ctx := context.Background()

	raw, _ := json.Marshal(map[string]string{"key": "value"})

	created, err := client.CreateDeployment(ctx, &pb.CreateDeploymentRequest{
		DeploymentId: "dep-1",
		Deployment: &pb.Deployment{
			ManifestStrategy: &pb.ManifestStrategy{
				Type: pb.ManifestStrategy_TYPE_INLINE,
				Manifests: []*pb.Manifest{{
					ResourceType: "test.resource",
					Raw:          raw,
				}},
			},
			PlacementStrategy: &pb.PlacementStrategy{
				Type:      pb.PlacementStrategy_TYPE_STATIC,
				TargetIds: []string{"t1"},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateDeployment: %v", err)
	}

	if created.GetName() != "deployments/dep-1" {
		t.Errorf("name = %q, want %q", created.GetName(), "deployments/dep-1")
	}
	if created.GetUid() == "" {
		t.Error("created uid is empty, want non-empty UUID")
	}
	if created.GetCreateTime() == nil {
		t.Error("created create_time is nil, want non-nil")
	}
	if created.GetEtag() == "" {
		t.Error("created etag is empty, want non-empty")
	}

	got, err := client.GetDeployment(ctx, &pb.GetDeploymentRequest{
		Name: "deployments/dep-1",
	})
	if err != nil {
		t.Fatalf("GetDeployment: %v", err)
	}

	if got.GetName() != created.GetName() {
		t.Errorf("got name = %q, want %q", got.GetName(), created.GetName())
	}
	if got.GetManifestStrategy().GetType() != pb.ManifestStrategy_TYPE_INLINE {
		t.Errorf("manifest strategy type = %v, want INLINE", got.GetManifestStrategy().GetType())
	}
	if got.GetPlacementStrategy().GetType() != pb.PlacementStrategy_TYPE_STATIC {
		t.Errorf("placement strategy type = %v, want STATIC", got.GetPlacementStrategy().GetType())
	}
	if got.GetUid() == "" {
		t.Error("uid is empty, want non-empty UUID")
	}
	if got.GetCreateTime() == nil {
		t.Error("create_time is nil, want non-nil")
	}
	if got.GetUpdateTime() == nil {
		t.Error("update_time is nil, want non-nil")
	}
	if got.GetEtag() == "" {
		t.Error("etag is empty, want non-empty")
	}
}

func TestCreateThenList(t *testing.T) {
	client := setup(t)
	ctx := context.Background()

	raw, _ := json.Marshal(map[string]string{"k": "v"})
	for _, id := range []string{"dep-a", "dep-b"} {
		_, err := client.CreateDeployment(ctx, &pb.CreateDeploymentRequest{
			DeploymentId: id,
			Deployment: &pb.Deployment{
				ManifestStrategy: &pb.ManifestStrategy{
					Type:      pb.ManifestStrategy_TYPE_INLINE,
					Manifests: []*pb.Manifest{{ResourceType: "t", Raw: raw}},
				},
				PlacementStrategy: &pb.PlacementStrategy{
					Type:      pb.PlacementStrategy_TYPE_STATIC,
					TargetIds: []string{"t1"},
				},
			},
		})
		if err != nil {
			t.Fatalf("CreateDeployment(%s): %v", id, err)
		}
	}

	resp, err := client.ListDeployments(ctx, &pb.ListDeploymentsRequest{})
	if err != nil {
		t.Fatalf("ListDeployments: %v", err)
	}
	if len(resp.GetDeployments()) != 2 {
		t.Fatalf("got %d deployments, want 2", len(resp.GetDeployments()))
	}
}

func TestGetNotFound(t *testing.T) {
	client := setup(t)
	ctx := context.Background()

	_, err := client.GetDeployment(ctx, &pb.GetDeploymentRequest{
		Name: "deployments/nonexistent",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.NotFound {
		t.Errorf("code = %v, want NotFound", status.Code(err))
	}
}

func TestCreateDuplicate(t *testing.T) {
	client := setup(t)
	ctx := context.Background()

	raw, _ := json.Marshal(map[string]string{"k": "v"})
	req := &pb.CreateDeploymentRequest{
		DeploymentId: "dup",
		Deployment: &pb.Deployment{
			ManifestStrategy: &pb.ManifestStrategy{
				Type:      pb.ManifestStrategy_TYPE_INLINE,
				Manifests: []*pb.Manifest{{ResourceType: "t", Raw: raw}},
			},
			PlacementStrategy: &pb.PlacementStrategy{
				Type:      pb.PlacementStrategy_TYPE_STATIC,
				TargetIds: []string{"t1"},
			},
		},
	}

	if _, err := client.CreateDeployment(ctx, req); err != nil {
		t.Fatalf("first create: %v", err)
	}

	_, err := client.CreateDeployment(ctx, req)
	if err == nil {
		t.Fatal("expected error on duplicate, got nil")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.AlreadyExists {
		t.Errorf("code = %v, want AlreadyExists", status.Code(err))
	}
}

func TestCreateMissingFields(t *testing.T) {
	client := setup(t)
	ctx := context.Background()

	_, err := client.CreateDeployment(ctx, &pb.CreateDeploymentRequest{
		DeploymentId: "",
		Deployment:   &pb.Deployment{},
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestCreateStateAndReconciling(t *testing.T) {
	client := setup(t)
	ctx := context.Background()

	raw, _ := json.Marshal(map[string]string{"k": "v"})
	created, err := client.CreateDeployment(ctx, &pb.CreateDeploymentRequest{
		DeploymentId: "state-test",
		Deployment: &pb.Deployment{
			ManifestStrategy: &pb.ManifestStrategy{
				Type:      pb.ManifestStrategy_TYPE_INLINE,
				Manifests: []*pb.Manifest{{ResourceType: "t", Raw: raw}},
			},
			PlacementStrategy: &pb.PlacementStrategy{
				Type:      pb.PlacementStrategy_TYPE_STATIC,
				TargetIds: []string{"t1"},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateDeployment: %v", err)
	}

	// The create workflow returns the deployment in pending/CREATING state.
	if created.GetState() != pb.Deployment_STATE_CREATING {
		t.Errorf("state = %v, want STATE_CREATING", created.GetState())
	}
	if !created.GetReconciling() {
		t.Error("reconciling = false, want true for CREATING state")
	}
}
