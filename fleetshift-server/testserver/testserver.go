// Package testserver provides a fully wired in-process FleetShift gRPC
// server for integration testing. The server uses SQLite in-memory storage
// and the synchronous workflow engine, making tests fast and deterministic.
package testserver

import (
	"net"
	"testing"

	"google.golang.org/grpc"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/syncworkflow"
	transportgrpc "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/grpc"
)

// Start launches an in-process gRPC server and returns its address.
// The server is stopped automatically when the test finishes.
func Start(t *testing.T) string {
	t.Helper()

	db := sqlite.OpenTestDB(t)

	targetRepo := &sqlite.TargetRepo{DB: db}
	deploymentRepo := &sqlite.DeploymentRepo{DB: db}
	deliveryRepo := &sqlite.DeliveryRepo{DB: db}

	router := delivery.NewRoutingDeliveryService()
	recording := &sqlite.RecordingDeliveryService{Deliveries: deliveryRepo}
	router.Register("test", recording)

	owf := &domain.OrchestrationWorkflow{
		Deployments: deploymentRepo,
		Targets:     targetRepo,
		Deliveries:  deliveryRepo,
		Delivery:    router,
		Strategies:  domain.DefaultStrategyFactory{},
	}
	cwf := &domain.CreateDeploymentWorkflow{
		Deployments: deploymentRepo,
	}

	engine := &syncworkflow.Engine{}
	runners, err := engine.Register(owf, cwf)
	if err != nil {
		t.Fatalf("register workflows: %v", err)
	}
	deploymentSvc := &application.DeploymentService{
		Deployments: deploymentRepo,
		Deliveries:  deliveryRepo,
		CreateWF:    runners.CreateDeployment,
	}

	srv := grpc.NewServer()
	pb.RegisterDeploymentServiceServer(srv, &transportgrpc.DeploymentServer{
		Deployments: deploymentSvc,
	})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	go srv.Serve(lis)
	t.Cleanup(func() { srv.GracefulStop() })

	return lis.Addr().String()
}
