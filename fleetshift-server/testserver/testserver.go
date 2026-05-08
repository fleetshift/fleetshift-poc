// Package testserver provides a fully wired in-process FleetShift gRPC
// server for integration testing. The server uses SQLite in-memory storage
// and the in-memory workflow engine, making tests fast and deterministic.
package testserver

import (
	"context"
	"net"
	"testing"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/reflect/protoreflect"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/memworkflow"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
	transportgrpc "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/grpc"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/managedresource"
)

// stubVerifier returns a fixed test identity for any token.
type stubVerifier struct{}

func (stubVerifier) Verify(_ context.Context, _ domain.OIDCConfig, _ string) (domain.SubjectClaims, error) {
	return domain.SubjectClaims{
		FederatedIdentity: domain.FederatedIdentity{
			Subject: "test-user",
			Issuer:  "test-issuer",
		},
	}, nil
}

// stubDiscovery returns fixed test metadata.
type stubDiscovery struct{}

func (stubDiscovery) FetchMetadata(_ context.Context, issuerURL domain.IssuerURL) (domain.OIDCMetadata, error) {
	return domain.OIDCMetadata{
		Issuer:                issuerURL,
		AuthorizationEndpoint: domain.EndpointURL(string(issuerURL) + "/authorize"),
		TokenEndpoint:         domain.EndpointURL(string(issuerURL) + "/token"),
		JWKSURI:               domain.EndpointURL(string(issuerURL) + "/jwks"),
	}, nil
}

// Start launches an in-process gRPC server and returns its address.
// The server is stopped automatically when the test finishes.
func Start(t *testing.T) string {
	t.Helper()

	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	router := delivery.NewRoutingDeliveryService()
	recording := &sqlite.RecordingDeliveryService{Store: store}
	router.Register("test", recording)

	reg := &memworkflow.Registry{}

	orchSpec := &domain.OrchestrationWorkflowSpec{
		Store:      store,
		Delivery:   router,
		Strategies: domain.StrategyFactory{Store: store},
		Registry:   reg,
	}
	orchWf, err := reg.RegisterOrchestration(orchSpec)
	if err != nil {
		t.Fatalf("RegisterOrchestration: %v", err)
	}

	cwfSpec := &domain.CreateDeploymentWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
	}
	createWf, err := reg.RegisterCreateDeployment(cwfSpec)
	if err != nil {
		t.Fatalf("RegisterCreateDeployment: %v", err)
	}

	provSpec := &domain.ProvisionIdPWorkflowSpec{
		AuthMethods:      &sqlite.AuthMethodRepo{DB: db},
		Discovery:        stubDiscovery{},
		CreateDeployment: createWf,
		TrustBundlePlacement: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"kind-local"},
		},
	}
	provWf, err := reg.RegisterProvisionIdP(provSpec)
	if err != nil {
		t.Fatalf("RegisterProvisionIdP: %v", err)
	}

	cleanupSpec := &domain.DeleteDeploymentCleanupWorkflowSpec{Store: store}
	cleanupWf, err := reg.RegisterDeleteDeploymentCleanup(cleanupSpec)
	if err != nil {
		t.Fatalf("RegisterDeleteDeploymentCleanup: %v", err)
	}

	deleteSpec := &domain.DeleteDeploymentWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
		Cleanup:       cleanupWf,
	}
	deleteWf, err := reg.RegisterDeleteDeployment(deleteSpec)
	if err != nil {
		t.Fatalf("RegisterDeleteDeployment: %v", err)
	}

	resumeSpec := &domain.ResumeDeploymentWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
	}
	resumeWf, err := reg.RegisterResumeDeployment(resumeSpec)
	if err != nil {
		t.Fatalf("RegisterResumeDeployment: %v", err)
	}

	createMRSpec := &domain.CreateManagedResourceWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
	}
	createMRWf, err := reg.RegisterCreateManagedResource(createMRSpec)
	if err != nil {
		t.Fatalf("RegisterCreateManagedResource: %v", err)
	}

	mrCleanupSpec := &domain.DeleteManagedResourceCleanupWorkflowSpec{Store: store}
	mrCleanupWf, err := reg.RegisterDeleteManagedResourceCleanup(mrCleanupSpec)
	if err != nil {
		t.Fatalf("RegisterDeleteManagedResourceCleanup: %v", err)
	}

	deleteMRSpec := &domain.DeleteManagedResourceWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
		Cleanup:       mrCleanupWf,
	}
	deleteMRWf, err := reg.RegisterDeleteManagedResource(deleteMRSpec)
	if err != nil {
		t.Fatalf("RegisterDeleteManagedResource: %v", err)
	}

	deploymentSvc := &application.DeploymentService{
		Store:    store,
		CreateWF: createWf,
		DeleteWF: deleteWf,
		ResumeWF: resumeWf,
	}

	managedResourceSvc := &application.ManagedResourceService{
		Store:    store,
		CreateWF: createMRWf,
		DeleteWF: deleteMRWf,
	}

	specValidator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("protovalidate.New: %v", err)
	}

	authMethodRepo := &sqlite.AuthMethodRepo{DB: db}
	authMethodSvc := &application.AuthMethodService{
		Methods:     authMethodRepo,
		ProvisionWF: provWf,
	}
	authnInterceptor := transportgrpc.NewAuthnInterceptor(authMethodSvc, stubVerifier{}, domain.NoOpAuthnObserver{})

	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(authnInterceptor.Unary()),
		grpc.ChainStreamInterceptor(authnInterceptor.Stream()),
	)
	pb.RegisterDeploymentServiceServer(srv, &transportgrpc.DeploymentServer{
		Deployments: deploymentSvc,
	})
	pb.RegisterAuthMethodServiceServer(srv, &transportgrpc.AuthMethodServer{
		AuthMethods: authMethodSvc,
	})
	schema := kindaddon.Schema()
	if schema.EntryFile == "" {
		if len(schema.ProtoFiles) != 1 {
			t.Fatalf("expected exactly 1 cluster schema proto, got %d (or set EntryFile)", len(schema.ProtoFiles))
		}
		for name := range schema.ProtoFiles {
			schema.EntryFile = name
		}
	}
	clusterSpecDesc, err := managedresource.CompileInline(
		context.Background(),
		schema.ProtoFiles,
		schema.EntryFile,
		protoreflect.FullName(schema.SpecMessage),
	)
	if err != nil {
		t.Fatalf("compile cluster spec proto: %v", err)
	}
	clusterTypeCfg := &managedresource.ResourceTypeConfig{
		ResourceType:   kindaddon.ClusterResourceType,
		Singular:       schema.Singular,
		Plural:         schema.Plural,
		ProtoPackage:   "fleetshift.v1",
		SpecMessage:    schema.SpecMessage,
		SpecDescriptor: clusterSpecDesc.Message,
	}
	if _, err := managedresource.BuildAndRegister(srv, clusterTypeCfg, managedresource.Deps{
		Resources: managedResourceSvc,
		Validator: specValidator,
	}); err != nil {
		t.Fatalf("BuildAndRegister cluster: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	go srv.Serve(lis)
	t.Cleanup(func() { srv.GracefulStop() })

	return lis.Addr().String()
}
