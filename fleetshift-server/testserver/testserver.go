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

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	gcphcpaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/gcphcp"
	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/testharness"
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

	// Build gRPC infrastructure first (needed for DynamicSchemaActivator)
	specValidator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("protovalidate.New: %v", err)
	}

	dynamicMux := managedresource.NewDynamicServiceMux()
	fileRegistry := managedresource.NewDynamicFileRegistry()

	// Create DynamicSchemaActivator with placeholder deps (will be wired after harness creation)
	activator := &managedresource.DynamicSchemaActivator{
		GRPCMux:      dynamicMux,
		FileRegistry: fileRegistry,
		Deps: managedresource.Deps{
			Validator: specValidator,
			// Resources will be wired after harness creation
		},
	}

	// Create the harness with all platform services
	h := testharness.New(t, testharness.WithSchemaActivator(activator))

	// Wire the activator's Resources dependency now that harness is created
	activator.Deps.Resources = h.ManagedResources

	// Register recording delivery for test and gcphcp targets
	recording := &sqlite.RecordingDeliveryService{Store: h.Store}
	recording.Reporter = h.Reporter
	h.Router.Register("test", recording)
	h.Router.Register(gcphcpaddon.TargetType, recording)

	// Create DB handle for auth methods (harness uses Store but we need DB for auth)
	// We can get it from the Store since it's a sqlite.Store
	store := h.Store.(*sqlite.Store)
	authMethodRepo := &sqlite.AuthMethodRepo{DB: store.DB}

	// Register ProvisionIdP workflow (testserver-specific, for OIDC auth)
	provSpec := &domain.ProvisionIdPWorkflowSpec{
		AuthMethods:      authMethodRepo,
		Discovery:        stubDiscovery{},
		CreateDeployment: h.Deployments.CreateWF,
	}
	trustBundleTargets := []domain.TargetID{"kind-local"}
	if len(trustBundleTargets) > 0 {
		provSpec.TrustBundlePlacement = domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: trustBundleTargets,
		}
	}
	provWf, err := h.Registry.RegisterProvisionIdP(provSpec)
	if err != nil {
		t.Fatalf("RegisterProvisionIdP: %v", err)
	}

	authMethodSvc := &application.AuthMethodService{
		Methods:     authMethodRepo,
		ProvisionWF: provWf,
	}
	authnInterceptor := transportgrpc.NewAuthnInterceptor(authMethodSvc, stubVerifier{}, domain.NoOpAuthnObserver{})

	// Create gRPC server with dynamic service handling
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(authnInterceptor.Unary()),
		grpc.ChainStreamInterceptor(authnInterceptor.Stream()),
		grpc.UnknownServiceHandler(dynamicMux.Handle),
	)
	pb.RegisterDeploymentServiceServer(srv, &transportgrpc.DeploymentServer{
		Deployments: h.Deployments,
	})
	pb.RegisterAuthMethodServiceServer(srv, &transportgrpc.AuthMethodServer{
		AuthMethods: authMethodSvc,
	})
	managedresource.RegisterCompositeReflection(srv, dynamicMux, fileRegistry)

	// Use the AddonManager lifecycle (Enable → Connect) to match
	// production wiring in serve.go. This registers targets, creates
	// managed resource type definitions, and activates schemas.
	ctx := context.Background()
	if err := h.AddonMgr.Enable(ctx, kindaddon.Descriptor()); err != nil {
		t.Fatalf("enable kind addon: %v", err)
	}

	schema := kindaddon.Schema()
	if err := h.AddonMgr.Connect(ctx, "kind", application.ConnectInput{
		Targets: []domain.TargetInfo{domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
			ID:                    "kind-local",
			Type:                  kindaddon.TargetType,
			Name:                  "Local Kind Provider",
			AcceptedResourceTypes: []domain.ResourceType{kindaddon.ClusterResourceType},
		})},
		Schemas: []domain.ManagedResourceSchema{schema},
	}); err != nil {
		t.Fatalf("connect kind addon: %v", err)
	}

	if err := h.AddonMgr.Enable(ctx, gcphcpaddon.Descriptor()); err != nil {
		t.Fatalf("enable gcphcp addon: %v", err)
	}

	gcpSchema := gcphcpaddon.Schema("gcphcp-test")
	if err := h.AddonMgr.Connect(ctx, "gcphcp", application.ConnectInput{
		Targets: []domain.TargetInfo{domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
			ID:                    "gcphcp-test",
			Type:                  gcphcpaddon.TargetType,
			Name:                  "Test GCP HCP Provider",
			AcceptedResourceTypes: []domain.ResourceType{gcphcpaddon.ClusterResourceType},
		})},
		Schemas: []domain.ManagedResourceSchema{gcpSchema},
	}); err != nil {
		t.Fatalf("connect gcphcp addon: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	go srv.Serve(lis)
	t.Cleanup(func() { srv.GracefulStop() })

	return lis.Addr().String()
}
