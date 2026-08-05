// Package testserver is a frozen fixture for existing fleetshift-cli
// unit/contract tests that call Start. Do not add new callers, prefer
// adding new e2e tests.
package testserver

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	gcphcpaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/gcphcp"
	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/bootstrap"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

// stubVerifier returns a fixed test identity for any token.
type stubVerifier struct{}

// Verify implements domain.OIDCTokenVerifier with a fixed test subject.
func (stubVerifier) Verify(_ context.Context, _ domain.OIDCConfig, _ string) (domain.SubjectClaims, error) {
	return domain.SubjectClaims{
		FederatedIdentity: domain.FederatedIdentity{
			Subject: "test-user",
			Issuer:  "test-issuer",
		},
	}, nil
}

// RegisterKeySet implements bootstrap.KeySetRegistrar as a no-op.
func (stubVerifier) RegisterKeySet(context.Context, domain.EndpointURL) error { return nil }

// stubDiscovery returns fixed test metadata.
type stubDiscovery struct{}

// FetchMetadata implements domain.OIDCDiscoveryClient with endpoints derived
// from the requested issuer URL.
func (stubDiscovery) FetchMetadata(_ context.Context, issuerURL domain.IssuerURL) (domain.OIDCMetadata, error) {
	return domain.OIDCMetadata{
		Issuer:                issuerURL,
		AuthorizationEndpoint: domain.EndpointURL(string(issuerURL) + "/authorize"),
		TokenEndpoint:         domain.EndpointURL(string(issuerURL) + "/token"),
		JWKSURI:               domain.EndpointURL(string(issuerURL) + "/jwks"),
	}, nil
}

// Start launches an in-process FleetShift server via bootstrap and returns
// its gRPC dial address. The server is stopped when the test finishes.
func Start(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "fleetshift.db")

	cfg, err := bootstrap.NewConfig(bootstrap.ConfigInput{
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
		DBPath:   dbPath,
		// kind alone drives trust-bundle placement for provision-IdP;
		// gcphcp is assembled via WithAddonAssembly without requiring a
		// production gcphcp config file.
		Addons: "kind",
	})
	if err != nil {
		t.Fatalf("NewConfig: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, err := bootstrap.Start(ctx, cfg, logger,
		bootstrap.WithWorkflowRegistry(bootstrap.NewMemWorkflowRegistry()),
		bootstrap.WithOIDCDeps(bootstrap.OIDCDeps{
			Discovery: stubDiscovery{},
			Verifier:  stubVerifier{},
		}),
		bootstrap.WithAddonAssembly(testAddonAssembly),
	)
	if err != nil {
		t.Fatalf("bootstrap.Start: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := srv.Close(closeCtx); err != nil {
			t.Errorf("bootstrap.Close: %v", err)
		}
	})

	return srv.Endpoints().GRPC.Dial
}

// testAddonAssembly preserves focused Kind/GCP HCP create/read semantics
// for sibling CLI tests.
//
// Kind omits DeliveryCapability so Connect.Agent may be nil: schemas/targets
// stay live, resources remain CREATING, and delete does not wait on delivery.
// GCP HCP keeps DeliveryCapability with a recording agent and no Reporter so
// the target type is routable while deliveries stay incomplete.
func testAddonAssembly(_ context.Context, deps bootstrap.AddonDeps) ([]bootstrap.AddonSpec, error) {
	// No Reporter: deliveries stay incomplete so create/read tests observe CREATING.
	recording := &sqlite.RecordingDeliveryService{Store: deps.Store}

	kindDesc := kindaddon.Descriptor()
	kindDesc.Capabilities = []domain.Capability{
		domain.ManagedResourceCapability{ResourceType: kindaddon.ClusterResourceType},
		domain.InventoryResourceCapability{ResourceType: kindaddon.ClusterResourceType},
		domain.InventoryResourceCapability{ResourceType: kindaddon.NodeResourceType},
	}

	return []bootstrap.AddonSpec{
		{
			Descriptor: kindDesc,
			Connect: application.ConnectInput{
				Targets: []domain.TargetInfo{domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
					ID:   "kind-local",
					Type: kindaddon.TargetType,
					Name: "Local Kind Provider",
					AcceptedManifestTypes: []domain.ManifestType{
						kindaddon.ClusterManifestType,
						kindaddon.ManagedClusterManifestType,
					},
				})},
				Schemas: []domain.ExtensionResourceSchema{kindaddon.Schema(), kindaddon.NodeSchema()},
			},
		},
		{
			Descriptor: gcphcpaddon.Descriptor(),
			Connect: application.ConnectInput{
				Agent: recording,
				Targets: []domain.TargetInfo{domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
					ID:   "gcphcp-test",
					Type: gcphcpaddon.TargetType,
					Name: "Test GCP HCP Provider",
					AcceptedManifestTypes: []domain.ManifestType{
						gcphcpaddon.ClusterManifestType,
					},
				})},
				Schemas: []domain.ExtensionResourceSchema{gcphcpaddon.Schema("gcphcp-test")},
			},
		},
	}, nil
}
