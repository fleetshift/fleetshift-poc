package bootstrap

import (
	"context"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gcphcpaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/gcphcp"
	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc/oidctest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
	"google.golang.org/grpc"
)

// expectedGRPCServiceFamilies is the independent inventory of required gRPC
// service registrations. Introspection alone cannot detect a missing expectation.
var expectedGRPCServiceFamilies = []string{
	"fleetshift.v1.DeploymentService",
	"fleetshift.v1.AuthMethodService",
	"fleetshift.v1.SignerEnrollmentService",
	"fleetshift.v1.ResourceQueryService",
	"grpc.reflection.v1.ServerReflection",
}

// expectedHTTPRouteFamilies lists required HTTP route prefixes/patterns that
// must be reachable (or explicitly exempt) on a ready server without WebDir.
var expectedHTTPRouteFamilies = []string{
	"/v1/",
	"/apis/fleetshift.io/",
	"/api/ui/setup/ws",
	"/api/ui/events/ws",
	"/api/ui/github-signing-keys/",
	"/api/ui/verify-sign",
}

func TestExpectedSurface_GRPCServices(t *testing.T) {
	srv := startTestServer(t)
	info := srv.grpcServer.GetServiceInfo()
	for _, want := range expectedGRPCServiceFamilies {
		if _, ok := info[want]; !ok {
			t.Errorf("missing expected gRPC service %q; registered=%v", want, keys(info))
		}
	}
}

func TestExpectedSurface_HTTPRouteFamilies(t *testing.T) {
	srv := startTestServer(t)
	base := "http://" + srv.Endpoints().HTTP.Dial
	client := &http.Client{Timeout: 2 * time.Second}

	probes := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/v1/deployments"},
		{http.MethodGet, "/apis/fleetshift.io/v1/-:queryResources"},
		{http.MethodGet, "/api/ui/setup/ws"},
		{http.MethodGet, "/api/ui/events/ws"},
		{http.MethodGet, "/api/ui/github-signing-keys/octocat"},
		{http.MethodPost, "/api/ui/verify-sign"},
	}

	for _, p := range probes {
		req, err := http.NewRequest(p.method, base+p.path, strings.NewReader(`{}`))
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		if p.method == http.MethodPost {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", p.method, p.path, err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			t.Errorf("%s %s returned 404; route family missing", p.method, p.path)
		}
	}

	// Keep the independent expectation list aligned with probes above.
	for _, family := range expectedHTTPRouteFamilies {
		matched := false
		for _, p := range probes {
			if strings.HasPrefix(p.path, family) {
				matched = true
				break
			}
		}
		if !matched {
			t.Errorf("expected HTTP family %q has no probe", family)
		}
	}
}

func TestExpectedSurface_AuthExemptionsWithConfiguredOIDC(t *testing.T) {
	// D18 composition guardrail: with an auth method present, exempt WS routes
	// stay reachable while wrapped UI routes require a bearer token.
	idp := oidctest.Start(t, oidctest.WithAudience("fleetshift"))
	dbPath := filepath.Join(t.TempDir(), "fleetshift.db")
	seedOIDCAuthMethod(t, dbPath, idp.OIDCConfig())

	srv := startTestServerWithConfig(t, ConfigInput{
		DBPath: dbPath,
	})
	base := "http://" + srv.Endpoints().HTTP.Dial
	client := &http.Client{Timeout: 2 * time.Second}

	for _, path := range []string{"/api/ui/setup/ws", "/api/ui/events/ws"} {
		resp, err := client.Get(base + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusUnauthorized {
			t.Errorf("%s returned 401; route must remain unauthenticated", path)
		}
		if resp.StatusCode == http.StatusNotFound {
			t.Errorf("%s returned 404; route family missing", path)
		}
	}

	resp, err := client.Get(base + "/api/ui/github-signing-keys/octocat")
	if err != nil {
		t.Fatalf("GET github-signing-keys: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("github-signing-keys status = %d, want 401 with auth configured and no token", resp.StatusCode)
	}
}

func TestExpectedSurface_KindGCPTargetsAndSchemasLive(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "fleetshift.db")
	srv := startTestServerWithConfig(t, ConfigInput{DBPath: dbPath}, WithAddonAssembly(func(_ context.Context, deps AddonDeps) ([]AddonSpec, error) {
		kindDesc := kindaddon.Descriptor()
		kindDesc.Capabilities = []domain.Capability{
			domain.ManagedResourceCapability{ResourceType: kindaddon.ClusterResourceType},
			domain.InventoryResourceCapability{ResourceType: kindaddon.ClusterResourceType},
			domain.InventoryResourceCapability{ResourceType: kindaddon.NodeResourceType},
		}
		recording := &sqlite.RecordingDeliveryService{Store: deps.Store}
		return []AddonSpec{
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
						ID:                    "gcphcp-test",
						Type:                  gcphcpaddon.TargetType,
						Name:                  "Test GCP HCP Provider",
						AcceptedManifestTypes: []domain.ManifestType{gcphcpaddon.ClusterManifestType},
					})},
					Schemas: []domain.ExtensionResourceSchema{gcphcpaddon.Schema("gcphcp-test")},
				},
			},
		}, nil
	}))
	_ = srv

	db, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := &sqlite.Store{DB: db}

	ctx := context.Background()
	tx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()

	targets, err := tx.Targets().List(ctx)
	if err != nil {
		t.Fatalf("list targets: %v", err)
	}
	wantTargets := map[domain.TargetID]bool{"kind-local": false, "gcphcp-test": false}
	for _, target := range targets {
		if _, ok := wantTargets[target.ID()]; ok {
			wantTargets[target.ID()] = true
		}
	}
	for id, found := range wantTargets {
		if !found {
			t.Errorf("missing connected target %q", id)
		}
	}

	types, err := tx.ExtensionResources().ListTypes(ctx)
	if err != nil {
		t.Fatalf("list types: %v", err)
	}
	wantTypes := map[domain.ResourceType]bool{
		kindaddon.ClusterResourceType:   false,
		kindaddon.NodeResourceType:      false,
		gcphcpaddon.ClusterResourceType: false,
	}
	for _, typ := range types {
		if _, ok := wantTypes[typ.ResourceType()]; ok {
			wantTypes[typ.ResourceType()] = true
		}
	}
	for rt, found := range wantTypes {
		if !found {
			t.Errorf("missing activated schema type %q", rt)
		}
	}
}

func keys(m map[string]grpc.ServiceInfo) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
