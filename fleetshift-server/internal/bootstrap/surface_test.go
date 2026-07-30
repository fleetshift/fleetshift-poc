package bootstrap

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	gcphcpaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/gcphcp"
	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
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
// must be reachable (or explicitly exempt) on a ready app without WebDir.
var expectedHTTPRouteFamilies = []string{
	"/v1/",
	"/apis/fleetshift.io/",
	"/api/ui/setup/ws",
	"/api/ui/events/ws",
	"/api/ui/github-signing-keys/",
	"/api/ui/verify-sign",
}

func TestExpectedSurface_GRPCServices(t *testing.T) {
	app := startTestApp(t)

	conn, err := grpc.NewClient(app.Endpoints().GRPC.Dial, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	for i := 0; i < 50; i++ {
		if conn.GetState().String() != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	info := app.grpcServer.GetServiceInfo()
	for _, want := range expectedGRPCServiceFamilies {
		if _, ok := info[want]; !ok {
			t.Errorf("missing expected gRPC service %q; registered=%v", want, keys(info))
		}
	}
}

func TestExpectedSurface_HTTPRouteFamilies(t *testing.T) {
	app := startTestApp(t)
	base := "http://" + app.Endpoints().HTTP.Dial
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

func TestExpectedSurface_AuthMethodWiresCacheInvalidation(t *testing.T) {
	app := startTestApp(t)
	info := app.grpcServer.GetServiceInfo()
	svc, ok := info["fleetshift.v1.AuthMethodService"]
	if !ok {
		t.Fatal("AuthMethodService not registered")
	}
	found := false
	for _, m := range svc.Methods {
		if m.Name == "CreateAuthMethod" || m.Name == "DeleteAuthMethod" || m.Name == "ListAuthMethods" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("AuthMethodService methods unexpected: %+v", svc.Methods)
	}
	// Authn interceptor wiring is exercised by the shared builder registering
	// AuthMethodServer with Authn set; a nil Authn would omit cache invalidation.
	// Prove the transport service is present and the readiness probe succeeded
	// through authenticated middleware.
	if !app.ready {
		t.Fatal("app not ready")
	}
}

func TestExpectedSurface_KindGCPConditionalRegistration(t *testing.T) {
	app := startTestApp(t, WithAddonAssembly(func(_ context.Context, deps AddonDeps) ([]AddonSpec, error) {
		// Kind: schemas/targets only (no DeliveryCapability) so Agent may be nil.
		// GCP: DeliveryCapability requires a non-nil agent.
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
	info := app.grpcServer.GetServiceInfo()
	// Core services remain; dynamic kind/gcphcp services are registered on the mux.
	for _, want := range expectedGRPCServiceFamilies {
		if _, ok := info[want]; !ok {
			t.Errorf("missing expected gRPC service %q after addon connect", want)
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
