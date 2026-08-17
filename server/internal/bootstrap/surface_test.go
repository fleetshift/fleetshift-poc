package bootstrap

import (
	"context"
	"io"
	"net/http"
	"os"
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
	"/livez",
	"/readyz",
	"/api/ui/config",
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
		{http.MethodGet, "/livez"},
		{http.MethodGet, "/readyz"},
		{http.MethodGet, "/api/ui/config"},
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
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			t.Errorf("%s %s returned 404; route family missing", p.method, p.path)
		}
		switch p.path {
		case "/livez", "/readyz":
			if resp.StatusCode != http.StatusOK {
				t.Errorf("%s status = %d, want 200", p.path, resp.StatusCode)
			}
			if string(body) != "ok" {
				t.Errorf("%s body = %q, want ok", p.path, body)
			}
		case "/api/ui/config":
			if resp.StatusCode != http.StatusOK {
				t.Errorf("/api/ui/config status = %d, want 200", resp.StatusCode)
			}
			bodyStr := string(body)
			if !strings.Contains(bodyStr, `"oidc"`) {
				t.Errorf("/api/ui/config body missing oidc: %s", bodyStr)
			}
			if !strings.Contains(bodyStr, `"authConfigured"`) {
				t.Errorf("/api/ui/config body missing authConfigured: %s", bodyStr)
			}
			if !strings.Contains(bodyStr, `"uiOrigin"`) {
				t.Errorf("/api/ui/config body missing uiOrigin: %s", bodyStr)
			}
			if !strings.Contains(bodyStr, `"authorizationEndpoint"`) {
				t.Errorf("/api/ui/config body missing authorizationEndpoint: %s", bodyStr)
			}
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
	// With an auth method present, exempt WS routes stay reachable while
	// wrapped UI routes require a bearer token.
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

func TestUIHTTP_AppPrefix(t *testing.T) {
	webDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(webDir, "index.html"), []byte("<html>app</html>"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(webDir, "plugin-registry.json"), []byte(`{"plugins":{}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	srv := startTestServerWithConfig(t, ConfigInput{WebDir: webDir})
	base := "http://" + srv.Endpoints().HTTP.Dial
	client := &http.Client{
		Timeout: 2 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	for _, path := range []string{"/", "/app"} {
		resp, err := client.Get(base + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusFound {
			t.Errorf("GET %s status = %d, want %d body %s", path, resp.StatusCode, http.StatusFound, body)
		}
		if loc := resp.Header.Get("Location"); loc != "/app/" {
			t.Errorf("GET %s Location = %q, want /app/", path, loc)
		}
	}

	req, err := http.NewRequest(http.MethodGet, base+"/app/", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "text/html")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /app/ status = %d, want 200", resp.StatusCode)
	}
	if !strings.Contains(string(body), "app") {
		t.Errorf("GET /app/ body = %q", body)
	}
}

func keys(m map[string]grpc.ServiceInfo) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
