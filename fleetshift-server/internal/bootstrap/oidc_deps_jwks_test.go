package bootstrap

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc/oidctest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

// gatedKeySetVerifier forces RegisterKeySet to fail instantly so startup's
// registerPersistedKeySets path is exercised without waiting on httprc.
type gatedKeySetVerifier struct {
	inner         *oidc.Verifier
	allowRegister atomic.Bool
}

func (v *gatedKeySetVerifier) RegisterKeySet(ctx context.Context, jwksURI domain.EndpointURL) error {
	if !v.allowRegister.Load() {
		return context.DeadlineExceeded
	}
	return v.inner.RegisterKeySet(ctx, jwksURI)
}

func (v *gatedKeySetVerifier) Verify(ctx context.Context, config domain.OIDCConfig, rawToken string) (domain.SubjectClaims, error) {
	return v.inner.Verify(ctx, config, rawToken)
}

func seedOIDCAuthMethod(t *testing.T, dbPath string, cfg domain.OIDCConfig) {
	t.Helper()
	db, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("open db for seed: %v", err)
	}
	defer db.Close()

	repo := &sqlite.AuthMethodRepo{DB: db}
	method := domain.NewOIDCAuthMethod("oidc-1", &cfg)
	if err := repo.Save(context.Background(), method); err != nil {
		t.Fatalf("seed auth method: %v", err)
	}
}

func TestStart_ContinuesWhenPersistedJWKSUnavailable(t *testing.T) {
	// Fast cement for bootstrap: registerPersistedKeySets failures must not
	// fail Start (warn-and-continue).
	idp := oidctest.Start(t, oidctest.WithAudience("fleetshift"))
	dbPath := filepath.Join(t.TempDir(), "fleetshift.db")
	seedOIDCAuthMethod(t, dbPath, idp.OIDCConfig())

	inner, err := oidc.NewVerifier(context.Background(), oidc.WithHTTPClient(idp.HTTPClient()))
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	gated := &gatedKeySetVerifier{inner: inner}
	gated.allowRegister.Store(false)

	cfg, err := NewConfig(ConfigInput{
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
		DBPath:   dbPath,
	})
	if err != nil {
		t.Fatalf("NewConfig: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, err := Start(ctx, cfg, testLogger(),
		WithWorkflowRegistry(NewMemWorkflowRegistry()),
		WithOIDCDeps(OIDCDeps{Discovery: testDiscovery{}, Verifier: gated}),
		WithAddonAssembly(func(context.Context, AddonDeps) ([]AddonSpec, error) {
			return nil, nil
		}),
	)
	if err != nil {
		t.Fatalf("Start must succeed when persisted JWKS registration fails: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = srv.Close(closeCtx)
	})
}

func TestStart_OnDemandJWKSRegistrationAfterIdPRecovery(t *testing.T) {
	// Production *oidc.Verifier against a JWKS URL that is down at boot and
	// up later. Uses a short HTTP timeout so a 503 cannot stall Start.
	idp := oidctest.Start(t, oidctest.WithAudience("fleetshift"))
	oidcCfg := idp.OIDCConfig()

	req, err := http.NewRequest(http.MethodGet, string(oidcCfg.JWKSURI), nil)
	if err != nil {
		t.Fatalf("jwks request: %v", err)
	}
	resp, err := idp.HTTPClient().Do(req)
	if err != nil {
		t.Fatalf("fetch idp JWKS: %v", err)
	}
	jwksBody, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read idp JWKS: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("idp JWKS status %d", resp.StatusCode)
	}

	var jwksUp atomic.Bool
	jwksProxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !jwksUp.Load() {
			http.Error(w, "jwks unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(jwksBody)
	}))
	t.Cleanup(jwksProxy.Close)

	cfgForSeed := oidcCfg
	cfgForSeed.JWKSURI = domain.EndpointURL(jwksProxy.URL + "/jwks")

	dbPath := filepath.Join(t.TempDir(), "fleetshift.db")
	seedOIDCAuthMethod(t, dbPath, cfgForSeed)

	// Short client timeout keeps boot-time RegisterKeySet from hanging when
	// the proxy returns 503 / when httprc waits on first successful fetch.
	verifier, err := oidc.NewVerifier(context.Background(), oidc.WithHTTPClient(&http.Client{
		Timeout: 500 * time.Millisecond,
	}))
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	appCfg, err := NewConfig(ConfigInput{
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
		DBPath:   dbPath,
	})
	if err != nil {
		t.Fatalf("NewConfig: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	jwksUp.Store(false)
	startAt := time.Now()
	srv, err := Start(ctx, appCfg, testLogger(),
		WithWorkflowRegistry(NewMemWorkflowRegistry()),
		WithOIDCDeps(OIDCDeps{Discovery: testDiscovery{}, Verifier: verifier}),
		WithAddonAssembly(func(context.Context, AddonDeps) ([]AddonSpec, error) {
			return nil, nil
		}),
	)
	if err != nil {
		t.Fatalf("Start with unavailable JWKS: %v", err)
	}
	// Whole Start includes DB/listeners/readiness, so keep a CI-tolerant
	// ceiling that still flags a non-fail-fast JWKS preflight (2s/method).
	if elapsed := time.Since(startAt); elapsed > 8*time.Second {
		t.Fatalf("Start took %v with JWKS down; want fail-fast warn-and-continue", elapsed)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = srv.Close(closeCtx)
	})

	conn, err := grpc.NewClient(srv.Endpoints().GRPC.Dial, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewDeploymentServiceClient(conn)

	token := idp.IssueToken(t, oidctest.TokenClaims{Subject: "user-1"})
	authed := func(parent context.Context) context.Context {
		return metadata.NewOutgoingContext(parent, metadata.Pairs("authorization", "Bearer "+token))
	}

	callCtx, callCancel := context.WithTimeout(authed(context.Background()), 5*time.Second)
	_, err = client.ListDeployments(callCtx, &pb.ListDeploymentsRequest{})
	callCancel()
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("while JWKS down: got %v (%v), want Unauthenticated", err, status.Code(err))
	}

	jwksUp.Store(true)
	callCtx, callCancel = context.WithTimeout(authed(context.Background()), 5*time.Second)
	defer callCancel()
	if _, err := client.ListDeployments(callCtx, &pb.ListDeploymentsRequest{}); err != nil {
		t.Fatalf("after JWKS recovery, authenticated ListDeployments: %v", err)
	}
}
