package bootstrap

import (
	"context"
	"encoding/base64"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc/oidctest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

func TestLifecycle_DynamicEndpointsDialable(t *testing.T) {
	srv := startTestServer(t)
	ep := srv.Endpoints()
	if ep.GRPC.Dial == "" || ep.HTTP.Dial == "" {
		t.Fatalf("empty endpoints: %+v", ep)
	}
	if ep.GRPC.Dial == "127.0.0.1:0" || ep.HTTP.Dial == "127.0.0.1:0" {
		t.Fatalf("endpoints not resolved: %+v", ep)
	}

	conn, err := grpc.NewClient(ep.GRPC.Dial, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = pb.NewDeploymentServiceClient(conn).ListDeployments(ctx, &pb.ListDeploymentsRequest{})
	// AuthMethod exists after initial AuthMethod install; unauthenticated calls must still
	// reach the service (Unauthenticated), proving the dial target works.
	if err == nil {
		t.Fatal("expected Unauthenticated without token after initial AuthMethod install")
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("ListDeployments via resolved dial: %v (want Unauthenticated)", err)
	}

	resp, err := http.Get("http://" + ep.HTTP.Dial + "/v1/deployments")
	if err != nil {
		t.Fatalf("http get via gateway: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		t.Fatal("gateway /v1/deployments not found; dial target mismatch?")
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("gateway status = %d, want 401 after initial AuthMethod install", resp.StatusCode)
	}
}

func TestLifecycle_GatewaySurvivesStartContextCancel(t *testing.T) {
	// HandlerFromEndpoint closed its ClientConn when the start ctx ended
	// (testserver.Start's defer cancel). Shared Close-owned conn must remain.
	startCtx, startCancel := context.WithCancel(context.Background())
	cfg, err := NewConfig(ConfigInput{
		GRPCAddr:             "127.0.0.1:0",
		HTTPAddr:             "127.0.0.1:0",
		DBPath:               filepath.Join(t.TempDir(), "fleetshift.db"),
		OIDCIssuer:           "https://test-issuer.example",
		OIDCResourceAudience: "fleetshift",
	})
	if err != nil {
		t.Fatalf("NewConfig: %v", err)
	}
	srv, err := Start(startCtx, cfg, testLogger(),
		WithWorkflowRegistry(NewMemWorkflowRegistry()),
		WithOIDCDeps(OIDCDeps{Discovery: testDiscovery{}, Verifier: testVerifier{}}),
		WithAddonAssembly(func(context.Context, AddonDeps) ([]AddonSpec, error) { return nil, nil }),
	)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = srv.Close(closeCtx)
	})

	startCancel()

	resp, err := http.Get("http://" + srv.Endpoints().HTTP.Dial + "/v1/deployments")
	if err != nil {
		t.Fatalf("gateway after start ctx cancel: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		t.Fatal("gateway /v1/deployments missing after start ctx cancel")
	}
	if resp.StatusCode >= 500 {
		t.Fatalf("gateway status %d after start ctx cancel; loopback conn likely closed", resp.StatusCode)
	}
}

func TestLifecycle_SecondListenerFailureUnwinds(t *testing.T) {
	// Hold HTTP so Start's second net.Listen fails after gRPC bind succeeds.
	heldHTTP, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("hold HTTP addr: %v", err)
	}
	t.Cleanup(func() { _ = heldHTTP.Close() })

	grpcAddr := freeLocalAddr(t)
	cfg, err := NewConfig(ConfigInput{
		GRPCAddr:             grpcAddr,
		HTTPAddr:             heldHTTP.Addr().String(),
		DBPath:               filepath.Join(t.TempDir(), "fleetshift.db"),
		OIDCIssuer:           "https://test-issuer.example",
		OIDCResourceAudience: "fleetshift",
	})
	if err != nil {
		t.Fatalf("NewConfig: %v", err)
	}

	_, err = Start(context.Background(), cfg, testLogger(),
		WithWorkflowRegistry(NewMemWorkflowRegistry()),
		WithOIDCDeps(OIDCDeps{Discovery: testDiscovery{}, Verifier: testVerifier{}}),
		WithAddonAssembly(func(context.Context, AddonDeps) ([]AddonSpec, error) { return nil, nil }),
	)
	if err == nil {
		t.Fatal("expected HTTP listen failure")
	}

	// fail() must release the gRPC listener acquired before the HTTP failure.
	rebind, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		t.Fatalf("gRPC addr %s still held after failed Start: %v", grpcAddr, err)
	}
	_ = rebind.Close()
}

func TestLifecycle_ConnectFailureUnwinds(t *testing.T) {
	grpcAddr := freeLocalAddr(t)
	httpAddr := freeLocalAddr(t)
	dbPath := filepath.Join(t.TempDir(), "fleetshift.db")

	cfg, err := NewConfig(ConfigInput{
		GRPCAddr:             grpcAddr,
		HTTPAddr:             httpAddr,
		DBPath:               dbPath,
		OIDCIssuer:           "https://test-issuer.example",
		OIDCResourceAudience: "fleetshift",
	})
	if err != nil {
		t.Fatalf("NewConfig: %v", err)
	}

	_, err = Start(context.Background(), cfg, testLogger(),
		WithWorkflowRegistry(NewMemWorkflowRegistry()),
		WithOIDCDeps(OIDCDeps{Discovery: testDiscovery{}, Verifier: testVerifier{}}),
		WithAddonAssembly(func(context.Context, AddonDeps) ([]AddonSpec, error) {
			return []AddonSpec{{
				Descriptor: domain.AddonDescriptor{ID: "noop", Name: "noop"},
				Connect:    application.ConnectInput{},
				AfterConnect: func(context.Context) error {
					return errors.New("injected connect failure")
				},
			}}, nil
		}),
	)
	if err == nil {
		t.Fatal("expected AfterConnect failure")
	}

	rebindGRPC, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		t.Fatalf("gRPC addr %s still held after failed Start: %v", grpcAddr, err)
	}
	_ = rebindGRPC.Close()
	rebindHTTP, err := net.Listen("tcp", httpAddr)
	if err != nil {
		t.Fatalf("HTTP addr %s still held after failed Start: %v", httpAddr, err)
	}
	_ = rebindHTTP.Close()
}

func TestLifecycle_NilClaimedDeliveryAgentRejected(t *testing.T) {
	cfg, err := NewConfig(ConfigInput{
		GRPCAddr:             "127.0.0.1:0",
		HTTPAddr:             "127.0.0.1:0",
		DBPath:               filepath.Join(t.TempDir(), "fleetshift.db"),
		OIDCIssuer:           "https://test-issuer.example",
		OIDCResourceAudience: "fleetshift",
	})
	if err != nil {
		t.Fatalf("NewConfig: %v", err)
	}

	_, err = Start(context.Background(), cfg, testLogger(),
		WithWorkflowRegistry(NewMemWorkflowRegistry()),
		WithOIDCDeps(OIDCDeps{Discovery: testDiscovery{}, Verifier: testVerifier{}}),
		WithAddonAssembly(func(context.Context, AddonDeps) ([]AddonSpec, error) {
			return []AddonSpec{{
				Descriptor: domain.AddonDescriptor{
					ID:           "nil-agent",
					Name:         "nil-agent",
					Capabilities: []domain.Capability{domain.DeliveryCapability{TargetType: "test"}},
				},
				Connect: application.ConnectInput{}, // Agent intentionally nil
			}}, nil
		}),
	)
	if err == nil || !strings.Contains(err.Error(), "Connect.Agent is nil") {
		t.Fatalf("Start error = %v, want Connect.Agent is nil", err)
	}
}

func TestStart_FailClosedEmptyStoreWithoutOIDC(t *testing.T) {
	cfg, err := NewConfig(ConfigInput{
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
		DBPath:   filepath.Join(t.TempDir(), "fleetshift.db"),
	})
	if err != nil {
		t.Fatalf("NewConfig: %v", err)
	}

	_, err = Start(context.Background(), cfg, testLogger(),
		WithWorkflowRegistry(NewMemWorkflowRegistry()),
		WithOIDCDeps(OIDCDeps{Discovery: testDiscovery{}, Verifier: testVerifier{}}),
		WithAddonAssembly(func(context.Context, AddonDeps) ([]AddonSpec, error) { return nil, nil }),
	)
	if err == nil {
		t.Fatal("expected Start to fail closed with empty AuthMethod store and no OIDC")
	}
	if !strings.Contains(err.Error(), "refusing to open the public API") {
		t.Fatalf("Start error = %v, want refuse public API", err)
	}
}

func TestStart_InstallsDefaultAuthMethodWhenStoreEmpty(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "fleetshift.db")
	cfg, err := NewConfig(ConfigInput{
		GRPCAddr:             "127.0.0.1:0",
		HTTPAddr:             "127.0.0.1:0",
		DBPath:               dbPath,
		OIDCIssuer:           "https://test-issuer.example",
		OIDCResourceAudience: "fleetshift",
	})
	if err != nil {
		t.Fatalf("NewConfig: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	srv, err := Start(ctx, cfg, testLogger(),
		WithWorkflowRegistry(NewMemWorkflowRegistry()),
		WithOIDCDeps(OIDCDeps{Discovery: testDiscovery{}, Verifier: testVerifier{}}),
		WithAddonAssembly(func(context.Context, AddonDeps) ([]AddonSpec, error) { return nil, nil }),
	)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer closeCancel()
	if err := srv.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	repo := &sqlite.AuthMethodRepo{DB: db}
	methods, err := repo.List(context.Background())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(methods) != 1 || methods[0].ID() != domain.DefaultAuthMethodID {
		t.Fatalf("methods = %+v, want default AuthMethod", methods)
	}
	oidc := methods[0].OIDC()
	if oidc == nil || string(oidc.IssuerURL) != "https://test-issuer.example" {
		t.Fatalf("issuer = %#v, want https://test-issuer.example", oidc)
	}
	if string(oidc.AuthorizationEndpoint) != "https://test-issuer.example/authorize" {
		t.Fatalf("AuthorizationEndpoint = %q, want .../authorize", oidc.AuthorizationEndpoint)
	}
	if string(oidc.TokenEndpoint) != "https://test-issuer.example/token" {
		t.Fatalf("TokenEndpoint = %q, want .../token", oidc.TokenEndpoint)
	}
	if string(oidc.JWKSURI) != "https://test-issuer.example/jwks" {
		t.Fatalf("JWKSURI = %q, want .../jwks", oidc.JWKSURI)
	}
}

func TestStart_SkipsBootstrapWhenAuthMethodExists(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "fleetshift.db")
	startOpts := []Option{
		WithWorkflowRegistry(NewMemWorkflowRegistry()),
		WithOIDCDeps(OIDCDeps{Discovery: testDiscovery{}, Verifier: testVerifier{}}),
		WithAddonAssembly(func(context.Context, AddonDeps) ([]AddonSpec, error) { return nil, nil }),
	}

	firstCfg, err := NewConfig(ConfigInput{
		GRPCAddr:             "127.0.0.1:0",
		HTTPAddr:             "127.0.0.1:0",
		DBPath:               dbPath,
		OIDCIssuer:           "https://first-issuer.example",
		OIDCResourceAudience: "fleetshift",
	})
	if err != nil {
		t.Fatalf("NewConfig (first): %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	first, err := Start(ctx, firstCfg, testLogger(), startOpts...)
	if err != nil {
		t.Fatalf("first Start: %v", err)
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer closeCancel()
	if err := first.Close(closeCtx); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	before := listAuthMethods(t, dbPath)
	if len(before) != 1 || before[0].ID() != domain.DefaultAuthMethodID {
		t.Fatalf("after first Start: methods = %+v, want default", before)
	}
	if got := before[0].OIDC(); got == nil || string(got.IssuerURL) != "https://first-issuer.example" {
		t.Fatalf("after first Start: issuer = %#v, want https://first-issuer.example", got)
	}

	// Different OIDC argv on restart must not reinstall or overwrite; persisted
	// AuthMethod remains the authority.
	secondCfg, err := NewConfig(ConfigInput{
		GRPCAddr:             "127.0.0.1:0",
		HTTPAddr:             "127.0.0.1:0",
		DBPath:               dbPath,
		OIDCIssuer:           "https://other-issuer.example",
		OIDCResourceAudience: "fleetshift",
	})
	if err != nil {
		t.Fatalf("NewConfig (second): %v", err)
	}
	second, err := Start(ctx, secondCfg, testLogger(), startOpts...)
	if err != nil {
		t.Fatalf("second Start: %v", err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer ccancel()
		_ = second.Close(cctx)
	})

	after := listAuthMethods(t, dbPath)
	if len(after) != 1 || after[0].ID() != domain.DefaultAuthMethodID {
		t.Fatalf("after second Start: methods = %+v, want single default", after)
	}
	if got := after[0].OIDC(); got == nil || string(got.IssuerURL) != "https://first-issuer.example" {
		t.Fatalf("after second Start: issuer = %#v, want persisted https://first-issuer.example", got)
	}
}

func TestStart_MiddlewareAcceptsOIDCAccessToken(t *testing.T) {
	idp := oidctest.Start(t, oidctest.WithAudience("fleetshift"))
	other := oidctest.Start(t, oidctest.WithAudience("fleetshift"))

	oidcDeps, err := NewProductionOIDCDeps(context.Background(), idp.HTTPClient())
	if err != nil {
		t.Fatalf("NewProductionOIDCDeps: %v", err)
	}

	cfg, err := NewConfig(ConfigInput{
		GRPCAddr:             "127.0.0.1:0",
		HTTPAddr:             "127.0.0.1:0",
		DBPath:               filepath.Join(t.TempDir(), "fleetshift.db"),
		OIDCIssuer:           string(idp.IssuerURL()),
		OIDCResourceAudience: "fleetshift",
		OIDCCABundle:         idp.CACertPEM(),
	})
	if err != nil {
		t.Fatalf("NewConfig: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	srv, err := Start(ctx, cfg, testLogger(),
		WithWorkflowRegistry(NewMemWorkflowRegistry()),
		WithOIDCDeps(oidcDeps),
		WithAddonAssembly(func(context.Context, AddonDeps) ([]AddonSpec, error) { return nil, nil }),
	)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer closeCancel()
		_ = srv.Close(closeCtx)
	})

	conn, err := grpc.NewClient(srv.Endpoints().GRPC.Dial, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewDeploymentServiceClient(conn)

	authed := func(token string) context.Context {
		return metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
	}
	list := func(t *testing.T, token string) error {
		t.Helper()
		callCtx, callCancel := context.WithTimeout(authed(token), 5*time.Second)
		defer callCancel()
		_, err := client.ListDeployments(callCtx, &pb.ListDeploymentsRequest{})
		return err
	}

	valid := idp.IssueToken(t, oidctest.TokenClaims{Subject: "user-1"})
	if err := list(t, valid); err != nil {
		t.Fatalf("valid token ListDeployments: %v", err)
	}

	t.Run("wrong_audience", func(t *testing.T) {
		tok := idp.IssueToken(t, oidctest.TokenClaims{Subject: "user-1", Audience: "wrong-audience"})
		err := list(t, tok)
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("got %v (%v), want Unauthenticated", err, status.Code(err))
		}
	})
	t.Run("wrong_issuer", func(t *testing.T) {
		tok := other.IssueToken(t, oidctest.TokenClaims{Subject: "user-1"})
		err := list(t, tok)
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("got %v (%v), want Unauthenticated", err, status.Code(err))
		}
	})
	t.Run("bad_signature", func(t *testing.T) {
		tok := valid
		parts := strings.Split(tok, ".")
		if len(parts) != 3 {
			t.Fatalf("token segments = %d, want 3", len(parts))
		}
		sig, err := base64.RawURLEncoding.DecodeString(parts[2])
		if err != nil {
			t.Fatalf("decode signature: %v", err)
		}
		if len(sig) == 0 {
			t.Fatal("empty signature")
		}
		sig[len(sig)-1] ^= 0x01
		parts[2] = base64.RawURLEncoding.EncodeToString(sig)
		err = list(t, strings.Join(parts, "."))
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("got %v (%v), want Unauthenticated", err, status.Code(err))
		}
	})
}

func listAuthMethods(t *testing.T, dbPath string) []domain.AuthMethod {
	t.Helper()
	db, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	methods, err := (&sqlite.AuthMethodRepo{DB: db}).List(context.Background())
	if err != nil {
		t.Fatalf("list auth methods: %v", err)
	}
	return methods
}

func TestStopIngress_ReleasesListenersAfterServe(t *testing.T) {
	// stopIngress is shared by Close and Start's post-Serve fail cleanup.
	// Exercise it against live Serve loops without a production test hook.
	grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen gRPC: %v", err)
	}
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen HTTP: %v", err)
	}
	grpcAddr := grpcLis.Addr().String()
	httpAddr := httpLis.Addr().String()

	grpcServer := grpc.NewServer()
	httpServer := &http.Server{Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})}

	serveDone := make(chan struct{}, 2)
	go func() {
		_ = grpcServer.Serve(grpcLis)
		serveDone <- struct{}{}
	}()
	go func() {
		_ = httpServer.Serve(httpLis)
		serveDone <- struct{}{}
	}()

	// Prove both listeners accepted connections before stop.
	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}
	conn.Connect()
	conn.Close()
	resp, err := http.Get("http://" + httpAddr + "/")
	if err != nil {
		t.Fatalf("http get: %v", err)
	}
	resp.Body.Close()

	if err := stopIngress(grpcServer, httpServer, 2*time.Second); err != nil {
		t.Fatalf("stopIngress: %v", err)
	}

	for range 2 {
		select {
		case <-serveDone:
		case <-time.After(2 * time.Second):
			t.Fatal("Serve goroutines did not exit after stopIngress")
		}
	}

	rebindGRPC, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		t.Fatalf("gRPC addr %s still held after stopIngress: %v", grpcAddr, err)
	}
	_ = rebindGRPC.Close()
	rebindHTTP, err := net.Listen("tcp", httpAddr)
	if err != nil {
		t.Fatalf("HTTP addr %s still held after stopIngress: %v", httpAddr, err)
	}
	_ = rebindHTTP.Close()

	// Second stop must remain safe for fail/Close overlap.
	if err := stopIngress(grpcServer, httpServer, 2*time.Second); err != nil {
		t.Fatalf("second stopIngress: %v", err)
	}
}

func TestStopIngress_GracefulFallbackForcesStop(t *testing.T) {
	grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen gRPC: %v", err)
	}
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen HTTP: %v", err)
	}
	grpcAddr := grpcLis.Addr().String()

	hold := make(chan struct{})
	handlerEntered := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-hold:
		default:
			close(hold)
		}
	})

	impl := &hangServer{hold: hold, entered: handlerEntered}
	grpcServer := grpc.NewServer()
	grpcServer.RegisterService(&grpc.ServiceDesc{
		ServiceName: "bootstrap.test.Hang",
		HandlerType: (*hangServiceServer)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Hang",
			Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
				in := new(emptypb.Empty)
				if err := dec(in); err != nil {
					return nil, err
				}
				if interceptor == nil {
					return srv.(hangServiceServer).Hang(ctx, in)
				}
				info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/bootstrap.test.Hang/Hang"}
				return interceptor(ctx, in, info, func(ctx context.Context, req any) (any, error) {
					return srv.(hangServiceServer).Hang(ctx, req.(*emptypb.Empty))
				})
			},
		}},
	}, impl)
	httpServer := &http.Server{Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})}

	serveDone := make(chan struct{})
	go func() {
		_ = grpcServer.Serve(grpcLis)
		close(serveDone)
	}()
	go func() { _ = httpServer.Serve(httpLis) }()

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}
	defer conn.Close()

	go func() {
		_ = conn.Invoke(context.Background(), "/bootstrap.test.Hang/Hang", &emptypb.Empty{}, &emptypb.Empty{})
	}()
	select {
	case <-handlerEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("hanging RPC never entered server handler")
	}

	stopDone := make(chan error, 1)
	go func() { stopDone <- stopIngress(grpcServer, httpServer, 50*time.Millisecond) }()

	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("stopIngress: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stopIngress did not return after forced Stop")
	}

	select {
	case <-serveDone:
	case <-time.After(2 * time.Second):
		t.Fatal("gRPC Serve did not exit after forced Stop")
	}

	rebind, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		t.Fatalf("gRPC addr still held after forced stop: %v", err)
	}
	_ = rebind.Close()
}

type hangServiceServer interface {
	Hang(context.Context, *emptypb.Empty) (*emptypb.Empty, error)
}

type hangServer struct {
	hold    <-chan struct{}
	entered chan struct{}
}

func (h *hangServer) Hang(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	select {
	case <-h.entered:
	default:
		close(h.entered)
	}
	select {
	case <-h.hold:
		return &emptypb.Empty{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestLifecycle_ServeFailureReachesWait(t *testing.T) {
	// Close the live gRPC listener under Serve so Wait sees a real serve error
	// (same-package access; no production test hook).
	srv := startTestServer(t)

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Wait() }()

	if err := srv.grpcLis.Close(); err != nil {
		t.Fatalf("close gRPC listener: %v", err)
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("Wait returned nil after gRPC listener closed; want serve failure")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Wait")
	}
}

func TestLifecycle_CloseThenWaitReturnsCloseResult(t *testing.T) {
	srv := startTestServer(t)

	// Start Wait before Close so the serveErrCh path runs isExpectedServeStop
	// while closeDone is still open (Close-then-Wait can race to closeDone).
	waitDone := make(chan error, 1)
	go func() { waitDone <- srv.Wait() }()

	closeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatalf("Wait after Close = %v, want nil close result", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Wait after Close")
	}
}

// freeLocalAddr returns a 127.0.0.1 address that is free at the moment of the call.
func freeLocalAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve local addr: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("release local addr: %v", err)
	}
	return addr
}

func TestLifecycle_CloseIdempotentAndConcurrent(t *testing.T) {
	srv := startTestServer(t)

	var wg sync.WaitGroup
	errs := make([]error, 3)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			errs[i] = srv.Close(ctx)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("Close[%d]: %v", i, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Close(ctx); err != nil {
		t.Fatalf("Close after close: %v", err)
	}
}

func TestLifecycle_ShutdownJoinsIndexReplayBeforeReturn(t *testing.T) {
	// Kubernetes in config creates kubeIndexing + startup replay. Substitute a
	// held join channel to prove ingress quiesces while Close waits on replay.
	srv := startTestServerWithConfig(t, ConfigInput{Addons: "kubernetes"})
	if srv.kubeIndexing == nil {
		t.Fatal("expected kubeIndexing when kubernetes addon enabled")
	}

	grpcAddr := srv.Endpoints().GRPC.Dial
	httpAddr := srv.Endpoints().HTTP.Dial

	held := make(chan struct{})
	srv.indexReplayDone = held
	srv.shutdownGrace = 5 * time.Second

	closeDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		closeDone <- srv.Close(ctx)
	}()

	// Ingress stops before dependency join — addresses become rebindable while
	// Close is still waiting on indexReplayDone.
	deadline := time.Now().Add(2 * time.Second)
	var reboundGRPC, reboundHTTP bool
	for time.Now().Before(deadline) && !(reboundGRPC && reboundHTTP) {
		if !reboundGRPC {
			if ln, err := net.Listen("tcp", grpcAddr); err == nil {
				_ = ln.Close()
				reboundGRPC = true
			}
		}
		if !reboundHTTP {
			if ln, err := net.Listen("tcp", httpAddr); err == nil {
				_ = ln.Close()
				reboundHTTP = true
			}
		}
		if reboundGRPC && reboundHTTP {
			break
		}
		select {
		case err := <-closeDone:
			t.Fatalf("Close returned before replay release (grpc rebound=%v http rebound=%v): %v", reboundGRPC, reboundHTTP, err)
		case <-time.After(20 * time.Millisecond):
		}
	}
	if !reboundGRPC || !reboundHTTP {
		t.Fatalf("listeners not released during replay join wait (grpc=%v http=%v)", reboundGRPC, reboundHTTP)
	}

	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before replay release: %v", err)
	default:
	}

	close(held)
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close after replay release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not finish after replay release")
	}
}

func TestLifecycle_WorkflowsRegisteredBeforeWorkerStart(t *testing.T) {
	reg := &trackingRegistry{inner: NewMemWorkflowRegistry()}
	_ = startTestServer(t, WithWorkflowRegistry(reg))
	if !reg.started {
		t.Fatal("workflow registry was not started")
	}
	if reg.startBeforeRegister {
		t.Fatal("workflow Start occurred before registrations completed")
	}
	if reg.registersBeforeStart < 8 {
		t.Fatalf("registers before Start = %d, want at least 8 workflow registrations", reg.registersBeforeStart)
	}
}

// trackingRegistry verifies Start is not called until the server finishes registering.
type trackingRegistry struct {
	inner                domain.Registry
	mu                   sync.Mutex
	started              bool
	registersBeforeStart int
	startBeforeRegister  bool
	registerCount        int
}

func (r *trackingRegistry) note() {
	r.mu.Lock()
	r.registerCount++
	r.mu.Unlock()
}

func (r *trackingRegistry) Start(ctx context.Context) error {
	r.mu.Lock()
	if r.registerCount == 0 {
		r.startBeforeRegister = true
	}
	r.registersBeforeStart = r.registerCount
	r.started = true
	r.mu.Unlock()
	return r.inner.Start(ctx)
}
func (r *trackingRegistry) Wait(ctx context.Context) error  { return r.inner.Wait(ctx) }
func (r *trackingRegistry) Close(ctx context.Context) error { return r.inner.Close(ctx) }

func (r *trackingRegistry) SignalFulfillmentEvent(ctx context.Context, id domain.FulfillmentID, event domain.FulfillmentEvent) error {
	return r.inner.SignalFulfillmentEvent(ctx, id, event)
}
func (r *trackingRegistry) SignalDeleteCleanupComplete(ctx context.Context, id domain.FulfillmentID, event domain.DeleteCleanupCompleteEvent) error {
	return r.inner.SignalDeleteCleanupComplete(ctx, id, event)
}
func (r *trackingRegistry) RegisterOrchestration(spec *domain.OrchestrationWorkflowSpec) (domain.OrchestrationWorkflow, error) {
	r.note()
	return r.inner.RegisterOrchestration(spec)
}
func (r *trackingRegistry) RegisterCreateDeployment(spec *domain.CreateDeploymentWorkflowSpec) (domain.CreateDeploymentWorkflow, error) {
	r.note()
	return r.inner.RegisterCreateDeployment(spec)
}
func (r *trackingRegistry) RegisterDeleteDeployment(spec *domain.DeleteDeploymentWorkflowSpec) (domain.DeleteDeploymentWorkflow, error) {
	r.note()
	return r.inner.RegisterDeleteDeployment(spec)
}
func (r *trackingRegistry) RegisterDeleteDeploymentCleanup(spec *domain.DeleteDeploymentCleanupWorkflowSpec) (domain.DeleteDeploymentCleanupWorkflow, error) {
	r.note()
	return r.inner.RegisterDeleteDeploymentCleanup(spec)
}
func (r *trackingRegistry) RegisterResumeDeployment(spec *domain.ResumeDeploymentWorkflowSpec) (domain.ResumeDeploymentWorkflow, error) {
	r.note()
	return r.inner.RegisterResumeDeployment(spec)
}
func (r *trackingRegistry) RegisterProvisionIdP(spec *domain.ProvisionIdPWorkflowSpec) (domain.ProvisionIdPWorkflow, error) {
	r.note()
	return r.inner.RegisterProvisionIdP(spec)
}
func (r *trackingRegistry) RegisterCreateManagedResource(spec *domain.CreateManagedResourceWorkflowSpec) (domain.CreateManagedResourceWorkflow, error) {
	r.note()
	return r.inner.RegisterCreateManagedResource(spec)
}
func (r *trackingRegistry) RegisterDeleteManagedResource(spec *domain.DeleteManagedResourceWorkflowSpec) (domain.DeleteManagedResourceWorkflow, error) {
	r.note()
	return r.inner.RegisterDeleteManagedResource(spec)
}
func (r *trackingRegistry) RegisterDeleteManagedResourceCleanup(spec *domain.DeleteManagedResourceCleanupWorkflowSpec) (domain.DeleteManagedResourceCleanupWorkflow, error) {
	r.note()
	return r.inner.RegisterDeleteManagedResourceCleanup(spec)
}
func (r *trackingRegistry) RegisterResumeManagedResource(spec *domain.ResumeManagedResourceWorkflowSpec) (domain.ResumeManagedResourceWorkflow, error) {
	r.note()
	return r.inner.RegisterResumeManagedResource(spec)
}
