package bootstrap

import (
	"context"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
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
	if err != nil {
		t.Fatalf("ListDeployments via resolved dial: %v", err)
	}

	resp, err := http.Get("http://" + ep.HTTP.Dial + "/v1/deployments")
	if err != nil {
		t.Fatalf("http get via gateway: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		t.Fatal("gateway /v1/deployments not found; dial target mismatch?")
	}
}

func TestLifecycle_GatewaySurvivesStartContextCancel(t *testing.T) {
	// HandlerFromEndpoint closed its ClientConn when the start ctx ended
	// (testserver.Start's defer cancel). Shared Close-owned conn must remain.
	startCtx, startCancel := context.WithCancel(context.Background())
	cfg, err := NewConfig(ConfigInput{
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
		DBPath:   filepath.Join(t.TempDir(), "fleetshift.db"),
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
		GRPCAddr: grpcAddr,
		HTTPAddr: heldHTTP.Addr().String(),
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
		GRPCAddr: grpcAddr,
		HTTPAddr: httpAddr,
		DBPath:   dbPath,
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

func TestLifecycle_WithSQLiteDBCallerRetainsOwnership(t *testing.T) {
	db, sentinel, err := sqlite.OpenMemory(t.Name())
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	t.Cleanup(func() {
		_ = sentinel.Close()
		_ = db.Close()
	})

	if _, err := db.Exec(`CREATE TABLE ownership_probe (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	srv := startTestServerWithConfig(t, ConfigInput{
		DBPath: "memory", // not opened; handle comes from WithSQLiteDB
	}, WithSQLiteDB(db))

	closeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Injected DB must remain usable after Server.Close.
	if err := db.Ping(); err != nil {
		t.Fatalf("Ping after Server.Close: %v", err)
	}
	path := filepath.Join(t.TempDir(), "fleetshift.db")
	if err := sqlite.DumpToFile(db, path); err != nil {
		t.Fatalf("DumpToFile after Server.Close: %v", err)
	}
}

func TestOpenPersistence_WithSQLiteDBRequiresSQLite(t *testing.T) {
	db, sentinel, err := sqlite.OpenMemory(t.Name())
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	t.Cleanup(func() {
		_ = sentinel.Close()
		_ = db.Close()
	})

	_, err = openPersistence(Postgres{
		Host:      "localhost",
		Port:      5432,
		Name:      "fleetshift",
		DriverDSN: "postgres://localhost:5432/fleetshift",
	}, db)
	if err == nil {
		t.Fatal("expected error injecting SQLite DB into Postgres config")
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
