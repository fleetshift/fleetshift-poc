package serverapp

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

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestLifecycle_DynamicEndpointsDialable(t *testing.T) {
	app := startTestApp(t)
	ep := app.Endpoints()
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
		WithWorkflowRuntime(NewMemWorkflowRuntime()),
		WithIdentity(Identity{Discovery: testDiscovery{}, Verifier: testVerifier{}}),
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
	cfg, err := NewConfig(ConfigInput{
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
		DBPath:   filepath.Join(t.TempDir(), "fleetshift.db"),
	})
	if err != nil {
		t.Fatalf("NewConfig: %v", err)
	}

	_, err = Start(context.Background(), cfg, testLogger(),
		WithWorkflowRuntime(NewMemWorkflowRuntime()),
		WithIdentity(Identity{Discovery: testDiscovery{}, Verifier: testVerifier{}}),
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
		WithWorkflowRuntime(NewMemWorkflowRuntime()),
		WithIdentity(Identity{Discovery: testDiscovery{}, Verifier: testVerifier{}}),
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

func TestLifecycle_ServeFailureReachesWait(t *testing.T) {
	// Close the live gRPC listener under Serve so Wait sees a real serve error
	// (same-package access; no production test hook).
	app := startTestApp(t)

	errCh := make(chan error, 1)
	go func() { errCh <- app.Wait() }()

	if err := app.grpcLis.Close(); err != nil {
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
	app := startTestApp(t)

	var wg sync.WaitGroup
	errs := make([]error, 3)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			errs[i] = app.Close(ctx)
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
	if err := app.Close(ctx); err != nil {
		t.Fatalf("Close after close: %v", err)
	}
}

func TestLifecycle_CloseCallerDeadlineIndependent(t *testing.T) {
	app := startTestApp(t)

	// A timed-out caller must not cancel shared cleanup for a later caller.
	expired, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)
	if err := app.Close(expired); !errors.Is(err, context.DeadlineExceeded) {
		// Close may finish before the nanosecond deadline on a fast machine;
		// either deadline exceeded or success is acceptable for the first call.
		if err != nil {
			t.Logf("first Close: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := app.Close(ctx); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestLifecycle_WorkflowsRegisteredBeforeWorkerStart(t *testing.T) {
	reg := &trackingRuntime{inner: NewMemWorkflowRuntime()}
	_ = startTestApp(t, WithWorkflowRuntime(reg))
	if !reg.started {
		t.Fatal("workflow runtime was not started")
	}
	if reg.startBeforeRegister {
		t.Fatal("workflow Start occurred before registrations completed")
	}
	if reg.registersBeforeStart < 8 {
		t.Fatalf("registers before Start = %d, want at least 8 workflow registrations", reg.registersBeforeStart)
	}
}

// trackingRuntime verifies Start is not called until the app finishes registering.
type trackingRuntime struct {
	inner                *MemWorkflowRuntime
	mu                   sync.Mutex
	started              bool
	registersBeforeStart int
	startBeforeRegister  bool
	registerCount        int
}

func (r *trackingRuntime) Registry() domain.Registry {
	return &trackingRegistry{rt: r, inner: r.inner.Registry()}
}
func (r *trackingRuntime) Start(ctx context.Context) error {
	r.mu.Lock()
	if r.registerCount == 0 {
		r.startBeforeRegister = true
	}
	r.registersBeforeStart = r.registerCount
	r.started = true
	r.mu.Unlock()
	return r.inner.Start(ctx)
}
func (r *trackingRuntime) Wait(ctx context.Context) error  { return r.inner.Wait(ctx) }
func (r *trackingRuntime) Close(ctx context.Context) error { return r.inner.Close(ctx) }

type trackingRegistry struct {
	rt    *trackingRuntime
	inner domain.Registry
}

func (r *trackingRegistry) note() {
	r.rt.mu.Lock()
	r.rt.registerCount++
	r.rt.mu.Unlock()
}

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
