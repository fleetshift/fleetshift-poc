package managedresource_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/memworkflow"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/managedresource"
)

// buildClusterService creates a RegisteredService backed only by the
// proto compiler and a validator. The handlers will panic if invoked
// because the Resources dependency is nil — use buildFullClusterService
// for tests that invoke actual CRUD operations through the mux.
func buildClusterService(t *testing.T) *managedresource.RegisteredService {
	t.Helper()
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("protovalidate.New: %v", err)
	}
	svc, err := managedresource.Build(clusterConfig(t), managedresource.Deps{Validator: validator})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return svc
}

// buildFullClusterService creates a RegisteredService backed by a real
// in-memory database, workflow engine, and delivery router. Use this for
// tests that invoke CRUD handlers through the mux (e.g. CreateCluster).
//
// Each call within the same test reuses the same database (keyed by
// t.Name()). Use buildFullClusterServiceN when you need multiple
// independent instances in one test (e.g. Replace scenarios).
func buildFullClusterService(t *testing.T) *managedresource.RegisteredService {
	t.Helper()
	return buildFullClusterServiceN(t, 0)
}

// buildFullClusterServiceN is like [buildFullClusterService] but uses a
// sequence number to create a distinct in-memory database per call. This
// allows tests that need multiple independent services (e.g. Replace)
// within a single test function.
func buildFullClusterServiceN(t *testing.T, n int) *managedresource.RegisteredService {
	t.Helper()

	dsn := fmt.Sprintf("file:%s_%d?mode=memory&cache=shared", t.Name(), n)
	db, err := sqlite.Open(dsn)
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	db.SetMaxOpenConns(2)
	sentinel, err := db.Conn(context.Background())
	if err != nil {
		db.Close()
		t.Fatalf("open sentinel: %v", err)
	}
	t.Cleanup(func() {
		sentinel.Close()
		db.Close()
	})
	store := &sqlite.Store{DB: db}

	recordingAgent := &sqlite.RecordingDeliveryService{
		Store: store,
		Now:   func() time.Time { return time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC) },
	}
	router := delivery.NewRoutingDeliveryService()
	router.Register(clusterTargetType, recordingAgent)

	reg := &memworkflow.Registry{}
	recordingAgent.Reporter = application.NewDeliveryReportService(store, reg)
	orchSpec := &domain.OrchestrationWorkflowSpec{
		Store: store, Delivery: router,
		Strategies: domain.StrategyFactory{Store: store}, CleanupSignaler: reg,
	}
	orchWf, err := reg.RegisterOrchestration(orchSpec)
	if err != nil {
		t.Fatalf("RegisterOrchestration: %v", err)
	}
	createMRWf, err := reg.RegisterCreateManagedResource(&domain.CreateManagedResourceWorkflowSpec{
		Store: store, Orchestration: orchWf,
	})
	if err != nil {
		t.Fatalf("RegisterCreateManagedResource: %v", err)
	}
	mrCleanupWf, err := reg.RegisterDeleteManagedResourceCleanup(&domain.DeleteManagedResourceCleanupWorkflowSpec{Store: store})
	if err != nil {
		t.Fatalf("RegisterDeleteManagedResourceCleanup: %v", err)
	}
	deleteMRWf, err := reg.RegisterDeleteManagedResource(&domain.DeleteManagedResourceWorkflowSpec{
		Store: store, Orchestration: orchWf, Cleanup: mrCleanupWf,
	})
	if err != nil {
		t.Fatalf("RegisterDeleteManagedResource: %v", err)
	}
	resumeMRWf, err := reg.RegisterResumeManagedResource(&domain.ResumeManagedResourceWorkflowSpec{
		Store: store, Orchestration: orchWf,
	})
	if err != nil {
		t.Fatalf("RegisterResumeManagedResource: %v", err)
	}

	managedResourceSvc := &application.ManagedResourceService{
		Store: store, CreateWF: createMRWf, DeleteWF: deleteMRWf, ResumeWF: resumeMRWf,
	}

	targetSvc := &application.TargetService{Store: store}
	if err := targetSvc.Register(context.Background(), domain.TargetInfo{
		ID: "kind-local", Type: clusterTargetType, Name: "Kind Cluster Addon",
		AcceptedResourceTypes: []domain.ResourceType{kindaddon.ClusterResourceType},
	}); err != nil {
		t.Fatalf("register target: %v", err)
	}

	typeSvc := &application.ManagedResourceTypeService{Store: store}
	if _, err := typeSvc.Create(context.Background(), application.CreateTypeInput{
		ResourceType: kindaddon.ClusterResourceType,
		Relation:     domain.RegisteredSelfTarget{AddonTarget: "kind-local"},
		Signature:    domain.Signature{},
	}); err != nil {
		t.Fatalf("register cluster type: %v", err)
	}

	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("protovalidate.New: %v", err)
	}

	svc, err := managedresource.Build(clusterConfig(t), managedresource.Deps{
		Resources: managedResourceSvc, Validator: validator,
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return svc
}

// dialMux starts a gRPC server with mux.Handle as the unknown service
// handler and returns a client connection over bufconn. Additional
// server options (e.g. stream interceptors) can be passed.
func dialMux(t *testing.T, mux *managedresource.DynamicServiceMux, opts ...grpc.ServerOption) *grpc.ClientConn {
	t.Helper()

	allOpts := append([]grpc.ServerOption{grpc.UnknownServiceHandler(mux.Handle)}, opts...)
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(allOpts...)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// createClusterRequest builds a valid CreateKindCluster request from
// the service's dynamic message descriptors.
func createClusterRequest(svc *managedresource.RegisteredService, id string) *dynamicpb.Message {
	req := dynamicpb.NewMessage(svc.Descriptors.CreateRequest)
	req.Set(svc.Descriptors.CreateRequest.Fields().ByNumber(1), protoreflect.ValueOfString(id))

	resource := dynamicpb.NewMessage(svc.Descriptors.Resource)
	spec := dynamicpb.NewMessage(svc.Descriptors.Spec)
	spec.Set(svc.Descriptors.Spec.Fields().ByName("name"), protoreflect.ValueOfString(id))
	resource.Set(svc.Descriptors.Resource.Fields().ByName("spec"), protoreflect.ValueOfMessage(spec))
	req.Set(svc.Descriptors.CreateRequest.Fields().ByNumber(2), protoreflect.ValueOfMessage(resource))

	return req
}

// ---------------------------------------------------------------------------
// Tests that don't need gRPC dispatch — pure mux operations
// ---------------------------------------------------------------------------

func TestDynamicMux_DuplicateRegisterReturnsError(t *testing.T) {
	svc := buildClusterService(t)
	mux := managedresource.NewDynamicServiceMux()

	if err := mux.Register(svc); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	if err := mux.Register(svc); err == nil {
		t.Fatal("expected error on duplicate register, got nil")
	}
}

func TestDynamicMux_ReplaceSwapsService(t *testing.T) {
	svc := buildClusterService(t)
	mux := managedresource.NewDynamicServiceMux()

	if err := mux.Register(svc); err != nil {
		t.Fatalf("Register: %v", err)
	}

	svc2 := buildClusterService(t)
	mux.Replace(svc2)

	info := mux.ServiceInfo()
	if _, ok := info["fleetshift.v1.KindClusterService"]; !ok {
		t.Fatal("expected service present after Replace")
	}
}

func TestDynamicMux_ReplaceAddsIfAbsent(t *testing.T) {
	svc := buildClusterService(t)
	mux := managedresource.NewDynamicServiceMux()

	mux.Replace(svc)

	info := mux.ServiceInfo()
	if _, ok := info["fleetshift.v1.KindClusterService"]; !ok {
		t.Fatal("expected service added by Replace")
	}
}

func TestDynamicMux_ReplaceDispatchesToNewHandler(t *testing.T) {
	mux := managedresource.NewDynamicServiceMux()

	svc1 := buildFullClusterServiceN(t, 1)
	if err := mux.Register(svc1); err != nil {
		t.Fatalf("Register: %v", err)
	}
	conn := dialMux(t, mux)

	req1 := createClusterRequest(svc1, "before-replace")
	resp1 := dynamicpb.NewMessage(svc1.Descriptors.Resource)
	if err := conn.Invoke(context.Background(), "/fleetshift.v1.KindClusterService/CreateKindCluster", req1, resp1); err != nil {
		t.Fatalf("CreateCluster before replace: %v", err)
	}

	// Replace with a service backed by a fresh, empty database.
	svc2 := buildFullClusterServiceN(t, 2)
	mux.Replace(svc2)

	getReq := dynamicpb.NewMessage(svc2.Descriptors.GetRequest)
	getReq.Set(svc2.Descriptors.GetRequest.Fields().ByName("name"),
		protoreflect.ValueOfString("kindClusters/before-replace"))
	getResp := dynamicpb.NewMessage(svc2.Descriptors.Resource)
	err := conn.Invoke(context.Background(), "/fleetshift.v1.KindClusterService/GetKindCluster", getReq, getResp)
	if err == nil {
		t.Fatal("expected NotFound from replaced handler, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status, got: %v", err)
	}
	if st.Code() != codes.NotFound {
		t.Errorf("code = %v, want NotFound (replaced handler has empty DB)", st.Code())
	}

	// But a new create through the replaced handler should succeed.
	req2 := createClusterRequest(svc2, "after-replace")
	resp2 := dynamicpb.NewMessage(svc2.Descriptors.Resource)
	if err := conn.Invoke(context.Background(), "/fleetshift.v1.KindClusterService/CreateKindCluster", req2, resp2); err != nil {
		t.Fatalf("CreateCluster after replace: %v", err)
	}
	nameField := svc2.Descriptors.Resource.Fields().ByName("name")
	if got := resp2.Get(nameField).String(); got != "kindClusters/after-replace" {
		t.Errorf("name = %q, want kindClusters/after-replace", got)
	}
}

func TestDynamicMux_ServiceInfo(t *testing.T) {
	svc := buildClusterService(t)
	mux := managedresource.NewDynamicServiceMux()

	if err := mux.Register(svc); err != nil {
		t.Fatalf("Register: %v", err)
	}

	info := mux.ServiceInfo()
	si, ok := info["fleetshift.v1.KindClusterService"]
	if !ok {
		t.Fatal("ServiceInfo missing fleetshift.v1.KindClusterService")
	}
	if len(si.Methods) != 5 {
		t.Errorf("method count = %d, want 5", len(si.Methods))
	}

	methodNames := make(map[string]bool)
	for _, m := range si.Methods {
		methodNames[m.Name] = true
	}
	for _, want := range []string{"CreateKindCluster", "GetKindCluster", "ListKindClusters", "DeleteKindCluster", "ResumeKindCluster"} {
		if !methodNames[want] {
			t.Errorf("missing method %q in ServiceInfo", want)
		}
	}
}

func TestDynamicMux_CompositeServiceInfoProvider(t *testing.T) {
	svc := buildClusterService(t)
	mux := managedresource.NewDynamicServiceMux()
	if err := mux.Register(svc); err != nil {
		t.Fatalf("Register: %v", err)
	}

	srv := grpc.NewServer()
	composite := &managedresource.CompositeServiceInfoProvider{
		Server:     srv,
		DynamicMux: mux,
	}

	info := composite.GetServiceInfo()
	if _, ok := info["fleetshift.v1.KindClusterService"]; !ok {
		t.Error("composite info missing dynamic service fleetshift.v1.KindClusterService")
	}
}

// ---------------------------------------------------------------------------
// Tests that dispatch through gRPC but don't need working handlers
// ---------------------------------------------------------------------------

func TestDynamicMux_UnregisteredServiceReturnsUnimplemented(t *testing.T) {
	svc := buildClusterService(t)
	mux := managedresource.NewDynamicServiceMux()
	conn := dialMux(t, mux)

	req := dynamicpb.NewMessage(svc.Descriptors.CreateRequest)
	resp := dynamicpb.NewMessage(svc.Descriptors.Resource)
	err := conn.Invoke(context.Background(), "/fleetshift.v1.KindClusterService/CreateKindCluster", req, resp)
	if err == nil {
		t.Fatal("expected error for unregistered service, got nil")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status, got: %v", err)
	}
	if st.Code() != codes.Unimplemented {
		t.Errorf("code = %v, want Unimplemented", st.Code())
	}
}

func TestDynamicMux_DeregisterMakesServiceUnreachable(t *testing.T) {
	svc := buildClusterService(t)
	mux := managedresource.NewDynamicServiceMux()
	conn := dialMux(t, mux)

	if err := mux.Register(svc); err != nil {
		t.Fatalf("Register: %v", err)
	}

	mux.Deregister("fleetshift.v1.KindClusterService")

	req := dynamicpb.NewMessage(svc.Descriptors.CreateRequest)
	resp := dynamicpb.NewMessage(svc.Descriptors.Resource)
	err := conn.Invoke(context.Background(), "/fleetshift.v1.KindClusterService/CreateKindCluster", req, resp)
	if err == nil {
		t.Fatal("expected error after deregister, got nil")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status, got: %v", err)
	}
	if st.Code() != codes.Unimplemented {
		t.Errorf("code = %v, want Unimplemented", st.Code())
	}
}

// ---------------------------------------------------------------------------
// Tests that invoke real CRUD handlers — need the full application stack
// ---------------------------------------------------------------------------

func TestDynamicMux_RegisterAndDispatch(t *testing.T) {
	svc := buildFullClusterService(t)
	mux := managedresource.NewDynamicServiceMux()
	if err := mux.Register(svc); err != nil {
		t.Fatalf("Register: %v", err)
	}
	conn := dialMux(t, mux)

	req := createClusterRequest(svc, "dyn-cluster-1")
	resp := dynamicpb.NewMessage(svc.Descriptors.Resource)
	if err := conn.Invoke(context.Background(), "/fleetshift.v1.KindClusterService/CreateKindCluster", req, resp); err != nil {
		t.Fatalf("CreateCluster via dynamic mux: %v", err)
	}

	nameField := svc.Descriptors.Resource.Fields().ByName("name")
	got := resp.Get(nameField).String()
	if got != "kindClusters/dyn-cluster-1" {
		t.Errorf("name = %q, want %q", got, "kindClusters/dyn-cluster-1")
	}
}

func TestDynamicMux_StreamInterceptorFires(t *testing.T) {
	svc := buildFullClusterService(t)
	mux := managedresource.NewDynamicServiceMux()
	if err := mux.Register(svc); err != nil {
		t.Fatalf("Register: %v", err)
	}

	var interceptorCalled atomic.Bool
	conn := dialMux(t, mux,
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			interceptorCalled.Store(true)
			return handler(srv, ss)
		}),
	)

	req := createClusterRequest(svc, "interceptor-test")
	resp := dynamicpb.NewMessage(svc.Descriptors.Resource)
	if err := conn.Invoke(context.Background(), "/fleetshift.v1.KindClusterService/CreateKindCluster", req, resp); err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	if !interceptorCalled.Load() {
		t.Fatal("stream interceptor was NOT called for dynamic service dispatch")
	}
}

// ---------------------------------------------------------------------------
// DynamicHTTPMux tests — handler indirection, replace, deregister
// ---------------------------------------------------------------------------

// serveGRPCOverTCP starts a real TCP gRPC server with the service
// registered via a DynamicServiceMux and returns the listener address.
// Needed by the HTTP proxy handler which connects over a real address.
func serveGRPCOverTCP(t *testing.T, svc *managedresource.RegisteredService) string {
	t.Helper()
	grpcMux := managedresource.NewDynamicServiceMux()
	if err := grpcMux.Register(svc); err != nil {
		t.Fatalf("Register: %v", err)
	}
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(grpc.UnknownServiceHandler(grpcMux.Handle))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)
	return lis.Addr().String()
}

func httpCreateCluster(t *testing.T, baseURL, id string) *http.Response {
	t.Helper()
	body := `{"spec": {"name": "` + id + `"}}`
	resp, err := http.Post(baseURL+"/v1/kindClusters?kindCluster_id="+id, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/kindClusters: %v", err)
	}
	return resp
}

func httpGetCluster(t *testing.T, baseURL, id string) *http.Response {
	t.Helper()
	resp, err := http.Get(baseURL + "/v1/kindClusters/" + id)
	if err != nil {
		t.Fatalf("GET /v1/kindClusters/%s: %v", id, err)
	}
	return resp
}

func TestDynamicHTTPMux_RegisterAndDispatch(t *testing.T) {
	svc := buildFullClusterService(t)
	grpcAddr := serveGRPCOverTCP(t, svc)

	httpMux := managedresource.NewDynamicHTTPMux(nil)
	if err := httpMux.Register(svc, grpcAddr); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ts := httptest.NewServer(httpMux.ServeMux())
	defer ts.Close()

	resp := httpCreateCluster(t, ts.URL, "http-test-1")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST status = %d, want 200; body = %s", resp.StatusCode, body)
	}

	resp2 := httpGetCluster(t, ts.URL, "http-test-1")
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("GET status = %d, want 200", resp2.StatusCode)
	}
}

func TestDynamicHTTPMux_DeregisterReturns404(t *testing.T) {
	svc := buildFullClusterService(t)
	grpcAddr := serveGRPCOverTCP(t, svc)

	httpMux := managedresource.NewDynamicHTTPMux(nil)
	if err := httpMux.Register(svc, grpcAddr); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ts := httptest.NewServer(httpMux.ServeMux())
	defer ts.Close()

	// Verify it's routable first.
	resp := httpCreateCluster(t, ts.URL, "will-deregister")
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST before deregister: status = %d, want 200", resp.StatusCode)
	}

	httpMux.Deregister("KindClusters")

	resp2 := httpGetCluster(t, ts.URL, "will-deregister")
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("GET after deregister: status = %d, want 404", resp2.StatusCode)
	}
}

func TestDynamicHTTPMux_DuplicateRegisterReturnsError(t *testing.T) {
	svc := buildClusterService(t)

	httpMux := managedresource.NewDynamicHTTPMux(nil)
	if err := httpMux.Register(svc, "localhost:0"); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	if err := httpMux.Register(svc, "localhost:0"); err == nil {
		t.Fatal("expected error on duplicate Register")
	}
}

func TestDynamicHTTPMux_ReplaceDispatchesToNewHandler(t *testing.T) {
	svc1 := buildFullClusterServiceN(t, 1)
	grpcAddr1 := serveGRPCOverTCP(t, svc1)

	httpMux := managedresource.NewDynamicHTTPMux(nil)
	if err := httpMux.Register(svc1, grpcAddr1); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ts := httptest.NewServer(httpMux.ServeMux())
	defer ts.Close()

	resp := httpCreateCluster(t, ts.URL, "before-swap")
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST before replace: status = %d", resp.StatusCode)
	}

	// Replace with a service backed by a fresh, empty database.
	svc2 := buildFullClusterServiceN(t, 2)
	grpcAddr2 := serveGRPCOverTCP(t, svc2)
	if err := httpMux.Replace(svc2, grpcAddr2); err != nil {
		t.Fatalf("Replace: %v", err)
	}

	// The old resource should not be reachable through the new handler.
	resp2 := httpGetCluster(t, ts.URL, "before-swap")
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("GET after replace: status = %d, want 404 (new DB is empty)", resp2.StatusCode)
	}

	// A new create through the replaced handler should succeed.
	resp3 := httpCreateCluster(t, ts.URL, "after-swap")
	resp3.Body.Close()
	if resp3.StatusCode != http.StatusOK {
		t.Errorf("POST after replace: status = %d, want 200", resp3.StatusCode)
	}
}

func TestDynamicHTTPMux_ReplaceAddsIfAbsent(t *testing.T) {
	svc := buildFullClusterService(t)
	grpcAddr := serveGRPCOverTCP(t, svc)

	httpMux := managedresource.NewDynamicHTTPMux(nil)
	if err := httpMux.Replace(svc, grpcAddr); err != nil {
		t.Fatalf("Replace (absent): %v", err)
	}

	ts := httptest.NewServer(httpMux.ServeMux())
	defer ts.Close()

	resp := httpCreateCluster(t, ts.URL, "replace-absent")
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("POST status = %d, want 200", resp.StatusCode)
	}
}

// serveGRPCOverTCPWithAuth starts a TCP gRPC server whose stream
// interceptor authenticates when an "authorization" metadata value is
// present. The dynamic mux dispatches through [grpc.UnknownServiceHandler]
// which fires stream interceptors, matching production wiring.
func serveGRPCOverTCPWithAuth(t *testing.T, svc *managedresource.RegisteredService) string {
	t.Helper()
	grpcMux := managedresource.NewDynamicServiceMux()
	if err := grpcMux.Register(svc); err != nil {
		t.Fatalf("Register: %v", err)
	}

	authStreamInterceptor := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		md, _ := metadata.FromIncomingContext(ss.Context())
		vals := md.Get("authorization")
		if len(vals) > 0 && strings.HasPrefix(vals[0], "Bearer ") {
			ctx := application.ContextWithAuth(ss.Context(), &application.AuthorizationContext{
				Subject: &domain.SubjectClaims{
					FederatedIdentity: domain.FederatedIdentity{Subject: "http-user", Issuer: "https://http-issuer.example.com"},
				},
				Token: domain.RawToken(strings.TrimPrefix(vals[0], "Bearer ")),
			})
			ss = &wrappedServerStream{ServerStream: ss, ctx: ctx}
		}
		return handler(srv, ss)
	}

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(
		grpc.ChainStreamInterceptor(authStreamInterceptor),
		grpc.UnknownServiceHandler(grpcMux.Handle),
	)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)
	return lis.Addr().String()
}

type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStream) Context() context.Context { return w.ctx }

func TestDynamicHTTPMux_ResumeForwardsAuth(t *testing.T) {
	svc := buildFullClusterService(t)
	grpcAddr := serveGRPCOverTCPWithAuth(t, svc)

	httpMux := managedresource.NewDynamicHTTPMux(nil)
	if err := httpMux.Register(svc, grpcAddr); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ts := httptest.NewServer(httpMux.ServeMux())
	defer ts.Close()

	// Create a resource (no auth required for create in this harness).
	resp := httpCreateCluster(t, ts.URL, "http-resume-test")
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST create status = %d, want 200", resp.StatusCode)
	}

	// Use the gRPC path (with auth) to confirm the resource exists via HTTP GET.
	getResp := httpGetCluster(t, ts.URL, "http-resume-test")
	getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", getResp.StatusCode)
	}

	// We can't easily transition to paused_auth via HTTP alone, so we
	// verify the auth forwarding by calling resume on a non-paused
	// resource WITH a bearer token. The expected error is
	// "not paused_auth" (InvalidArgument/400), NOT "requires authenticated
	// caller" — which would mean the token wasn't forwarded.
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/kindClusters/http-resume-test:resume", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer valid-test-token")

	resumeResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST resume: %v", err)
	}
	defer resumeResp.Body.Close()
	body, _ := io.ReadAll(resumeResp.Body)

	// 400 (InvalidArgument) = auth succeeded, state check failed.
	// If auth forwarding were broken, we'd get 400 with "requires
	// authenticated caller" instead of "not paused_auth".
	if resumeResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("resume status = %d, want 400; body = %s", resumeResp.StatusCode, body)
	}
	if !strings.Contains(string(body), "not paused_auth") {
		t.Errorf("expected 'not paused_auth' error, got: %s", body)
	}
}

func TestDynamicHTTPMux_ResumeWithoutAuth_Rejected(t *testing.T) {
	svc := buildFullClusterService(t)
	grpcAddr := serveGRPCOverTCPWithAuth(t, svc)

	httpMux := managedresource.NewDynamicHTTPMux(nil)
	if err := httpMux.Register(svc, grpcAddr); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ts := httptest.NewServer(httpMux.ServeMux())
	defer ts.Close()

	// Create a resource.
	resp := httpCreateCluster(t, ts.URL, "no-auth-resume")
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST create status = %d, want 200", resp.StatusCode)
	}

	// Call resume WITHOUT Authorization header — should fail with auth error.
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/kindClusters/no-auth-resume:resume", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	resumeResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST resume: %v", err)
	}
	defer resumeResp.Body.Close()
	body, _ := io.ReadAll(resumeResp.Body)

	if resumeResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("resume status = %d, want 400; body = %s", resumeResp.StatusCode, body)
	}
	if !strings.Contains(string(body), "requires an authenticated caller") {
		t.Errorf("expected 'requires an authenticated caller' error, got: %s", body)
	}
}
