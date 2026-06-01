package managedresource_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
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

func newActivator(t *testing.T) (*managedresource.DynamicSchemaActivator, *managedresource.DynamicServiceMux) {
	t.Helper()
	mux := managedresource.NewDynamicServiceMux()
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("protovalidate.New: %v", err)
	}
	return &managedresource.DynamicSchemaActivator{
		GRPCMux: mux,
		Deps:    managedresource.Deps{Validator: validator},
	}, mux
}

type activatorHTTPEnv struct {
	activator *managedresource.DynamicSchemaActivator
	grpcMux   *managedresource.DynamicServiceMux
	httpMux   *managedresource.DynamicHTTPMux
	httpURL   string
}

// newActivatorWithHTTP creates an activator backed by both a gRPC mux
// and an HTTP mux with a live TCP gRPC server for the proxy to connect
// to. The returned httpURL is the base URL for the httptest server.
func newActivatorWithHTTP(t *testing.T) activatorHTTPEnv {
	t.Helper()
	grpcMux := managedresource.NewDynamicServiceMux()
	httpMux := managedresource.NewDynamicHTTPMux(nil)

	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("protovalidate.New: %v", err)
	}

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	// Wrap the mux handler with panic recovery. The activator tests
	// don't provide a Resources dep, so gRPC handlers will panic on
	// actual CRUD. Recovery converts panics to Internal errors so the
	// HTTP proxy gets a clean gRPC status instead of a process crash.
	safeHandle := func(srv any, stream grpc.ServerStream) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = status.Errorf(codes.Internal, "recovered: %v", r)
			}
		}()
		return grpcMux.Handle(srv, stream)
	}
	srv := grpc.NewServer(grpc.UnknownServiceHandler(safeHandle))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	ts := httptest.NewServer(httpMux.ServeMux())
	t.Cleanup(ts.Close)

	return activatorHTTPEnv{
		activator: &managedresource.DynamicSchemaActivator{
			GRPCMux:  grpcMux,
			HTTPMux:  httpMux,
			GRPCAddr: lis.Addr().String(),
			Deps:     managedresource.Deps{Validator: validator},
		},
		grpcMux: grpcMux,
		httpMux: httpMux,
		httpURL: ts.URL,
	}
}

func TestDynamicSchemaActivator_ActivateRegistersService(t *testing.T) {
	activator, mux := newActivator(t)

	schema := kindaddon.Schema()
	handle, err := activator.Activate(context.Background(), schema)
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}

	if handle.ServiceName != "fleetshift.v1.KindClusterService" {
		t.Errorf("handle.ServiceName = %q, want fleetshift.v1.KindClusterService", handle.ServiceName)
	}
	if handle.Plural != "KindClusters" {
		t.Errorf("handle.Plural = %q, want KindClusters", handle.Plural)
	}

	info := mux.ServiceInfo()
	si, ok := info["fleetshift.v1.KindClusterService"]
	if !ok {
		t.Fatal("expected KindClusterService in mux ServiceInfo after Activate")
	}
	if len(si.Methods) != 5 {
		t.Errorf("method count = %d, want 5", len(si.Methods))
	}
}

func TestDynamicSchemaActivator_DeactivateRemovesService(t *testing.T) {
	activator, mux := newActivator(t)

	schema := kindaddon.Schema()
	handle, err := activator.Activate(context.Background(), schema)
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}

	activator.Deactivate(handle)

	if _, ok := mux.ServiceInfo()["fleetshift.v1.KindClusterService"]; ok {
		t.Error("expected KindClusterService removed from mux after Deactivate")
	}

	if _, ok := activator.ContentHash("fleetshift.v1.KindClusterService"); ok {
		t.Error("expected content hash cleared after Deactivate")
	}
}

func TestDynamicSchemaActivator_DuplicateActivateIsIdempotent(t *testing.T) {
	activator, _ := newActivator(t)

	schema := kindaddon.Schema()
	h1, err := activator.Activate(context.Background(), schema)
	if err != nil {
		t.Fatalf("first Activate: %v", err)
	}

	h2, err := activator.Activate(context.Background(), schema)
	if err != nil {
		t.Fatalf("second Activate: %v", err)
	}

	if h1 != h2 {
		t.Errorf("handles differ: %v vs %v", h1, h2)
	}
}

func TestDynamicSchemaActivator_ChangedContentSwapsAtomically(t *testing.T) {
	activator, mux := newActivator(t)

	schema := kindaddon.Schema()
	h1, err := activator.Activate(context.Background(), schema)
	if err != nil {
		t.Fatalf("first Activate: %v", err)
	}

	hash1, _ := activator.ContentHash(h1.ServiceName)

	schema.ProtoFiles = map[string]string{
		"cluster_spec.proto": `syntax = "proto3"; message ClusterSpecV2 { string name = 1; }`,
	}
	schema.EntryFile = "cluster_spec.proto"
	schema.SpecMessage = "ClusterSpecV2"

	h2, err := activator.Activate(context.Background(), schema)
	if err != nil {
		t.Fatalf("second Activate: %v", err)
	}

	if h2.ServiceName != h1.ServiceName {
		t.Errorf("service name changed: %q vs %q", h1.ServiceName, h2.ServiceName)
	}

	hash2, _ := activator.ContentHash(h2.ServiceName)
	if hash1 == hash2 {
		t.Error("expected content hash to change after schema update")
	}

	if _, ok := mux.ServiceInfo()["fleetshift.v1.KindClusterService"]; !ok {
		t.Error("expected KindClusterService still in mux after atomic swap")
	}
}

func TestDynamicSchemaActivator_ReactivateAfterDeactivate(t *testing.T) {
	activator, mux := newActivator(t)

	schema := kindaddon.Schema()
	handle, err := activator.Activate(context.Background(), schema)
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	activator.Deactivate(handle)

	h2, err := activator.Activate(context.Background(), schema)
	if err != nil {
		t.Fatalf("re-Activate: %v", err)
	}
	if h2.ServiceName != handle.ServiceName {
		t.Errorf("service name changed: %q vs %q", handle.ServiceName, h2.ServiceName)
	}
	if _, ok := mux.ServiceInfo()["fleetshift.v1.KindClusterService"]; !ok {
		t.Error("expected KindClusterService in mux after re-activation")
	}
}

func TestDynamicSchemaActivator_EmptyProtoFilesReturnsError(t *testing.T) {
	activator, _ := newActivator(t)

	schema := kindaddon.Schema()
	schema.ProtoFiles = nil

	_, err := activator.Activate(context.Background(), schema)
	if err == nil {
		t.Fatal("expected error for schema with no proto files")
	}
}

func TestSchemaContentHash_Deterministic(t *testing.T) {
	s := domain.ManagedResourceSchema{
		ResourceType: "clusters",
		Singular:     "Cluster",
		Plural:       "Clusters",
		SpecMessage:  "ClusterSpec",
		ProtoFiles:   map[string]string{"a.proto": "syntax=\"proto3\";", "b.proto": "message B {}"},
	}

	h1 := managedresource.SchemaContentHash(s)
	h2 := managedresource.SchemaContentHash(s)
	if h1 != h2 {
		t.Fatalf("non-deterministic: %s vs %s", h1, h2)
	}

	s2 := s
	s2.SpecMessage = "ClusterSpecV2"
	h3 := managedresource.SchemaContentHash(s2)
	if h1 == h3 {
		t.Fatal("expected different hash for different SpecMessage")
	}
}

// httpStatus is a helper that makes a GET request and returns the
// status code. The gRPC handler will error (no Resources dep) but the
// HTTP proxy translates that to a non-404 code, which is enough to
// prove the route exists.
func httpStatus(t *testing.T, url string) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func TestDynamicSchemaActivator_ActivateRegistersHTTPRoutes(t *testing.T) {
	env := newActivatorWithHTTP(t)

	schema := kindaddon.Schema()
	_, err := env.activator.Activate(context.Background(), schema)
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}

	code := httpStatus(t, env.httpURL+"/v1/kindClusters/test-id")
	if code == http.StatusNotFound {
		t.Fatal("expected route to exist after Activate, got 404")
	}
}

func TestDynamicSchemaActivator_DeactivateRemovesHTTPRoutes(t *testing.T) {
	env := newActivatorWithHTTP(t)

	schema := kindaddon.Schema()
	handle, err := env.activator.Activate(context.Background(), schema)
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}

	env.activator.Deactivate(handle)

	code := httpStatus(t, env.httpURL+"/v1/kindClusters/test-id")
	if code != http.StatusNotFound {
		t.Errorf("expected 404 after Deactivate, got %d", code)
	}
}

func TestDynamicSchemaActivator_ChangedContentSwapsHTTPRoutes(t *testing.T) {
	env := newActivatorWithHTTP(t)

	schema := kindaddon.Schema()
	_, err := env.activator.Activate(context.Background(), schema)
	if err != nil {
		t.Fatalf("first Activate: %v", err)
	}

	code1 := httpStatus(t, env.httpURL+"/v1/kindClusters/test-id")
	if code1 == http.StatusNotFound {
		t.Fatal("expected route to exist after first Activate")
	}

	schema.ProtoFiles = map[string]string{
		"cluster_spec.proto": `syntax = "proto3"; message ClusterSpecV2 { string name = 1; }`,
	}
	schema.EntryFile = "cluster_spec.proto"
	schema.SpecMessage = "ClusterSpecV2"

	_, err = env.activator.Activate(context.Background(), schema)
	if err != nil {
		t.Fatalf("second Activate (changed): %v", err)
	}

	code2 := httpStatus(t, env.httpURL+"/v1/kindClusters/test-id")
	if code2 == http.StatusNotFound {
		t.Fatal("expected route to survive atomic swap, got 404")
	}
}

// ---------------------------------------------------------------------------
// Full-stack activator tests — prove schema swap changes request handling
// ---------------------------------------------------------------------------

const widgetTargetType domain.TargetType = "widget-target"

type activatorResourceEnv struct {
	activator *managedresource.DynamicSchemaActivator
	conn      *grpc.ClientConn
	typeSvc   *application.ManagedResourceTypeService
}

// newActivatorWithResources creates an activator backed by a real
// ManagedResourceService (SQLite + memworkflow) and a bufconn gRPC
// server. This lets tests send requests that exercise the full handler
// path including protovalidate.
func newActivatorWithResources(t *testing.T) activatorResourceEnv {
	t.Helper()

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())
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
	router.Register(widgetTargetType, recordingAgent)

	reg := &memworkflow.Registry{}
	recordingAgent.Reporter = application.NewDeliveryReportService(store, reg)
	orchWf, err := reg.RegisterOrchestration(&domain.OrchestrationWorkflowSpec{
		Store: store, Delivery: router,
		Strategies: domain.StrategyFactory{Store: store}, CleanupSignaler: reg,
	})
	if err != nil {
		t.Fatalf("RegisterOrchestration: %v", err)
	}
	createWf, err := reg.RegisterCreateManagedResource(&domain.CreateManagedResourceWorkflowSpec{
		Store: store, Orchestration: orchWf,
	})
	if err != nil {
		t.Fatalf("RegisterCreateManagedResource: %v", err)
	}
	cleanupWf, err := reg.RegisterDeleteManagedResourceCleanup(&domain.DeleteManagedResourceCleanupWorkflowSpec{Store: store})
	if err != nil {
		t.Fatalf("RegisterDeleteManagedResourceCleanup: %v", err)
	}
	deleteWf, err := reg.RegisterDeleteManagedResource(&domain.DeleteManagedResourceWorkflowSpec{
		Store: store, Orchestration: orchWf, Cleanup: cleanupWf,
	})
	if err != nil {
		t.Fatalf("RegisterDeleteManagedResource: %v", err)
	}

	resourceSvc := &application.ManagedResourceService{
		Store: store, CreateWF: createWf, DeleteWF: deleteWf,
	}

	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("protovalidate.New: %v", err)
	}

	grpcMux := managedresource.NewDynamicServiceMux()

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.UnknownServiceHandler(grpcMux.Handle))
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

	targetSvc := &application.TargetService{Store: store}
	if err := targetSvc.Register(context.Background(), domain.TargetInfo{
		ID: "widget-addon", Type: widgetTargetType, Name: "Widget Addon",
		AcceptedResourceTypes: []domain.ResourceType{"widgets"},
	}); err != nil {
		t.Fatalf("register target: %v", err)
	}

	return activatorResourceEnv{
		activator: &managedresource.DynamicSchemaActivator{
			GRPCMux: grpcMux,
			Deps: managedresource.Deps{
				Resources: resourceSvc,
				Validator: validator,
			},
		},
		conn:    conn,
		typeSvc: &application.ManagedResourceTypeService{Store: store},
	}
}

// widgetDescriptors compiles a widget schema and returns the service
// descriptors needed to construct dynamic gRPC messages. This is
// separate from the activator (which does its own compilation) — it
// just gives the test access to the message descriptors.
func widgetDescriptors(t *testing.T, schema domain.ManagedResourceSchema) *managedresource.ServiceDescriptors {
	t.Helper()
	var entryFile string
	for name := range schema.ProtoFiles {
		entryFile = name
		break
	}
	specDesc, err := managedresource.CompileInline(
		context.Background(),
		schema.ProtoFiles,
		entryFile,
		protoreflect.FullName(schema.SpecMessage),
	)
	if err != nil {
		t.Fatalf("CompileInline: %v", err)
	}
	descs, err := managedresource.BuildServiceDescriptors(&managedresource.ResourceTypeConfig{
		ResourceType: schema.ResourceType,
		Singular:     schema.Singular,
		Plural:       schema.Plural,
		ProtoPackage: "fleetshift.v1",
		SpecMessage:  protoreflect.FullName(schema.SpecMessage),
		SpecDescriptor: specDesc.Message,
	}, specDesc.Message)
	if err != nil {
		t.Fatalf("BuildServiceDescriptors: %v", err)
	}
	return descs
}

func TestDynamicSchemaActivator_SwapChangesRequestHandling(t *testing.T) {
	env := newActivatorWithResources(t)
	ctx := context.Background()

	// v1: name is required.
	v1 := domain.ManagedResourceSchema{
		ResourceType: "widgets",
		Singular:     "Widget",
		Plural:       "Widgets",
		SpecMessage:  "WidgetSpec",
		ProtoFiles: map[string]string{
			"widget_spec.proto": `syntax = "proto3";
import "buf/validate/validate.proto";
message WidgetSpec {
  string name = 1 [(buf.validate.field).required = true];
}`,
		},
		Relation: domain.RegisteredSelfTarget{AddonTarget: "widget-addon"},
	}

	// v2: name is optional.
	v2 := domain.ManagedResourceSchema{
		ResourceType: "widgets",
		Singular:     "Widget",
		Plural:       "Widgets",
		SpecMessage:  "WidgetSpec",
		ProtoFiles: map[string]string{
			"widget_spec.proto": `syntax = "proto3";
message WidgetSpec {
  string name = 1;
}`,
		},
		Relation: domain.RegisteredSelfTarget{AddonTarget: "widget-addon"},
	}

	// Register the widget type in the store so Create can look it up.
	if _, err := env.typeSvc.Create(ctx, application.CreateTypeInput{
		ResourceType: "widgets",
		Relation:     domain.RegisteredSelfTarget{AddonTarget: "widget-addon"},
		Signature:    domain.Signature{},
	}); err != nil {
		t.Fatalf("register widget type: %v", err)
	}

	// Activate v1.
	if _, err := env.activator.Activate(ctx, v1); err != nil {
		t.Fatalf("Activate v1: %v", err)
	}

	v1Descs := widgetDescriptors(t, v1)

	// Build a Create request with an EMPTY name (violates v1's required
	// constraint).
	emptyReq := dynamicpb.NewMessage(v1Descs.CreateRequest)
	emptyReq.Set(v1Descs.CreateRequest.Fields().ByNumber(1),
		protoreflect.ValueOfString("widget-1"))
	resource := dynamicpb.NewMessage(v1Descs.Resource)
	spec := dynamicpb.NewMessage(v1Descs.Spec)
	// name left empty — this is the field under test.
	resource.Set(v1Descs.Resource.Fields().ByName("spec"),
		protoreflect.ValueOfMessage(spec))
	emptyReq.Set(v1Descs.CreateRequest.Fields().ByNumber(2),
		protoreflect.ValueOfMessage(resource))

	resp := dynamicpb.NewMessage(v1Descs.Resource)
	err := env.conn.Invoke(ctx,
		"/fleetshift.v1.WidgetService/CreateWidget", emptyReq, resp)
	if err == nil {
		t.Fatal("expected InvalidArgument from v1 (required name), got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("v1 error = %v, want InvalidArgument", err)
	}

	// Swap to v2 (name is optional). The activator detects the content
	// change and atomically replaces the mux entry.
	if _, err := env.activator.Activate(ctx, v2); err != nil {
		t.Fatalf("Activate v2: %v", err)
	}

	// Send the SAME request (empty name) — should now succeed.
	v2Descs := widgetDescriptors(t, v2)
	emptyReq2 := dynamicpb.NewMessage(v2Descs.CreateRequest)
	emptyReq2.Set(v2Descs.CreateRequest.Fields().ByNumber(1),
		protoreflect.ValueOfString("widget-1"))
	resource2 := dynamicpb.NewMessage(v2Descs.Resource)
	spec2 := dynamicpb.NewMessage(v2Descs.Spec)
	resource2.Set(v2Descs.Resource.Fields().ByName("spec"),
		protoreflect.ValueOfMessage(spec2))
	emptyReq2.Set(v2Descs.CreateRequest.Fields().ByNumber(2),
		protoreflect.ValueOfMessage(resource2))

	resp2 := dynamicpb.NewMessage(v2Descs.Resource)
	err = env.conn.Invoke(ctx,
		"/fleetshift.v1.WidgetService/CreateWidget", emptyReq2, resp2)
	if err != nil {
		st2, _ := status.FromError(err)
		t.Fatalf("v2 CreateWidget failed: code=%v msg=%q (should pass with optional name)",
			st2.Code(), st2.Message())
	}

	nameField := v2Descs.Resource.Fields().ByName("name")
	if got := resp2.Get(nameField).String(); got != "widgets/widget-1" {
		t.Errorf("name = %q, want widgets/widget-1", got)
	}
}

var _ application.SchemaActivator = (*managedresource.DynamicSchemaActivator)(nil)
