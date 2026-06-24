package managedresource_test

import (
	"context"
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

	gcphcpaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/gcphcp"
	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/memworkflow"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/testutil"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/managedresource"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/platformresource"
)

func widgetRel() domain.RegisteredSelfTarget {
	return domain.NewRegisteredSelfTarget("widget-addon", "widgets")
}

func newActivator(t *testing.T) (*managedresource.DynamicSchemaActivator, *dynamicapi.DynamicServiceMux) {
	t.Helper()
	mux := dynamicapi.NewDynamicServiceMux()
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
	grpcMux   *dynamicapi.DynamicServiceMux
	httpMux   *dynamicapi.DynamicHTTPMux
	httpURL   string
}

// newActivatorWithHTTP creates an activator backed by both a gRPC mux
// and an HTTP mux with a live TCP gRPC server for the proxy to connect
// to. The returned httpURL is the base URL for the httptest server.
func newActivatorWithHTTP(t *testing.T) activatorHTTPEnv {
	t.Helper()
	grpcMux := dynamicapi.NewDynamicServiceMux()

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

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial grpc: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	httpMux := dynamicapi.NewDynamicHTTPMux(nil, conn)

	ts := httptest.NewServer(httpMux.ServeMux())
	t.Cleanup(ts.Close)

	return activatorHTTPEnv{
		activator: &managedresource.DynamicSchemaActivator{
			GRPCMux: grpcMux,
			HTTPMux: httpMux,
			Deps:    managedresource.Deps{Validator: validator},
		},
		grpcMux: grpcMux,
		httpMux: httpMux,
		httpURL: ts.URL,
	}
}

func newActivatorWithHTTPAndPlatform(t *testing.T) activatorHTTPEnv {
	t.Helper()

	env := newActivatorWithHTTP(t)
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}
	env.activator.PlatformDeps = platformresource.Deps{
		Resources: application.NewPlatformResourceService(store),
	}

	return env
}

func TestDynamicSchemaActivator_ActivateRegistersService(t *testing.T) {
	activator, mux := newActivator(t)

	schema := kindaddon.Schema()
	id, err := activator.Activate(context.Background(), schema)
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}

	if id != "kind.fleetshift.v1.ClusterService" {
		t.Errorf("registration ID = %q, want kind.fleetshift.v1.ClusterService", id)
	}

	info := mux.ServiceInfo()
	si, ok := info["kind.fleetshift.v1.ClusterService"]
	if !ok {
		t.Fatal("expected ClusterService in mux ServiceInfo after Activate")
	}
	if len(si.Methods) != 5 {
		t.Errorf("method count = %d, want 5", len(si.Methods))
	}
}

func TestDynamicSchemaActivator_DeactivateRemovesService(t *testing.T) {
	activator, mux := newActivator(t)

	schema := kindaddon.Schema()
	id, err := activator.Activate(context.Background(), schema)
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}

	activator.Deactivate(id)

	if _, ok := mux.ServiceInfo()["kind.fleetshift.v1.ClusterService"]; ok {
		t.Error("expected ClusterService removed from mux after Deactivate")
	}

	if _, ok := activator.ContentHash("kind.fleetshift.v1.ClusterService"); ok {
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
	id1, err := activator.Activate(context.Background(), schema)
	if err != nil {
		t.Fatalf("first Activate: %v", err)
	}

	hash1, _ := activator.ContentHash(string(id1))

	schema.ProtoFiles = map[string]string{
		"cluster_spec.proto": `syntax = "proto3"; message ClusterSpecV2 { string name = 1; }`,
	}
	schema.EntryFile = "cluster_spec.proto"
	schema.SpecMessage = "ClusterSpecV2"

	id2, err := activator.Activate(context.Background(), schema)
	if err != nil {
		t.Fatalf("second Activate: %v", err)
	}

	if id2 != id1 {
		t.Errorf("registration ID changed: %q vs %q", id1, id2)
	}

	hash2, _ := activator.ContentHash(string(id2))
	if hash1 == hash2 {
		t.Error("expected content hash to change after schema update")
	}

	if _, ok := mux.ServiceInfo()["kind.fleetshift.v1.ClusterService"]; !ok {
		t.Error("expected ClusterService still in mux after atomic swap")
	}
}

func TestDynamicSchemaActivator_ReactivateAfterDeactivate(t *testing.T) {
	activator, mux := newActivator(t)

	schema := kindaddon.Schema()
	id, err := activator.Activate(context.Background(), schema)
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	activator.Deactivate(id)

	id2, err := activator.Activate(context.Background(), schema)
	if err != nil {
		t.Fatalf("re-Activate: %v", err)
	}
	if id2 != id {
		t.Errorf("registration ID changed: %q vs %q", id, id2)
	}
	if _, ok := mux.ServiceInfo()["kind.fleetshift.v1.ClusterService"]; !ok {
		t.Error("expected ClusterService in mux after re-activation")
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
		ResourceType: "test.fleetshift.io/Cluster",
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

	code := httpStatus(t, env.httpURL+"/apis/kind.fleetshift.io/v1/clusters/test-id")
	if code == http.StatusNotFound {
		t.Fatal("expected canonical route to exist after Activate, got 404")
	}
}

func TestDynamicSchemaActivator_RegistersCanonicalHTTPRoute(t *testing.T) {
	env := newActivatorWithHTTP(t)

	schema := kindaddon.Schema()
	_, err := env.activator.Activate(context.Background(), schema)
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}

	code := httpStatus(t, env.httpURL+"/apis/kind.fleetshift.io/v1/clusters/test-id")
	if code == http.StatusNotFound {
		t.Fatal("expected /apis/kind.fleetshift.io/v1/clusters/test-id to route, got 404")
	}
}

func TestDynamicSchemaActivator_DeactivateRemovesHTTPRoutes(t *testing.T) {
	env := newActivatorWithHTTP(t)

	schema := kindaddon.Schema()
	id, err := env.activator.Activate(context.Background(), schema)
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}

	env.activator.Deactivate(id)

	code := httpStatus(t, env.httpURL+"/apis/kind.fleetshift.io/v1/clusters/test-id")
	if code != http.StatusNotFound {
		t.Errorf("expected 404 on canonical route after Deactivate, got %d", code)
	}
}

func TestDynamicSchemaActivator_ChangedContentSwapsHTTPRoutes(t *testing.T) {
	env := newActivatorWithHTTP(t)

	schema := kindaddon.Schema()
	_, err := env.activator.Activate(context.Background(), schema)
	if err != nil {
		t.Fatalf("first Activate: %v", err)
	}

	code1 := httpStatus(t, env.httpURL+"/apis/kind.fleetshift.io/v1/clusters/test-id")
	if code1 == http.StatusNotFound {
		t.Fatal("expected canonical route to exist after first Activate")
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

	code2 := httpStatus(t, env.httpURL+"/apis/kind.fleetshift.io/v1/clusters/test-id")
	if code2 == http.StatusNotFound {
		t.Fatal("expected canonical route to survive atomic swap, got 404")
	}
}

func TestDynamicSchemaActivator_ReplaceWithChangedHTTPIdentity(t *testing.T) {
	env := newActivatorWithHTTP(t)

	schema := kindaddon.Schema()
	_, err := env.activator.Activate(context.Background(), schema)
	if err != nil {
		t.Fatalf("Activate v1: %v", err)
	}

	oldPrefix := "/apis/kind.fleetshift.io/v1/clusters"
	code := httpStatus(t, env.httpURL+oldPrefix+"/test-id")
	if code == http.StatusNotFound {
		t.Fatal("expected old canonical route to exist after first Activate, got 404")
	}

	// Change the API service name (and thus the HTTP prefix) while
	// keeping the same gRPC service name. This simulates a transport
	// identity change that previously leaked the old HTTP route.
	schema.APIServiceName = "kindv2.fleetshift.io"
	schema.ProtoFiles = map[string]string{
		"cluster_spec.proto": `syntax = "proto3"; message KindClusterSpecV2 { string name = 1; }`,
	}
	schema.EntryFile = "cluster_spec.proto"
	schema.SpecMessage = "KindClusterSpecV2"

	_, err = env.activator.Activate(context.Background(), schema)
	if err != nil {
		t.Fatalf("Activate v2: %v", err)
	}

	newPrefix := "/apis/kindv2.fleetshift.io/v1/clusters"
	newCode := httpStatus(t, env.httpURL+newPrefix+"/test-id")
	if newCode == http.StatusNotFound {
		t.Fatal("expected new canonical route to be routable after replace, got 404")
	}

	oldCode := httpStatus(t, env.httpURL+oldPrefix+"/test-id")
	if oldCode != http.StatusNotFound {
		t.Errorf("expected old canonical route to return 404 after replace, got %d", oldCode)
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

	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	recordingAgent := &sqlite.RecordingDeliveryService{
		Store: store,
		Now:   func() time.Time { return time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC) },
	}
	router := delivery.NewRoutingDeliveryService()
	router.Register(widgetTargetType, recordingAgent)

	reg := &memworkflow.Registry{}
	recordingAgent.Reporter = application.NewDeliveryReportService(store, reg)
	orchWf, err := reg.RegisterOrchestration(domain.NewOrchestrationWorkflowSpec(
		store, router, domain.StrategyFactory{Store: store}, reg,
		domain.WithAckRetryInterval(5*time.Second),
	))
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

	grpcMux := dynamicapi.NewDynamicServiceMux()

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
	if err := targetSvc.Register(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID: "widget-addon", Type: widgetTargetType, Name: "Widget Addon",
		AcceptedManifestTypes: []domain.ManifestType{"widgets"},
	})); err != nil {
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
		typeSvc: application.NewManagedResourceTypeService(store),
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
	specDesc, err := dynamicapi.CompileInline(
		context.Background(),
		schema.ProtoFiles,
		entryFile,
		protoreflect.FullName(schema.SpecMessage),
	)
	if err != nil {
		t.Fatalf("CompileInline: %v", err)
	}
	descs, err := managedresource.BuildServiceDescriptors(&managedresource.ResourceTypeConfig{
		CollectionConfig: dynamicapi.CollectionConfig{
			Version:      schema.Version,
			CollectionID: schema.CollectionID,
			Singular:     schema.Singular,
			Plural:       schema.Plural,
		},
		ResourceType:   schema.ResourceType,
		APIServiceName: schema.APIServiceName,
		ProtoPackage:   schema.ProtoPackage,
		SpecMessage:    protoreflect.FullName(schema.SpecMessage),
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
		ResourceType:   "test.fleetshift.io/Widget",
		APIServiceName: "fleetshift.io",
		ProtoPackage:   "fleetshift.v1",
		Version:        "v1",
		CollectionID:   "widgets",
		Singular:       "Widget",
		Plural:         "Widgets",
		SpecMessage:    "WidgetSpec",
		ProtoFiles: map[string]string{
			"widget_spec.proto": `syntax = "proto3";
import "buf/validate/validate.proto";
message WidgetSpec {
  string name = 1 [(buf.validate.field).required = true];
}`,
		},
		Relation: widgetRel(),
	}

	// v2: name is optional.
	v2 := domain.ManagedResourceSchema{
		ResourceType:   "test.fleetshift.io/Widget",
		APIServiceName: "fleetshift.io",
		ProtoPackage:   "fleetshift.v1",
		Version:        "v1",
		CollectionID:   "widgets",
		Singular:       "Widget",
		Plural:         "Widgets",
		SpecMessage:    "WidgetSpec",
		ProtoFiles: map[string]string{
			"widget_spec.proto": `syntax = "proto3";
message WidgetSpec {
  string name = 1;
}`,
		},
		Relation: widgetRel(),
	}

	// Register the widget type in the store so Create can look it up.
	if _, err := env.typeSvc.Create(ctx, application.CreateTypeInput{
		ResourceType:   "test.fleetshift.io/Widget",
		Relation:       widgetRel(),
		Signature:      domain.Signature{},
		APIServiceName: "fleetshift.io",
		APIVersion:     "v1",
		CollectionID:   "widgets",
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

// ---------------------------------------------------------------------------
// Platform refcounting tests
// ---------------------------------------------------------------------------

// newActivatorWithPlatform creates an activator wired with a real
// PlatformResourceService backed by an in-memory SQLite store. This is
// the minimal setup needed for the platform refcounting code path.
func newActivatorWithPlatform(t *testing.T) (*managedresource.DynamicSchemaActivator, *dynamicapi.DynamicServiceMux) {
	t.Helper()
	mux := dynamicapi.NewDynamicServiceMux()
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("protovalidate.New: %v", err)
	}
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}
	return &managedresource.DynamicSchemaActivator{
		GRPCMux:      mux,
		Deps:         managedresource.Deps{Validator: validator},
		PlatformDeps: platformresource.Deps{Resources: application.NewPlatformResourceService(store)},
	}, mux
}

func TestDualRegistration(t *testing.T) {
	activator, mux := newActivatorWithPlatform(t)

	schema := kindaddon.Schema()
	id, err := activator.Activate(context.Background(), schema)
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}

	info := mux.ServiceInfo()

	// Extension gRPC service must be routable.
	if _, ok := info["kind.fleetshift.v1.ClusterService"]; !ok {
		t.Error("expected extension ClusterService in mux after Activate")
	}

	// Platform gRPC service must also be routable.
	if _, ok := info["fleetshift.v1.PlatformClusterService"]; !ok {
		t.Error("expected PlatformClusterService in mux after Activate")
	}

	if id != "kind.fleetshift.v1.ClusterService" {
		t.Errorf("registration ID = %q, want kind.fleetshift.v1.ClusterService", id)
	}
}

func TestPlatformRefCounting(t *testing.T) {
	activator, mux := newActivatorWithPlatform(t)
	ctx := context.Background()
	const platformSvc = "fleetshift.v1.PlatformClusterService"

	// Step 1: activate Kind (collection: "clusters")
	kindID, err := activator.Activate(ctx, kindaddon.Schema())
	if err != nil {
		t.Fatalf("Activate Kind: %v", err)
	}
	if _, ok := mux.ServiceInfo()[platformSvc]; !ok {
		t.Fatal("Kind activation should create the platform service (refcount 0→1)")
	}

	// Step 2: activate GCP HCP (same collection: "clusters"), but with
	// a different extension API version. This should still share the same
	// platform service.
	gcphcpSchema := gcphcpaddon.Schema("gcphcp-test")
	gcphcpSchema.Version = "v2"

	gcpID, err := activator.Activate(ctx, gcphcpSchema)
	if err != nil {
		t.Fatalf("Activate GCPHCP: %v", err)
	}

	// Step 3: one platform service exists
	if _, ok := mux.ServiceInfo()[platformSvc]; !ok {
		t.Fatal("platform service should be routable after two extensions activated")
	}

	// Step 4: deactivate Kind → platform still alive (refcount 2→1)
	activator.Deactivate(kindID)
	if _, ok := mux.ServiceInfo()[platformSvc]; !ok {
		t.Fatal("platform service should survive after deactivating one of two extensions")
	}

	// Step 5: deactivate GCP HCP → platform removed (refcount 1→0)
	activator.Deactivate(gcpID)

	// Step 6: platform no longer routable
	if _, ok := mux.ServiceInfo()[platformSvc]; ok {
		t.Error("platform service should be removed after all extensions deactivated")
	}
}

func TestReplaceDoesNotDropPlatform(t *testing.T) {
	activator, mux := newActivatorWithPlatform(t)
	ctx := context.Background()
	const platformSvc = "fleetshift.v1.PlatformClusterService"

	// Activate Kind.
	_, err := activator.Activate(ctx, kindaddon.Schema())
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if _, ok := mux.ServiceInfo()[platformSvc]; !ok {
		t.Fatal("platform service should exist after initial activation")
	}

	// Replace: modify a proto file comment so content hash changes, but
	// keep the same CollectionID.
	modified := kindaddon.Schema()
	for k, v := range modified.ProtoFiles {
		modified.ProtoFiles[k] = "// replaced for test\n" + v
	}

	_, err = activator.Activate(ctx, modified)
	if err != nil {
		t.Fatalf("Activate (replace): %v", err)
	}

	if _, ok := mux.ServiceInfo()[platformSvc]; !ok {
		t.Error("platform service should survive a content-only schema replace")
	}
}

func TestPlatformReflection(t *testing.T) {
	mux := dynamicapi.NewDynamicServiceMux()
	fileReg := dynamicapi.NewDynamicFileRegistry()
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("protovalidate.New: %v", err)
	}
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	activator := &managedresource.DynamicSchemaActivator{
		GRPCMux:      mux,
		FileRegistry: fileReg,
		Deps:         managedresource.Deps{Validator: validator},
		PlatformDeps: platformresource.Deps{Resources: application.NewPlatformResourceService(store)},
	}

	schema := kindaddon.Schema()
	id, err := activator.Activate(context.Background(), schema)
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}

	const platDescPath = "dynamic/fleetshift/v1/platform_cluster_service.proto"
	fd, err := fileReg.FindFileByPath(platDescPath)
	if err != nil {
		t.Fatalf("platform file descriptor not resolvable after Activate: %v", err)
	}
	if string(fd.Path()) != platDescPath {
		t.Errorf("descriptor path = %q, want %q", fd.Path(), platDescPath)
	}

	activator.Deactivate(id)

	if _, err := fileReg.FindFileByPath(platDescPath); err == nil {
		t.Error("platform file descriptor should not be resolvable after Deactivate")
	}
}

var _ application.SchemaActivator = (*managedresource.DynamicSchemaActivator)(nil)

// ---------------------------------------------------------------------------
// Platform version selection tests
// ---------------------------------------------------------------------------

// TestPlatformHTTPVersionIsFixed verifies that the platform API route
// comes from the activator, not from the extension schema's Version.
func TestPlatformHTTPVersionIsFixed(t *testing.T) {
	env := newActivatorWithHTTPAndPlatform(t)

	schema := kindaddon.Schema()
	schema.Version = "v2"

	if _, err := env.activator.Activate(context.Background(), schema); err != nil {
		t.Fatalf("Activate: %v", err)
	}

	platformV1 := env.httpURL + "/apis/fleetshift.io/" + platformresource.APIVersion + "/clusters"
	if code := httpStatus(t, platformV1); code != http.StatusOK {
		t.Fatalf("expected platform route %q to return 200, got %d", platformV1, code)
	}

	platformV2 := env.httpURL + "/apis/fleetshift.io/v2/clusters"
	if code := httpStatus(t, platformV2); code != http.StatusNotFound {
		t.Fatalf("expected platform route %q to be absent, got status %d", platformV2, code)
	}
}

// ---------------------------------------------------------------------------
// Cross-API contract: extension create → platform API visibility
// ---------------------------------------------------------------------------

type activatorPlatformResourceEnv struct {
	activator *managedresource.DynamicSchemaActivator
	conn      *grpc.ClientConn
	typeSvc   *application.ManagedResourceTypeService
	store     domain.Store
}

// newActivatorWithResourcesAndPlatform creates an activator wired with
// both a ManagedResourceService and a PlatformResourceService backed
// by the same SQLite store. This lets tests exercise the cross-API
// contract: creating a managed resource through the extension gRPC
// service should make it visible through the platform resource API.
func newActivatorWithResourcesAndPlatform(t *testing.T) activatorPlatformResourceEnv {
	t.Helper()

	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	recordingAgent := &sqlite.RecordingDeliveryService{
		Store: store,
		Now:   func() time.Time { return time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC) },
	}
	router := delivery.NewRoutingDeliveryService()
	router.Register(widgetTargetType, recordingAgent)

	reg := &memworkflow.Registry{}
	recordingAgent.Reporter = application.NewDeliveryReportService(store, reg)
	orchWf, err := reg.RegisterOrchestration(domain.NewOrchestrationWorkflowSpec(
		store, router, domain.StrategyFactory{Store: store}, reg,
		domain.WithAckRetryInterval(5*time.Second),
	))
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
	platformResourceSvc := application.NewPlatformResourceService(store)

	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("protovalidate.New: %v", err)
	}

	grpcMux := dynamicapi.NewDynamicServiceMux()

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
	if err := targetSvc.Register(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID: "widget-addon", Type: widgetTargetType, Name: "Widget Addon",
		AcceptedManifestTypes: []domain.ManifestType{"widgets"},
	})); err != nil {
		t.Fatalf("register target: %v", err)
	}

	return activatorPlatformResourceEnv{
		activator: &managedresource.DynamicSchemaActivator{
			GRPCMux: grpcMux,
			Deps: managedresource.Deps{
				Resources: resourceSvc,
				Validator: validator,
			},
			PlatformDeps: platformresource.Deps{
				Resources: platformResourceSvc,
			},
		},
		conn:    conn,
		typeSvc: application.NewManagedResourceTypeService(store),
		store:   store,
	}
}

// --- Shared helpers for cross-API contract tests ---

func platformTestWidgetSchema() domain.ManagedResourceSchema {
	return domain.ManagedResourceSchema{
		ResourceType:   "test.fleetshift.io/Widget",
		APIServiceName: "fleetshift.io",
		ProtoPackage:   "fleetshift.v1",
		Version:        "v1",
		CollectionID:   "widgets",
		Singular:       "Widget",
		Plural:         "Widgets",
		SpecMessage:    "WidgetSpec",
		ProtoFiles: map[string]string{
			"widget_spec.proto": `syntax = "proto3";
message WidgetSpec {
  string name = 1;
}`,
		},
		Relation: widgetRel(),
	}
}

func platformWidgetDescs(t *testing.T) *platformresource.ServiceDescriptors {
	t.Helper()
	descs, err := platformresource.BuildServiceDescriptors(
		&platformresource.Config{
			CollectionConfig: dynamicapi.CollectionConfig{
				Version:      "v1",
				CollectionID: "widgets",
				Singular:     "Widget",
				Plural:       "Widgets",
			},
		})
	if err != nil {
		t.Fatalf("BuildPlatformServiceDescriptors: %v", err)
	}
	return descs
}

// createWidgetViaExtension sends a CreateWidget gRPC request through the
// extension service and returns the extension descriptors for follow-up
// requests (e.g. delete).
func createWidgetViaExtension(t *testing.T, ctx context.Context, conn *grpc.ClientConn, schema domain.ManagedResourceSchema, id string) *managedresource.ServiceDescriptors {
	t.Helper()
	descs := widgetDescriptors(t, schema)

	req := dynamicpb.NewMessage(descs.CreateRequest)
	req.Set(descs.CreateRequest.Fields().ByNumber(1),
		protoreflect.ValueOfString(id))
	resource := dynamicpb.NewMessage(descs.Resource)
	spec := dynamicpb.NewMessage(descs.Spec)
	spec.Set(descs.Spec.Fields().ByName("name"),
		protoreflect.ValueOfString("test-"+id))
	resource.Set(descs.Resource.Fields().ByName("spec"),
		protoreflect.ValueOfMessage(spec))
	req.Set(descs.CreateRequest.Fields().ByNumber(2),
		protoreflect.ValueOfMessage(resource))

	resp := dynamicpb.NewMessage(descs.Resource)
	if err := conn.Invoke(ctx,
		"/fleetshift.v1.WidgetService/CreateWidget", req, resp); err != nil {
		t.Fatalf("CreateWidget(%s): %v", id, err)
	}
	return descs
}

func awaitFulfillmentActive(ctx context.Context, t *testing.T, store domain.Store, rt domain.ResourceType, name domain.ResourceName) {
	t.Helper()
	for {
		tx, err := store.BeginReadOnly(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		view, err := tx.ManagedResources().GetView(ctx, rt, name)
		tx.Rollback()
		if err == nil && view.Fulfillment.State() == domain.FulfillmentStateActive {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s/%s fulfillment to reach active", rt, name)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// --- Cross-API contract tests ---

// TestExtensionCreate_VisibleInPlatformAPI exercises the transport-level
// contract that creating a managed resource through an addon's extension
// gRPC service makes it visible in the platform resource API with the
// correct representation metadata.
func TestExtensionCreate_VisibleInPlatformAPI(t *testing.T) {
	env := newActivatorWithResourcesAndPlatform(t)
	ctx, cancel := context.WithTimeout(context.Background(), testutil.ServiceTimeout)
	defer cancel()

	schema := platformTestWidgetSchema()

	// Register the widget type with API identity metadata so the
	// create workflow claims a platform resource identity.
	if _, err := env.typeSvc.Create(ctx, application.CreateTypeInput{
		ResourceType:   "test.fleetshift.io/Widget",
		Relation:       widgetRel(),
		Signature:      domain.Signature{},
		APIServiceName: "fleetshift.io",
		APIVersion:     "v1",
		CollectionID:   "widgets",
	}); err != nil {
		t.Fatalf("register widget type: %v", err)
	}

	if _, err := env.activator.Activate(ctx, schema); err != nil {
		t.Fatalf("Activate: %v", err)
	}

	createWidgetViaExtension(t, ctx, env.conn, schema, "widget-1")

	platDescs := platformWidgetDescs(t)

	// Get via the platform API.
	getReq := dynamicpb.NewMessage(platDescs.GetRequest)
	getReq.Set(platDescs.GetRequest.Fields().ByName("name"),
		protoreflect.ValueOfString("widgets/widget-1"))
	getResp := dynamicpb.NewMessage(platDescs.Resource)
	if err := env.conn.Invoke(ctx,
		"/fleetshift.v1.PlatformWidgetService/GetPlatformWidget", getReq, getResp); err != nil {
		t.Fatalf("GetPlatformWidget: %v", err)
	}

	repsField := platDescs.Resource.Fields().ByName("representations")
	repList := getResp.Get(repsField).List()
	if repList.Len() != 1 {
		t.Fatalf("representations len = %d, want 1", repList.Len())
	}

	rep := repList.Get(0).Message()
	repDesc := repsField.Message()
	if got := rep.Get(repDesc.Fields().ByName("service_name")).String(); got != "fleetshift.io" {
		t.Errorf("representation service_name = %q, want %q", got, "fleetshift.io")
	}
	if got := rep.Get(repDesc.Fields().ByName("version")).String(); got != "v1" {
		t.Errorf("representation version = %q, want %q", got, "v1")
	}
	roles := rep.Get(repDesc.Fields().ByName("roles")).List()
	if roles.Len() != 1 || roles.Get(0).String() != string(domain.RepresentationRoleManaged) {
		var got []string
		for i := range roles.Len() {
			got = append(got, roles.Get(i).String())
		}
		t.Errorf("representation roles = %v, want [managed]", got)
	}

	// Also verify the resource appears in the platform List.
	listReq := dynamicpb.NewMessage(platDescs.ListRequest)
	listResp := dynamicpb.NewMessage(platDescs.ListResponse)
	if err := env.conn.Invoke(ctx,
		"/fleetshift.v1.PlatformWidgetService/ListPlatformWidgets", listReq, listResp); err != nil {
		t.Fatalf("ListPlatformWidgets: %v", err)
	}

	listField := platDescs.ListResponse.Fields().ByNumber(1)
	resourceList := listResp.Get(listField).List()
	if resourceList.Len() != 1 {
		t.Fatalf("ListPlatformWidgets returned %d resources, want 1", resourceList.Len())
	}

	listedName := resourceList.Get(0).Message().Get(platDescs.Resource.Fields().ByName("name")).String()
	if listedName != "widgets/widget-1" {
		t.Errorf("listed name = %q, want %q", listedName, "widgets/widget-1")
	}
}

// TestExtensionDelete_RemovesPlatformRepresentation verifies that
// deleting a managed resource through the extension gRPC service
// removes its representation from the platform resource API. The
// platform resource itself survives — only the active representation
// list becomes empty.
func TestExtensionDelete_RemovesPlatformRepresentation(t *testing.T) {
	env := newActivatorWithResourcesAndPlatform(t)
	ctx, cancel := context.WithTimeout(context.Background(), testutil.ServiceTimeout)
	defer cancel()

	schema := platformTestWidgetSchema()

	if _, err := env.typeSvc.Create(ctx, application.CreateTypeInput{
		ResourceType:   "test.fleetshift.io/Widget",
		Relation:       widgetRel(),
		Signature:      domain.Signature{},
		APIServiceName: "fleetshift.io",
		APIVersion:     "v1",
		CollectionID:   "widgets",
	}); err != nil {
		t.Fatalf("register widget type: %v", err)
	}

	if _, err := env.activator.Activate(ctx, schema); err != nil {
		t.Fatalf("Activate: %v", err)
	}

	extDescs := createWidgetViaExtension(t, ctx, env.conn, schema, "widget-1")

	awaitFulfillmentActive(ctx, t, env.store, "test.fleetshift.io/Widget", "widgets/widget-1")

	// Delete via extension gRPC.
	deleteReq := dynamicpb.NewMessage(extDescs.DeleteRequest)
	deleteReq.Set(extDescs.DeleteRequest.Fields().ByName("name"),
		protoreflect.ValueOfString("widgets/widget-1"))
	deleteResp := dynamicpb.NewMessage(extDescs.Resource)
	if err := env.conn.Invoke(ctx,
		"/fleetshift.v1.WidgetService/DeleteWidget", deleteReq, deleteResp); err != nil {
		t.Fatalf("DeleteWidget: %v", err)
	}

	// Platform Get: resource exists but the representation link is gone.
	platDescs := platformWidgetDescs(t)

	getReq := dynamicpb.NewMessage(platDescs.GetRequest)
	getReq.Set(platDescs.GetRequest.Fields().ByName("name"),
		protoreflect.ValueOfString("widgets/widget-1"))
	getResp := dynamicpb.NewMessage(platDescs.Resource)
	if err := env.conn.Invoke(ctx,
		"/fleetshift.v1.PlatformWidgetService/GetPlatformWidget", getReq, getResp); err != nil {
		t.Fatalf("GetPlatformWidget after delete: %v", err)
	}

	repsField := platDescs.Resource.Fields().ByName("representations")
	if getResp.Get(repsField).List().Len() != 0 {
		t.Errorf("representations len = %d after delete, want 0",
			getResp.Get(repsField).List().Len())
	}

	// Platform List: resource still appears (not soft-deleted at the
	// platform level — only the representation link is removed).
	listReq := dynamicpb.NewMessage(platDescs.ListRequest)
	listResp := dynamicpb.NewMessage(platDescs.ListResponse)
	if err := env.conn.Invoke(ctx,
		"/fleetshift.v1.PlatformWidgetService/ListPlatformWidgets", listReq, listResp); err != nil {
		t.Fatalf("ListPlatformWidgets after delete: %v", err)
	}

	listField := platDescs.ListResponse.Fields().ByNumber(1)
	if listResp.Get(listField).List().Len() != 1 {
		t.Errorf("ListPlatformWidgets after delete = %d resources, want 1 (resource survives representation removal)",
			listResp.Get(listField).List().Len())
	}
}

// TestSchemaDeactivate_PlatformAPIUnroutable verifies that after a
// schema is deactivated (simulating addon disable), the platform gRPC
// service is removed from the mux and calls return Unimplemented.
func TestSchemaDeactivate_PlatformAPIUnroutable(t *testing.T) {
	env := newActivatorWithResourcesAndPlatform(t)
	ctx := context.Background()

	schema := platformTestWidgetSchema()

	id, err := env.activator.Activate(ctx, schema)
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}

	platDescs := platformWidgetDescs(t)

	// Before deactivation: the platform service is routable. A Get for a
	// nonexistent resource returns NotFound (not Unimplemented).
	getReq := dynamicpb.NewMessage(platDescs.GetRequest)
	getReq.Set(platDescs.GetRequest.Fields().ByName("name"),
		protoreflect.ValueOfString("widgets/nonexistent"))
	getResp := dynamicpb.NewMessage(platDescs.Resource)
	err = env.conn.Invoke(ctx,
		"/fleetshift.v1.PlatformWidgetService/GetPlatformWidget", getReq, getResp)
	if err == nil {
		t.Fatal("expected error for nonexistent resource, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.NotFound {
		t.Fatalf("before deactivation: platform Get = code %v, want NotFound (proves service is routable)", st.Code())
	}

	env.activator.Deactivate(id)

	// After deactivation: the platform service is gone.
	getResp2 := dynamicpb.NewMessage(platDescs.Resource)
	err = env.conn.Invoke(ctx,
		"/fleetshift.v1.PlatformWidgetService/GetPlatformWidget", getReq, getResp2)
	if err == nil {
		t.Fatal("expected Unimplemented after deactivation, got nil")
	}
	st2, ok := status.FromError(err)
	if !ok || st2.Code() != codes.Unimplemented {
		t.Fatalf("after deactivation: platform Get = code %v, want Unimplemented", st2.Code())
	}
}
