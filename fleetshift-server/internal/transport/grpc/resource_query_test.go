package grpc_test

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/extensionresource"
	transportgrpc "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/grpc"
)

type resourceQueryEnv struct {
	client  pb.ResourceQueryServiceClient
	httpMux *runtime.ServeMux
}

func setupResourceQuery(t *testing.T) *resourceQueryEnv {
	t.Helper()

	db := sqlite.OpenTestDB(t)
	registry := extensionresource.NewActiveResourceRegistry()
	store := &sqlite.Store{DB: db, SchemaProvider: registry}

	cfg := kindClusterConfig(t)
	built, err := extensionresource.Build(cfg, extensionresource.Deps{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := registry.Register(extensionresource.ActiveResourceVersion{
		APIVersion:                  domain.APIVersion(cfg.Version),
		GRPCServiceName:             cfg.ProtoPackage + "." + cfg.Singular + "Service",
		HTTPPrefix:                  "/apis/" + string(cfg.ResourceType.ServiceName()) + "/" + cfg.Version + "/" + cfg.CollectionID,
		DescriptorPath:              string(built.Descriptors.File.Path()),
		ExtensionServiceDescriptors: built.Descriptors,
		Config:                      cfg,
		QuerySchema: domain.ResourceQuerySchema{
			ResourceType:   cfg.ResourceType,
			ServiceName:    cfg.ResourceType.ServiceName(),
			TypeName:       cfg.Singular,
			APIVersion:     domain.APIVersion(cfg.Version),
			CollectionName: domain.CollectionName(cfg.CollectionID),
			SpecDescriptor: cfg.Capabilities.Management.SpecDescriptor,
		},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	schema := kindaddon.Schema()
	typeDef := domain.NewExtensionResourceType(
		cfg.ResourceType, domain.APIVersion(cfg.Version), domain.CollectionID(cfg.CollectionID), now,
		domain.WithManagement(
			schema.Management.Relation,
			domain.Signature{
				Signer:         domain.FederatedIdentity{Subject: "addon-svc", Issuer: "https://issuer.test"},
				ContentHash:    []byte("hash"),
				SignatureBytes: []byte("sig"),
			},
		),
	)
	if err := tx.ExtensionResources().CreateType(ctx, typeDef); err != nil {
		tx.Rollback()
		t.Fatalf("CreateType: %v", err)
	}
	fID := domain.FulfillmentID("query-api-ful")
	if err := tx.Fulfillments().Create(ctx, domain.FulfillmentFromSnapshot(domain.FulfillmentSnapshot{
		ID: fID, State: domain.FulfillmentStateActive, CreatedAt: now, UpdatedAt: now,
	})); err != nil {
		tx.Rollback()
		t.Fatalf("Create fulfillment: %v", err)
	}
	er := domain.NewExtensionResource(domain.NewExtensionResourceUID(), cfg.ResourceType,
		domain.ResourceName("clusters/query-1"), now, domain.WithManagedState(fID))
	if _, err := er.RecordIntent(json.RawMessage(`{"name":"query-1"}`), now); err != nil {
		tx.Rollback()
		t.Fatalf("RecordIntent: %v", err)
	}
	if err := tx.ExtensionResources().Create(ctx, er); err != nil {
		tx.Rollback()
		t.Fatalf("Create resource: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	server := &transportgrpc.ResourceQueryServer{
		Queries:  application.NewResourceQueryService(store),
		Registry: registry,
	}

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterResourceQueryServiceServer(srv, server)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	gwMux := runtime.NewServeMux()
	if err := pb.RegisterResourceQueryServiceHandlerServer(context.Background(), gwMux, server); err != nil {
		t.Fatalf("RegisterResourceQueryServiceHandlerServer: %v", err)
	}

	return &resourceQueryEnv{
		client:  pb.NewResourceQueryServiceClient(conn),
		httpMux: gwMux,
	}
}

func kindClusterConfig(t *testing.T) *extensionresource.ResourceTypeConfig {
	t.Helper()
	schema := kindaddon.Schema()
	desc, err := dynamicapi.CompileInline(
		context.Background(),
		schema.ProtoFiles,
		schema.EntryFile,
		protoreflect.FullName(schema.Management.SpecMessage),
	)
	if err != nil {
		t.Fatalf("CompileInline: %v", err)
	}
	return &extensionresource.ResourceTypeConfig{
		CollectionConfig: dynamicapi.CollectionConfig{
			Version:      schema.Version,
			CollectionID: schema.CollectionID,
			Singular:     schema.Singular,
			Plural:       schema.Plural,
		},
		ResourceType: kindaddon.ClusterResourceType,
		ProtoPackage: schema.ProtoPackage,
		Capabilities: extensionresource.ResourceCapabilities{
			Management: &extensionresource.ManagementCapabilityConfig{
				SpecMessage:    schema.Management.SpecMessage,
				SpecDescriptor: desc.Message,
			},
		},
	}
}

func TestResourceQuery_InvalidScope(t *testing.T) {
	env := setupResourceQuery(t)
	_, err := env.client.QueryResources(context.Background(), &pb.QueryResourcesRequest{
		Scope: "clusters",
	})
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("err = %v, want status", err)
	}
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", st.Code())
	}
}

func TestResourceQuery_HappyPathGRPC(t *testing.T) {
	env := setupResourceQuery(t)
	resp, err := env.client.QueryResources(context.Background(), &pb.QueryResourcesRequest{
		Scope:    "-",
		Filter:   `resource_type == "kind.fleetshift.io/Cluster"`,
		PageSize: 10,
	})
	if err != nil {
		t.Fatalf("QueryResources: %v", err)
	}
	if len(resp.Resources) != 1 {
		t.Fatalf("len(resources) = %d, want 1", len(resp.Resources))
	}
	r := resp.Resources[0]
	if r.ResourceType != "kind.fleetshift.io/Cluster" {
		t.Errorf("resource_type = %q", r.ResourceType)
	}
	if r.Resource == nil {
		t.Fatal("resource body is nil")
	}
	if r.Resource.Fields["name"].GetStringValue() != "clusters/query-1" {
		t.Errorf("resource.name = %q", r.Resource.Fields["name"].GetStringValue())
	}
	if r.Resource.Fields["spec"] == nil {
		t.Error("resource.spec missing")
	}
	// Nested inventory message must not appear — observed state is
	// inlined when the type declares inventory capability.
	if _, ok := r.Resource.Fields["inventory"]; ok {
		t.Error("nested inventory must not appear on Struct body")
	}
}

func TestResourceQuery_HappyPathHTTP(t *testing.T) {
	env := setupResourceQuery(t)
	req := httptest.NewRequest(http.MethodGet,
		"/apis/fleetshift.io/v1/-:queryResources?filter=resource_type%20%3D%3D%20%22kind.fleetshift.io%2FCluster%22&page_size=10",
		nil)
	rr := httptest.NewRecorder()
	env.httpMux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		body, _ := io.ReadAll(rr.Body)
		t.Fatalf("HTTP status = %d, body = %s", rr.Code, body)
	}
	var resp pb.QueryResourcesResponse
	if err := protojson.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("protojson unmarshal: %v; body=%s", err, rr.Body.String())
	}
	if len(resp.Resources) != 1 {
		t.Fatalf("len(resources) = %d, want 1; body=%s", len(resp.Resources), rr.Body.String())
	}
}

func TestResourceQuery_InactiveTypeReturnsEmptyPage(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	registry := extensionresource.NewActiveResourceRegistry()
	store := &sqlite.Store{DB: db, SchemaProvider: registry}
	ctx := context.Background()

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	rt := domain.ResourceType("kind.fleetshift.io/Cluster")
	if err := tx.ExtensionResources().CreateType(ctx, domain.NewExtensionResourceType(
		rt, "v1", "clusters", now, domain.WithInventory(),
	)); err != nil {
		tx.Rollback()
		t.Fatalf("CreateType: %v", err)
	}
	if err := tx.ExtensionResources().Create(ctx, domain.NewExtensionResource(
		domain.NewExtensionResourceUID(), rt, "clusters/orphan", now,
	)); err != nil {
		tx.Rollback()
		t.Fatalf("Create: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterResourceQueryServiceServer(srv, &transportgrpc.ResourceQueryServer{
		Queries:  application.NewResourceQueryService(store),
		Registry: registry,
	})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	client := pb.NewResourceQueryServiceClient(conn)
	// Empty activated set must not surface inactive store rows (and
	// must not FailedPrecondition on projection of those rows).
	resp, err := client.QueryResources(ctx, &pb.QueryResourcesRequest{Scope: "-", PageSize: 10})
	if err != nil {
		t.Fatalf("QueryResources: %v", err)
	}
	if len(resp.Resources) != 0 {
		t.Fatalf("len(resources) = %d, want 0 when no types are activated", len(resp.Resources))
	}
}
