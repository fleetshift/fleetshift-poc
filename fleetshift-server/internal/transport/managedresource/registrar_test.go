package managedresource_test

import (
	"context"
	"net"
	"testing"
	"time"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
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

const clusterTargetType domain.TargetType = "cluster-addon"

func clusterConfig(t *testing.T) *managedresource.ResourceTypeConfig {
	t.Helper()

	schema := kindaddon.Schema()
	var entryFile string
	for name := range schema.ProtoFiles {
		entryFile = name
		break
	}
	desc, err := managedresource.CompileInline(
		context.Background(),
		schema.ProtoFiles,
		entryFile,
		protoreflect.FullName(schema.SpecMessage),
	)
	if err != nil {
		t.Fatalf("CompileInline: %v", err)
	}

	return &managedresource.ResourceTypeConfig{
		ResourceType:   kindaddon.ClusterResourceType,
		Singular:       schema.Singular,
		Plural:         schema.Plural,
		ProtoPackage:   "fleetshift.v1",
		SpecMessage:    schema.SpecMessage,
		SpecDescriptor: desc.Message,
	}
}

type testEnv struct {
	conn *grpc.ClientConn
	svc  *managedresource.RegisteredService
}

func setup(t *testing.T) *testEnv {
	t.Helper()

	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	recordingAgent := &sqlite.RecordingDeliveryService{
		Store: store,
		Now:   func() time.Time { return time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC) },
	}
	router := delivery.NewRoutingDeliveryService()
	router.Register(clusterTargetType, recordingAgent)

	reg := &memworkflow.Registry{}

	orchSpec := &domain.OrchestrationWorkflowSpec{
		Store:      store,
		Delivery:   router,
		Strategies: domain.StrategyFactory{Store: store},
		Registry:   reg,
	}
	orchWf, err := reg.RegisterOrchestration(orchSpec)
	if err != nil {
		t.Fatalf("RegisterOrchestration: %v", err)
	}

	createMRSpec := &domain.CreateManagedResourceWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
	}
	createMRWf, err := reg.RegisterCreateManagedResource(createMRSpec)
	if err != nil {
		t.Fatalf("RegisterCreateManagedResource: %v", err)
	}

	mrCleanupSpec := &domain.DeleteManagedResourceCleanupWorkflowSpec{Store: store}
	mrCleanupWf, err := reg.RegisterDeleteManagedResourceCleanup(mrCleanupSpec)
	if err != nil {
		t.Fatalf("RegisterDeleteManagedResourceCleanup: %v", err)
	}

	deleteMRSpec := &domain.DeleteManagedResourceWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
		Cleanup:       mrCleanupWf,
	}
	deleteMRWf, err := reg.RegisterDeleteManagedResource(deleteMRSpec)
	if err != nil {
		t.Fatalf("RegisterDeleteManagedResource: %v", err)
	}

	managedResourceSvc := &application.ManagedResourceService{
		Store:          store,
		CreateWF: createMRWf,
		DeleteWF: deleteMRWf,
	}

	targetSvc := &application.TargetService{Store: store}
	if err := targetSvc.Register(context.Background(), domain.TargetInfo{
		ID: "kind-local", Type: clusterTargetType, Name: "Kind Cluster Addon",
		AcceptedResourceTypes: []domain.ResourceType{kindaddon.ClusterResourceType},
	}); err != nil {
		t.Fatalf("register target: %v", err)
	}

	typeSvc := &application.ManagedResourceTypeService{
		Store: store,
	}
	if _, err := typeSvc.Create(context.Background(), application.CreateTypeInput{
		ResourceType: kindaddon.ClusterResourceType,
		Relation: domain.RegisteredSelfTarget{
			AddonTarget: "kind-local",
		},
		Signature: domain.Signature{},
	}); err != nil {
		t.Fatalf("register cluster type: %v", err)
	}

	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("protovalidate.New: %v", err)
	}

	// --- Build and register dynamic service ---

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()

	svc, err := managedresource.BuildAndRegister(srv, clusterConfig(t), managedresource.Deps{
		Resources: managedResourceSvc,
		Validator: validator,
	})
	if err != nil {
		t.Fatalf("BuildAndRegister: %v", err)
	}

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

	return &testEnv{conn: conn, svc: svc}
}

func TestDynamic_CreateThenGet(t *testing.T) {
	env := setup(t)
	ctx := context.Background()

	createReq := dynamicpb.NewMessage(env.svc.Descriptors.CreateRequest)

	// Set kind_cluster_id (field 1)
	idField := env.svc.Descriptors.CreateRequest.Fields().ByNumber(1)
	createReq.Set(idField, protoreflect.ValueOfString("dev-cluster"))

	// Build the resource message with a spec
	resourceField := env.svc.Descriptors.CreateRequest.Fields().ByNumber(2)
	resource := dynamicpb.NewMessage(env.svc.Descriptors.Resource)

	specField := env.svc.Descriptors.Resource.Fields().ByName("spec")
	spec := dynamicpb.NewMessage(env.svc.Descriptors.Spec)
	spec.Set(env.svc.Descriptors.Spec.Fields().ByName("name"), protoreflect.ValueOfString("dev-cluster"))

	resource.Set(specField, protoreflect.ValueOfMessage(spec))
	createReq.Set(resourceField, protoreflect.ValueOfMessage(resource))

	// Invoke the dynamic service via gRPC.
	createResp := dynamicpb.NewMessage(env.svc.Descriptors.Resource)
	err := env.conn.Invoke(ctx, "/fleetshift.v1.KindClusterService/CreateKindCluster", createReq, createResp)
	if err != nil {
		t.Fatalf("CreateKindCluster: %v", err)
	}

	// Verify the response.
	nameField := env.svc.Descriptors.Resource.Fields().ByName("name")
	gotName := createResp.Get(nameField).String()
	if gotName != "kindclusters/dev-cluster" {
		t.Errorf("name = %q, want %q", gotName, "kindclusters/dev-cluster")
	}

	uidField := env.svc.Descriptors.Resource.Fields().ByName("uid")
	if createResp.Get(uidField).String() == "" {
		t.Error("uid is empty, want non-empty UUID")
	}

	stateField := env.svc.Descriptors.Resource.Fields().ByName("state")
	if createResp.Get(stateField).Int() != 1 { // STATE_CREATING
		t.Errorf("state = %d, want 1 (STATE_CREATING)", createResp.Get(stateField).Int())
	}

	reconcilingField := env.svc.Descriptors.Resource.Fields().ByName("reconciling")
	if !createResp.Get(reconcilingField).Bool() {
		t.Error("reconciling = false, want true")
	}

	// Verify spec round-trips through the response.
	respSpec := createResp.Get(specField).Message()
	respJSON, _ := protojson.Marshal(respSpec.Interface())
	t.Logf("response spec JSON: %s", respJSON)
	if respSpec.Get(env.svc.Descriptors.Spec.Fields().ByName("name")).String() != "dev-cluster" {
		t.Error("spec.name not round-tripped correctly")
	}

	// --- GetKindCluster ---

	getReq := dynamicpb.NewMessage(env.svc.Descriptors.GetRequest)
	getNameField := env.svc.Descriptors.GetRequest.Fields().ByName("name")
	getReq.Set(getNameField, protoreflect.ValueOfString("kindclusters/dev-cluster"))

	getResp := dynamicpb.NewMessage(env.svc.Descriptors.Resource)
	err = env.conn.Invoke(ctx, "/fleetshift.v1.KindClusterService/GetKindCluster", getReq, getResp)
	if err != nil {
		t.Fatalf("GetKindCluster: %v", err)
	}

	if getResp.Get(nameField).String() != "kindclusters/dev-cluster" {
		t.Errorf("get name = %q, want %q", getResp.Get(nameField).String(), "kindclusters/dev-cluster")
	}
}

func TestDynamic_ValidationRejectsInvalidSpec(t *testing.T) {
	env := setup(t)
	ctx := context.Background()

	createReq := dynamicpb.NewMessage(env.svc.Descriptors.CreateRequest)
	idField := env.svc.Descriptors.CreateRequest.Fields().ByNumber(1)
	createReq.Set(idField, protoreflect.ValueOfString("bad-cluster"))

	resource := dynamicpb.NewMessage(env.svc.Descriptors.Resource)
	specField := env.svc.Descriptors.Resource.Fields().ByName("spec")
	spec := dynamicpb.NewMessage(env.svc.Descriptors.Spec)
	// name is required — leave it empty to trigger validation failure
	resource.Set(specField, protoreflect.ValueOfMessage(spec))
	createReq.Set(env.svc.Descriptors.CreateRequest.Fields().ByNumber(2), protoreflect.ValueOfMessage(resource))

	resp := dynamicpb.NewMessage(env.svc.Descriptors.Resource)
	err := env.conn.Invoke(ctx, "/fleetshift.v1.KindClusterService/CreateKindCluster", createReq, resp)
	if err == nil {
		t.Fatal("expected error for invalid spec, got nil")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", st.Code())
	}
}

func TestDynamic_ListAndDelete(t *testing.T) {
	env := setup(t)
	ctx := context.Background()

	// Create a cluster first.
	createReq := dynamicpb.NewMessage(env.svc.Descriptors.CreateRequest)
	idField := env.svc.Descriptors.CreateRequest.Fields().ByNumber(1)
	createReq.Set(idField, protoreflect.ValueOfString("cluster-a"))

	resource := dynamicpb.NewMessage(env.svc.Descriptors.Resource)
	specField := env.svc.Descriptors.Resource.Fields().ByName("spec")
	spec := dynamicpb.NewMessage(env.svc.Descriptors.Spec)
	spec.Set(env.svc.Descriptors.Spec.Fields().ByName("name"), protoreflect.ValueOfString("cluster-a"))
	resource.Set(specField, protoreflect.ValueOfMessage(spec))
	createReq.Set(env.svc.Descriptors.CreateRequest.Fields().ByNumber(2), protoreflect.ValueOfMessage(resource))

	createResp := dynamicpb.NewMessage(env.svc.Descriptors.Resource)
	if err := env.conn.Invoke(ctx, "/fleetshift.v1.KindClusterService/CreateKindCluster", createReq, createResp); err != nil {
		t.Fatalf("CreateKindCluster: %v", err)
	}

	// List
	listReq := dynamicpb.NewMessage(env.svc.Descriptors.ListRequest)
	listResp := dynamicpb.NewMessage(env.svc.Descriptors.ListResponse)
	if err := env.conn.Invoke(ctx, "/fleetshift.v1.KindClusterService/ListKindclusters", listReq, listResp); err != nil {
		t.Fatalf("ListKindclusters: %v", err)
	}

	resourcesField := env.svc.Descriptors.ListResponse.Fields().ByNumber(1)
	list := listResp.Get(resourcesField).List()
	if list.Len() != 1 {
		t.Fatalf("list count = %d, want 1", list.Len())
	}

	// Delete
	deleteReq := dynamicpb.NewMessage(env.svc.Descriptors.DeleteRequest)
	deleteNameField := env.svc.Descriptors.DeleteRequest.Fields().ByName("name")
	deleteReq.Set(deleteNameField, protoreflect.ValueOfString("kindclusters/cluster-a"))

	deleteResp := dynamicpb.NewMessage(env.svc.Descriptors.Resource)
	if err := env.conn.Invoke(ctx, "/fleetshift.v1.KindClusterService/DeleteKindCluster", deleteReq, deleteResp); err != nil {
		t.Fatalf("DeleteKindCluster: %v", err)
	}

	stateField := env.svc.Descriptors.Resource.Fields().ByName("state")
	if deleteResp.Get(stateField).Int() != 3 { // STATE_DELETING
		t.Errorf("deleted state = %d, want 3 (STATE_DELETING)", deleteResp.Get(stateField).Int())
	}
}

func TestDynamic_GetNotFound(t *testing.T) {
	env := setup(t)
	ctx := context.Background()

	getReq := dynamicpb.NewMessage(env.svc.Descriptors.GetRequest)
	nameField := env.svc.Descriptors.GetRequest.Fields().ByName("name")
	getReq.Set(nameField, protoreflect.ValueOfString("kindclusters/nonexistent"))

	resp := dynamicpb.NewMessage(env.svc.Descriptors.Resource)
	err := env.conn.Invoke(ctx, "/fleetshift.v1.KindClusterService/GetKindCluster", getReq, resp)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}
	if st.Code() != codes.NotFound {
		t.Errorf("code = %v, want NotFound", st.Code())
	}
}

// TestDynamic_ServiceDescriptors verifies the descriptor builder produces
// correctly-shaped message and service descriptors.
func TestDynamic_ServiceDescriptors(t *testing.T) {
	validator, _ := protovalidate.New()
	svc, err := managedresource.Build(clusterConfig(t), managedresource.Deps{
		Validator: validator,
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	// Verify service name
	if svc.Desc.ServiceName != "fleetshift.v1.KindClusterService" {
		t.Errorf("service name = %q, want %q", svc.Desc.ServiceName, "fleetshift.v1.KindClusterService")
	}

	// Verify method count
	if len(svc.Desc.Methods) != 4 {
		t.Fatalf("method count = %d, want 4", len(svc.Desc.Methods))
	}

	// Verify method names
	methods := make(map[string]bool)
	for _, m := range svc.Desc.Methods {
		methods[m.MethodName] = true
	}
	for _, want := range []string{"CreateKindCluster", "GetKindCluster", "ListKindclusters", "DeleteKindCluster"} {
		if !methods[want] {
			t.Errorf("missing method %q", want)
		}
	}

	// Verify resource message has spec field pointing to KindClusterSpec
	specField := svc.Descriptors.Resource.Fields().ByName("spec")
	if specField == nil {
		t.Fatal("resource message missing spec field")
	}
	if specField.Message().FullName() != "addons.kind.v1.KindClusterSpec" {
		t.Errorf("spec message = %q, want addons.kind.v1.KindClusterSpec", specField.Message().FullName())
	}

	// Verify we can create a dynamic message from the resource descriptor
	msg := dynamicpb.NewMessage(svc.Descriptors.Resource)
	nameField := svc.Descriptors.Resource.Fields().ByName("name")
	msg.Set(nameField, protoreflect.ValueOfString("kindclusters/test"))

	b, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal dynamic resource: %v", err)
	}
	if len(b) == 0 {
		t.Error("marshaled bytes are empty")
	}
}

func TestDynamic_SpecDescriptorIdentity(t *testing.T) {
	cfg := clusterConfig(t)

	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("protovalidate.New: %v", err)
	}

	svc, err := managedresource.Build(cfg, managedresource.Deps{Validator: validator})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	// The spec field in the resource message should resolve to the same
	// descriptor as svc.Descriptors.Spec (which carries buf.validate annotations).
	specFieldDesc := svc.Descriptors.Resource.Fields().ByName("spec").Message()
	if specFieldDesc.FullName() != svc.Descriptors.Spec.FullName() {
		t.Fatalf("spec field descriptor = %q, want %q", specFieldDesc.FullName(), svc.Descriptors.Spec.FullName())
	}

	// Build a request, marshal/unmarshal (simulating wire transit), then
	// extract the spec and validate directly — no JSON roundtrip.
	createReq := dynamicpb.NewMessage(svc.Descriptors.CreateRequest)
	idField := svc.Descriptors.CreateRequest.Fields().ByNumber(1)
	createReq.Set(idField, protoreflect.ValueOfString("test-cluster"))

	resource := dynamicpb.NewMessage(svc.Descriptors.Resource)
	specMsg := dynamicpb.NewMessage(svc.Descriptors.Spec)
	specMsg.Set(svc.Descriptors.Spec.Fields().ByName("name"), protoreflect.ValueOfString("test-cluster"))

	specField := svc.Descriptors.Resource.Fields().ByName("spec")
	resource.Set(specField, protoreflect.ValueOfMessage(specMsg))

	resourceField := svc.Descriptors.CreateRequest.Fields().ByNumber(2)
	createReq.Set(resourceField, protoreflect.ValueOfMessage(resource))

	// Simulate wire transit.
	wire, err := proto.Marshal(createReq)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	incoming := dynamicpb.NewMessage(svc.Descriptors.CreateRequest)
	if err := proto.Unmarshal(wire, incoming); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Extract spec from the unmarshaled request.
	incomingResource := incoming.Get(resourceField).Message()
	incomingSpec := incomingResource.Get(specField).Message()

	// Verify the descriptor is the original (annotation-carrying) one.
	if incomingSpec.Descriptor().FullName() != svc.Descriptors.Spec.FullName() {
		t.Fatalf("unmarshaled spec descriptor = %q, want %q",
			incomingSpec.Descriptor().FullName(), svc.Descriptors.Spec.FullName())
	}

	// Direct validation should work without JSON roundtrip.
	if err := validator.Validate(incomingSpec.Interface()); err != nil {
		t.Fatalf("direct validation failed: %v", err)
	}

	// Also verify that invalid spec is caught directly.
	invalidReq := dynamicpb.NewMessage(svc.Descriptors.CreateRequest)
	invalidReq.Set(idField, protoreflect.ValueOfString("bad-cluster"))
	invalidResource := dynamicpb.NewMessage(svc.Descriptors.Resource)
	emptySpec := dynamicpb.NewMessage(svc.Descriptors.Spec)
	// name is required by buf.validate — leave it empty
	invalidResource.Set(specField, protoreflect.ValueOfMessage(emptySpec))
	invalidReq.Set(resourceField, protoreflect.ValueOfMessage(invalidResource))

	wire2, _ := proto.Marshal(invalidReq)
	incoming2 := dynamicpb.NewMessage(svc.Descriptors.CreateRequest)
	_ = proto.Unmarshal(wire2, incoming2)

	incomingResource2 := incoming2.Get(resourceField).Message()
	incomingSpec2 := incomingResource2.Get(specField).Message()

	err = validator.Validate(incomingSpec2.Interface())
	if err == nil {
		t.Fatal("expected validation error for empty spec, got nil")
	}
	t.Logf("validation error (expected): %v", err)
}
