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
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/testutil"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
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
	desc, err := dynamicapi.CompileInline(
		context.Background(),
		schema.ProtoFiles,
		entryFile,
		protoreflect.FullName(schema.SpecMessage),
	)
	if err != nil {
		t.Fatalf("CompileInline: %v", err)
	}

	return &managedresource.ResourceTypeConfig{
		CollectionConfig: dynamicapi.CollectionConfig{
			Version:      schema.Version,
			CollectionID: schema.CollectionID,
			Singular:     schema.Singular,
			Plural:       schema.Plural,
		},
		ResourceType:   kindaddon.ClusterResourceType,
		APIServiceName: schema.APIServiceName,
		ProtoPackage:   schema.ProtoPackage,
		SpecMessage:    schema.SpecMessage,
		SpecDescriptor: desc.Message,
	}
}

type testEnv struct {
	conn  *grpc.ClientConn
	svc   *managedresource.RegisteredService
	store domain.Store
}

func setup(t *testing.T) *testEnv {
	return setupWithDelivery(t, nil)
}

type blockingRemoveDynamicDelivery struct {
	inner    *sqlite.RecordingDeliveryService
	reporter domain.DeliveryReporter
	started  chan struct{}
	release  chan struct{}
}

func newBlockingRemoveDynamicDelivery(store domain.Store) *blockingRemoveDynamicDelivery {
	return &blockingRemoveDynamicDelivery{
		inner: &sqlite.RecordingDeliveryService{
			Store: store,
			Now:   func() time.Time { return time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC) },
		},
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (d *blockingRemoveDynamicDelivery) Deliver(
	ctx context.Context,
	target domain.TargetInfo,
	deliveryID domain.DeliveryID,
	manifests []domain.Manifest,
	auth domain.DeliveryAuth,
	att *domain.Attestation,
	generation domain.Generation,
) error {
	if err := d.inner.Deliver(ctx, target, deliveryID, manifests, auth, att, generation); err != nil {
		return err
	}
	if d.reporter != nil {
		go func() {
			_ = d.reporter.ReportResult(context.Background(), deliveryID, generation, domain.DeliveryResult{State: domain.DeliveryStateDelivered})
		}()
	}
	return nil
}

func (d *blockingRemoveDynamicDelivery) Remove(
	ctx context.Context,
	target domain.TargetInfo,
	deliveryID domain.DeliveryID,
	manifests []domain.Manifest,
	auth domain.DeliveryAuth,
	att *domain.Attestation,
	generation domain.Generation,
) error {
	if err := d.inner.Remove(ctx, target, deliveryID, manifests, auth, att, generation); err != nil {
		return err
	}
	select {
	case <-d.started:
	default:
		close(d.started)
	}
	select {
	case <-d.release:
		if d.reporter != nil {
			go func() {
				_ = d.reporter.ReportResult(context.Background(), deliveryID, generation, domain.DeliveryResult{State: domain.DeliveryStateDelivered})
			}()
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func setupWithDelivery(
	t *testing.T,
	buildDelivery func(store domain.Store, reporter domain.DeliveryReporter) domain.DeliveryAgent,
) *testEnv {
	t.Helper()

	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	reg := &memworkflow.Registry{}
	reporter := application.NewDeliveryReportService(store, reg)
	recordingAgent := domain.DeliveryAgent(&sqlite.RecordingDeliveryService{
		Store:    store,
		Reporter: reporter,
		Now:      func() time.Time { return time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC) },
	})
	if buildDelivery != nil {
		recordingAgent = buildDelivery(store, reporter)
	}
	router := delivery.NewRoutingDeliveryService()
	router.Register(clusterTargetType, recordingAgent)

	orchSpec := domain.NewOrchestrationWorkflowSpec(
		store, router, domain.StrategyFactory{Store: store}, reg,
		domain.WithAckRetryInterval(5*time.Second),
	)
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

	resumeMRSpec := &domain.ResumeManagedResourceWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
	}
	resumeMRWf, err := reg.RegisterResumeManagedResource(resumeMRSpec)
	if err != nil {
		t.Fatalf("RegisterResumeManagedResource: %v", err)
	}

	managedResourceSvc := &application.ManagedResourceService{
		Store:    store,
		CreateWF: createMRWf,
		DeleteWF: deleteMRWf,
		ResumeWF: resumeMRWf,
	}

	targetSvc := &application.TargetService{Store: store}
	if err := targetSvc.Register(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID: "kind-local", Type: clusterTargetType, Name: "Kind Cluster Addon",
		AcceptedManifestTypes: []domain.ManifestType{kindaddon.ClusterManifestType},
	})); err != nil {
		t.Fatalf("register target: %v", err)
	}

	typeSvc := application.NewManagedResourceTypeService(store)
	if _, err := typeSvc.Create(context.Background(), application.CreateTypeInput{
		ResourceType:   kindaddon.ClusterResourceType,
		Relation:       domain.NewRegisteredSelfTarget("kind-local", kindaddon.ClusterManifestType),
		Signature:      domain.Signature{},
		APIServiceName: "kind.fleetshift.io",
		APIVersion:     "v1",
		CollectionID:   "clusters",
	}); err != nil {
		t.Fatalf("register cluster type: %v", err)
	}

	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("protovalidate.New: %v", err)
	}

	// --- Build and register dynamic service ---

	lis := bufconn.Listen(1 << 20)
	// Interceptor injects a test auth context so Resume (and future
	// auth-requiring RPCs) has a valid caller.
	testAuthInterceptor := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if application.AuthFromContext(ctx) == nil {
			ctx = application.ContextWithAuth(ctx, &application.AuthorizationContext{
				Subject: &domain.SubjectClaims{
					FederatedIdentity: domain.FederatedIdentity{Subject: "test-user", Issuer: "https://test-issuer.example.com"},
				},
				Token: "test-token",
			})
		}
		return handler(ctx, req)
	}
	srv := grpc.NewServer(grpc.UnaryInterceptor(testAuthInterceptor))

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

	return &testEnv{conn: conn, svc: svc, store: store}
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
	err := env.conn.Invoke(ctx, "/kind.fleetshift.v1.ClusterService/CreateCluster", createReq, createResp)
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	// Verify the response.
	nameField := env.svc.Descriptors.Resource.Fields().ByName("name")
	gotName := createResp.Get(nameField).String()
	if gotName != "clusters/dev-cluster" {
		t.Errorf("name = %q, want %q", gotName, "clusters/dev-cluster")
	}

	uidField := env.svc.Descriptors.Resource.Fields().ByName("uid")
	if createResp.Get(uidField).String() == "" {
		t.Error("uid is empty, want non-empty UUID")
	}

	stateField := env.svc.Descriptors.Resource.Fields().ByName("state")
	if int32(createResp.Get(stateField).Enum()) != 1 { // CREATING
		t.Errorf("state = %d, want 1 (CREATING)", createResp.Get(stateField).Enum())
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

	// --- GetCluster ---

	getReq := dynamicpb.NewMessage(env.svc.Descriptors.GetRequest)
	getNameField := env.svc.Descriptors.GetRequest.Fields().ByName("name")
	getReq.Set(getNameField, protoreflect.ValueOfString("clusters/dev-cluster"))

	getResp := dynamicpb.NewMessage(env.svc.Descriptors.Resource)
	err = env.conn.Invoke(ctx, "/kind.fleetshift.v1.ClusterService/GetCluster", getReq, getResp)
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}

	if getResp.Get(nameField).String() != "clusters/dev-cluster" {
		t.Errorf("get name = %q, want %q", getResp.Get(nameField).String(), "clusters/dev-cluster")
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
	err := env.conn.Invoke(ctx, "/kind.fleetshift.v1.ClusterService/CreateCluster", createReq, resp)
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
	if err := env.conn.Invoke(ctx, "/kind.fleetshift.v1.ClusterService/CreateCluster", createReq, createResp); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	// List
	listReq := dynamicpb.NewMessage(env.svc.Descriptors.ListRequest)
	listResp := dynamicpb.NewMessage(env.svc.Descriptors.ListResponse)
	if err := env.conn.Invoke(ctx, "/kind.fleetshift.v1.ClusterService/ListClusters", listReq, listResp); err != nil {
		t.Fatalf("ListClusters: %v", err)
	}

	resourcesField := env.svc.Descriptors.ListResponse.Fields().ByNumber(1)
	list := listResp.Get(resourcesField).List()
	if list.Len() != 1 {
		t.Fatalf("list count = %d, want 1", list.Len())
	}

	// Delete
	deleteReq := dynamicpb.NewMessage(env.svc.Descriptors.DeleteRequest)
	deleteNameField := env.svc.Descriptors.DeleteRequest.Fields().ByName("name")
	deleteReq.Set(deleteNameField, protoreflect.ValueOfString("clusters/cluster-a"))

	deleteResp := dynamicpb.NewMessage(env.svc.Descriptors.Resource)
	if err := env.conn.Invoke(ctx, "/kind.fleetshift.v1.ClusterService/DeleteCluster", deleteReq, deleteResp); err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}

	stateField := env.svc.Descriptors.Resource.Fields().ByName("state")
	if int32(deleteResp.Get(stateField).Enum()) != 3 { // DELETING
		t.Errorf("deleted state = %d, want 3 (DELETING)", deleteResp.Get(stateField).Enum())
	}
}

func TestDynamic_DeleteKeepsResourceVisibleDuringCleanup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testutil.ServiceTimeout)
	defer cancel()

	var blocker *blockingRemoveDynamicDelivery
	env := setupWithDelivery(t, func(store domain.Store, reporter domain.DeliveryReporter) domain.DeliveryAgent {
		blocker = newBlockingRemoveDynamicDelivery(store)
		blocker.reporter = reporter
		return blocker
	})

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
	if err := env.conn.Invoke(ctx, "/kind.fleetshift.v1.ClusterService/CreateCluster", createReq, createResp); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	awaitDynamicState(t, ctx, env, "cluster-a", 2)

	deleteReq := dynamicpb.NewMessage(env.svc.Descriptors.DeleteRequest)
	deleteNameField := env.svc.Descriptors.DeleteRequest.Fields().ByName("name")
	deleteReq.Set(deleteNameField, protoreflect.ValueOfString("clusters/cluster-a"))

	deleteResp := dynamicpb.NewMessage(env.svc.Descriptors.Resource)
	if err := env.conn.Invoke(ctx, "/kind.fleetshift.v1.ClusterService/DeleteCluster", deleteReq, deleteResp); err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}

	stateField := env.svc.Descriptors.Resource.Fields().ByName("state")
	if int32(deleteResp.Get(stateField).Enum()) != 3 { // DELETING
		t.Fatalf("deleted state = %d, want 3 (DELETING)", deleteResp.Get(stateField).Enum())
	}

	select {
	case <-blocker.started:
	case <-ctx.Done():
		t.Fatal("timed out waiting for remove to start")
	}

	getReq := dynamicpb.NewMessage(env.svc.Descriptors.GetRequest)
	getNameField := env.svc.Descriptors.GetRequest.Fields().ByName("name")
	getReq.Set(getNameField, protoreflect.ValueOfString("clusters/cluster-a"))

	getResp := dynamicpb.NewMessage(env.svc.Descriptors.Resource)
	if err := env.conn.Invoke(ctx, "/kind.fleetshift.v1.ClusterService/GetCluster", getReq, getResp); err != nil {
		t.Fatalf("GetCluster during delete: %v", err)
	}
	if int32(getResp.Get(stateField).Enum()) != 3 { // DELETING
		t.Fatalf("get state during delete = %d, want 3 (DELETING)", getResp.Get(stateField).Enum())
	}

	listReq := dynamicpb.NewMessage(env.svc.Descriptors.ListRequest)
	listResp := dynamicpb.NewMessage(env.svc.Descriptors.ListResponse)
	if err := env.conn.Invoke(ctx, "/kind.fleetshift.v1.ClusterService/ListClusters", listReq, listResp); err != nil {
		t.Fatalf("ListClusters during delete: %v", err)
	}

	resourcesField := env.svc.Descriptors.ListResponse.Fields().ByNumber(1)
	list := listResp.Get(resourcesField).List()
	if list.Len() != 1 {
		t.Fatalf("list count during delete = %d, want 1", list.Len())
	}
	if int32(list.Get(0).Message().Get(stateField).Enum()) != 3 { // DELETING
		t.Fatalf("list state during delete = %d, want 3 (DELETING)", list.Get(0).Message().Get(stateField).Enum())
	}

	close(blocker.release)
}

func awaitDynamicState(t *testing.T, ctx context.Context, env *testEnv, id string, want protoreflect.EnumNumber) {
	t.Helper()

	getNameField := env.svc.Descriptors.GetRequest.Fields().ByName("name")
	stateField := env.svc.Descriptors.Resource.Fields().ByName("state")
	for {
		getReq := dynamicpb.NewMessage(env.svc.Descriptors.GetRequest)
		getReq.Set(getNameField, protoreflect.ValueOfString("clusters/"+id))

		getResp := dynamicpb.NewMessage(env.svc.Descriptors.Resource)
		if err := env.conn.Invoke(ctx, "/kind.fleetshift.v1.ClusterService/GetCluster", getReq, getResp); err == nil {
			if getResp.Get(stateField).Enum() == want {
				return
			}
		}

		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for clusters/%s to reach state %d", id, want)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestDynamic_GetNotFound(t *testing.T) {
	env := setup(t)
	ctx := context.Background()

	getReq := dynamicpb.NewMessage(env.svc.Descriptors.GetRequest)
	nameField := env.svc.Descriptors.GetRequest.Fields().ByName("name")
	getReq.Set(nameField, protoreflect.ValueOfString("clusters/nonexistent"))

	resp := dynamicpb.NewMessage(env.svc.Descriptors.Resource)
	err := env.conn.Invoke(ctx, "/kind.fleetshift.v1.ClusterService/GetCluster", getReq, resp)
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
	if svc.Desc.ServiceName != "kind.fleetshift.v1.ClusterService" {
		t.Errorf("service name = %q, want %q", svc.Desc.ServiceName, "kind.fleetshift.v1.ClusterService")
	}

	// Verify method count
	if len(svc.Desc.Methods) != 5 {
		t.Fatalf("method count = %d, want 5", len(svc.Desc.Methods))
	}

	// Verify method names
	methods := make(map[string]bool)
	for _, m := range svc.Desc.Methods {
		methods[m.MethodName] = true
	}
	for _, want := range []string{"CreateCluster", "GetCluster", "ListClusters", "DeleteCluster", "ResumeCluster"} {
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
	msg.Set(nameField, protoreflect.ValueOfString("clusters/test"))

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

func TestDynamic_ProvenanceOnResponse(t *testing.T) {
	env := setup(t)
	ctx := context.Background()

	// Create a resource.
	createReq := dynamicpb.NewMessage(env.svc.Descriptors.CreateRequest)
	idField := env.svc.Descriptors.CreateRequest.Fields().ByNumber(1)
	createReq.Set(idField, protoreflect.ValueOfString("prov-cluster"))

	resource := dynamicpb.NewMessage(env.svc.Descriptors.Resource)
	specField := env.svc.Descriptors.Resource.Fields().ByName("spec")
	spec := dynamicpb.NewMessage(env.svc.Descriptors.Spec)
	spec.Set(env.svc.Descriptors.Spec.Fields().ByName("name"), protoreflect.ValueOfString("prov-cluster"))
	resource.Set(specField, protoreflect.ValueOfMessage(spec))
	createReq.Set(env.svc.Descriptors.CreateRequest.Fields().ByNumber(2), protoreflect.ValueOfMessage(resource))

	createResp := dynamicpb.NewMessage(env.svc.Descriptors.Resource)
	if err := env.conn.Invoke(ctx, "/kind.fleetshift.v1.ClusterService/CreateCluster", createReq, createResp); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	// Seed provenance on the fulfillment.
	tx, err := env.store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	mr, err := tx.ManagedResources().GetInstance(ctx, kindaddon.ClusterResourceType, "clusters/prov-cluster")
	if err != nil {
		t.Fatalf("get managed resource: %v", err)
	}
	f, err := tx.Fulfillments().Get(ctx, mr.FulfillmentID())
	if err != nil {
		t.Fatalf("get fulfillment: %v", err)
	}
	snap := f.Snapshot()
	snap.Provenance = &domain.Provenance{
		Sig: domain.Signature{
			Signer:         domain.FederatedIdentity{Subject: "user-1", Issuer: "https://issuer.example.com"},
			ContentHash:    []byte("hash-bytes"),
			SignatureBytes: []byte("sig-bytes"),
		},
		ValidUntil:         time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
		ExpectedGeneration: 1,
	}
	f = domain.FulfillmentFromSnapshot(snap)
	if err := tx.Fulfillments().Update(ctx, f); err != nil {
		t.Fatalf("update fulfillment: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Get the resource and verify provenance is populated.
	getReq := dynamicpb.NewMessage(env.svc.Descriptors.GetRequest)
	getReq.Set(env.svc.Descriptors.GetRequest.Fields().ByName("name"),
		protoreflect.ValueOfString("clusters/prov-cluster"))

	getResp := dynamicpb.NewMessage(env.svc.Descriptors.Resource)
	if err := env.conn.Invoke(ctx, "/kind.fleetshift.v1.ClusterService/GetCluster", getReq, getResp); err != nil {
		t.Fatalf("GetCluster: %v", err)
	}

	provField := env.svc.Descriptors.Resource.Fields().ByName("provenance")
	if !getResp.Has(provField) {
		t.Fatal("provenance field not set on response")
	}

	provMsg := getResp.Get(provField).Message()
	provJSON, _ := protojson.Marshal(provMsg.Interface())
	t.Logf("provenance JSON: %s", provJSON)

	// Verify signature.signer.subject
	sigField := provMsg.Descriptor().Fields().ByName("signature")
	if !provMsg.Has(sigField) {
		t.Fatal("provenance.signature not set")
	}
	sigMsg := provMsg.Get(sigField).Message()
	signerField := sigMsg.Descriptor().Fields().ByName("signer")
	signerMsg := sigMsg.Get(signerField).Message()
	subjectField := signerMsg.Descriptor().Fields().ByName("subject")
	if got := signerMsg.Get(subjectField).String(); got != "user-1" {
		t.Errorf("signer.subject = %q, want %q", got, "user-1")
	}

	// Verify expected_generation
	genField := provMsg.Descriptor().Fields().ByName("expected_generation")
	if got := provMsg.Get(genField).Int(); got != 1 {
		t.Errorf("expected_generation = %d, want 1", got)
	}
}

func TestDynamic_ResumeRPC(t *testing.T) {
	env := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create a resource.
	createReq := dynamicpb.NewMessage(env.svc.Descriptors.CreateRequest)
	idField := env.svc.Descriptors.CreateRequest.Fields().ByNumber(1)
	createReq.Set(idField, protoreflect.ValueOfString("resume-cluster"))
	resource := dynamicpb.NewMessage(env.svc.Descriptors.Resource)
	specField := env.svc.Descriptors.Resource.Fields().ByName("spec")
	spec := dynamicpb.NewMessage(env.svc.Descriptors.Spec)
	spec.Set(env.svc.Descriptors.Spec.Fields().ByName("name"), protoreflect.ValueOfString("resume-cluster"))
	resource.Set(specField, protoreflect.ValueOfMessage(spec))
	createReq.Set(env.svc.Descriptors.CreateRequest.Fields().ByNumber(2), protoreflect.ValueOfMessage(resource))

	createResp := dynamicpb.NewMessage(env.svc.Descriptors.Resource)
	if err := env.conn.Invoke(ctx, "/kind.fleetshift.v1.ClusterService/CreateCluster", createReq, createResp); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	// Wait for orchestration to finish before manipulating state directly.
	awaitDynamicState(t, ctx, env, "resume-cluster", 2)

	// Pause the fulfillment (simulate auth failure).
	tx, err := env.store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	mr, err := tx.ManagedResources().GetInstance(ctx, kindaddon.ClusterResourceType, "clusters/resume-cluster")
	if err != nil {
		t.Fatalf("get managed resource: %v", err)
	}
	f, err := tx.Fulfillments().Get(ctx, mr.FulfillmentID())
	if err != nil {
		t.Fatalf("get fulfillment: %v", err)
	}
	f.ApplyReconciliationResult(domain.ReconciliationResult{
		FulfillmentID:   f.ID(),
		PauseReason:     "delivery auth failed",
		ResolvedTargets: f.ResolvedTargets(),
		Auth:            f.Auth(),
	})
	if err := tx.Fulfillments().Update(ctx, f); err != nil {
		t.Fatalf("update fulfillment: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Call Resume RPC (server interceptor provides auth context).
	resumeReq := dynamicpb.NewMessage(env.svc.Descriptors.ResumeRequest)
	nameField := env.svc.Descriptors.ResumeRequest.Fields().ByName("name")
	resumeReq.Set(nameField, protoreflect.ValueOfString("clusters/resume-cluster"))

	resumeResp := dynamicpb.NewMessage(env.svc.Descriptors.Resource)
	err = env.conn.Invoke(ctx, "/kind.fleetshift.v1.ClusterService/ResumeCluster", resumeReq, resumeResp)
	if err != nil {
		t.Fatalf("ResumeCluster: %v", err)
	}

	// Verify the response contains the resource name (valid response).
	respNameField := env.svc.Descriptors.Resource.Fields().ByName("name")
	gotName := resumeResp.Get(respNameField).String()
	if gotName != "clusters/resume-cluster" {
		t.Errorf("name = %q, want %q", gotName, "clusters/resume-cluster")
	}

	// The intent_version should be populated.
	versionField := env.svc.Descriptors.Resource.Fields().ByName("intent_version")
	if resumeResp.Get(versionField).Int() < 1 {
		t.Error("intent_version < 1 in resume response")
	}
}

func TestDynamic_ResumeRPC_NotPaused(t *testing.T) {
	env := setup(t)
	ctx := context.Background()

	// Create a resource (starts in CREATING state).
	createReq := dynamicpb.NewMessage(env.svc.Descriptors.CreateRequest)
	idField := env.svc.Descriptors.CreateRequest.Fields().ByNumber(1)
	createReq.Set(idField, protoreflect.ValueOfString("not-paused-cluster"))
	resource := dynamicpb.NewMessage(env.svc.Descriptors.Resource)
	specField := env.svc.Descriptors.Resource.Fields().ByName("spec")
	spec := dynamicpb.NewMessage(env.svc.Descriptors.Spec)
	spec.Set(env.svc.Descriptors.Spec.Fields().ByName("name"), protoreflect.ValueOfString("not-paused"))
	resource.Set(specField, protoreflect.ValueOfMessage(spec))
	createReq.Set(env.svc.Descriptors.CreateRequest.Fields().ByNumber(2), protoreflect.ValueOfMessage(resource))

	createResp := dynamicpb.NewMessage(env.svc.Descriptors.Resource)
	if err := env.conn.Invoke(ctx, "/kind.fleetshift.v1.ClusterService/CreateCluster", createReq, createResp); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	// Try to resume a resource that is not paused (server interceptor
	// provides auth context).
	resumeReq := dynamicpb.NewMessage(env.svc.Descriptors.ResumeRequest)
	nameField := env.svc.Descriptors.ResumeRequest.Fields().ByName("name")
	resumeReq.Set(nameField, protoreflect.ValueOfString("clusters/not-paused-cluster"))

	resumeResp := dynamicpb.NewMessage(env.svc.Descriptors.Resource)
	err := env.conn.Invoke(ctx, "/kind.fleetshift.v1.ClusterService/ResumeCluster", resumeReq, resumeResp)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", st.Code())
	}
}
