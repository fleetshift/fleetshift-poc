package grpc_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/extensionresource"
	transportgrpc "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/grpc"
)

// seedManagedCluster describes one managed extension resource to seed
// for QueryResources round-trip tests.
type seedManagedCluster struct {
	ID          string
	State       domain.FulfillmentState
	Spec        string // JSON object; empty uses {"name":"<id>"}
	PauseReason string
	Generation  domain.Generation
	Provenance  *domain.Provenance
}

type resourceQueryHarness struct {
	client   pb.ResourceQueryServiceClient
	registry *extensionresource.ActiveResourceRegistry
	cfg      *extensionresource.ResourceTypeConfig
}

// setupResourceQueryHarness activates the kind Cluster type and seeds
// the given managed clusters (one fulfillment + extension resource each).
func setupResourceQueryHarness(t *testing.T, seeds []seedManagedCluster) *resourceQueryHarness {
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

	for _, seed := range seeds {
		fID := domain.FulfillmentID("ful-" + seed.ID)
		gen := seed.Generation
		if gen == 0 {
			gen = 1
		}
		if err := tx.Fulfillments().Create(ctx, domain.FulfillmentFromSnapshot(domain.FulfillmentSnapshot{
			ID:          fID,
			State:       seed.State,
			PauseReason: seed.PauseReason,
			Generation:  gen,
			Provenance:  seed.Provenance,
			CreatedAt:   now,
			UpdatedAt:   now,
		})); err != nil {
			tx.Rollback()
			t.Fatalf("Create fulfillment %s: %v", seed.ID, err)
		}
		spec := seed.Spec
		if spec == "" {
			spec = fmt.Sprintf(`{"name":%q}`, seed.ID)
		}
		er := domain.NewExtensionResource(domain.NewExtensionResourceUID(), cfg.ResourceType,
			domain.ResourceName("clusters/"+seed.ID), now, domain.WithManagedState(fID))
		if _, err := er.RecordIntent(json.RawMessage(spec), now); err != nil {
			tx.Rollback()
			t.Fatalf("RecordIntent %s: %v", seed.ID, err)
		}
		if err := tx.ExtensionResources().Create(ctx, er); err != nil {
			tx.Rollback()
			t.Fatalf("Create resource %s: %v", seed.ID, err)
		}
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

	return &resourceQueryHarness{
		client:   pb.NewResourceQueryServiceClient(conn),
		registry: registry,
		cfg:      cfg,
	}
}

func structField(s *structpb.Struct, key string) *structpb.Value {
	if s == nil || s.Fields == nil {
		return nil
	}
	return s.Fields[key]
}

func structFieldString(s *structpb.Struct, key string) string {
	v := structField(s, key)
	if v == nil {
		return ""
	}
	return v.GetStringValue()
}

// celEq builds `field == <literal>` using the Struct value's wire kind
// so round-trips stay faithful to what the API returned (string vs
// number vs bool), including protojson's string-encoded int64s.
func celEq(field string, v *structpb.Value) (string, error) {
	if v == nil {
		return "", fmt.Errorf("nil value for %s", field)
	}
	switch k := v.Kind.(type) {
	case *structpb.Value_StringValue:
		// ProtoJSON encodes int64 as a decimal string; keep that
		// spelling in the filter so round-trips match CEL over the
		// documented response (resource.intentVersion == "1").
		return fmt.Sprintf(`%s == %q`, field, k.StringValue), nil
	case *structpb.Value_NumberValue:
		// Emit an integer literal when the value is integral so CEL
		// compares against int columns without float noise.
		n := k.NumberValue
		if n == float64(int64(n)) {
			return fmt.Sprintf(`%s == %d`, field, int64(n)), nil
		}
		return fmt.Sprintf(`%s == %v`, field, n), nil
	case *structpb.Value_BoolValue:
		return fmt.Sprintf(`%s == %t`, field, k.BoolValue), nil
	default:
		return "", fmt.Errorf("unsupported struct value kind for %s: %T", field, v.Kind)
	}
}

func mustCelEq(t *testing.T, field string, v *structpb.Value) string {
	t.Helper()
	expr, err := celEq(field, v)
	if err != nil {
		t.Fatal(err)
	}
	return expr
}

func queryOne(t *testing.T, h *resourceQueryHarness, filter string) *pb.ResourceResult {
	t.Helper()
	page, err := h.client.QueryResources(context.Background(), &pb.QueryResourcesRequest{
		Scope: "-", Filter: filter, PageSize: 50,
	})
	if err != nil {
		t.Fatalf("QueryResources(%s): %v", filter, err)
	}
	if len(page.Resources) != 1 {
		t.Fatalf("QueryResources(%s): got %d results, want 1", filter, len(page.Resources))
	}
	return page.Resources[0]
}

func queryCount(t *testing.T, h *resourceQueryHarness, filter string) int {
	t.Helper()
	page, err := h.client.QueryResources(context.Background(), &pb.QueryResourcesRequest{
		Scope: "-", Filter: filter, PageSize: 50,
	})
	if err != nil {
		t.Fatalf("QueryResources(%s): %v", filter, err)
	}
	return len(page.Resources)
}

// responseEnvelopeFields are the top-level ResourceResult fields
// returned by QueryResources today (proto ResourceResult).
var responseEnvelopeFields = []string{"name", "resourceType", "resource"}

// responseBodyKeys are every field the managed-resource Get/List
// message (and thus ResourceResult.resource Struct bodies) may carry
// today. Round-trip tests assert each is either filterable from its
// verbatim API value or explicitly non-filterable with a
// present/absent check.
var responseBodyKeys = []string{
	"name", "uid", "spec", "intentVersion", "state", "reconciling",
	"createTime", "updateTime", "deleteTime", "etag", "provenance",
	"generation", "pauseReason",
}

// TestResourceQuery_RoundTripAllResponseFields seeds a richly populated
// resource (plus siblings in other states), reads each QueryResources
// hit, and for every queryable field — envelope and Struct body —
// re-filters using the exact value from that response. Fields present
// on the response but not on the CEL surface are asserted for shape
// only so gaps stay intentional and visible.
func TestResourceQuery_RoundTripAllResponseFields(t *testing.T) {
	prov := &domain.Provenance{
		Sig: domain.Signature{
			Signer:         domain.FederatedIdentity{Subject: "user@example.com", Issuer: "https://issuer.test"},
			ContentHash:    []byte("content-hash"),
			SignatureBytes: []byte("sig-bytes"),
		},
		ExpectedGeneration: 7,
	}
	seeds := []seedManagedCluster{
		{
			ID:          "rich",
			State:       domain.FulfillmentStateActive,
			PauseReason: "waiting-for-auth",
			Generation:  7,
			Provenance:  prov,
			Spec:        `{"name":"rich","nodes":[{"role":"control-plane"}]}`,
		},
		{ID: "creating-1", State: domain.FulfillmentStateCreating},
		{ID: "deleting-1", State: domain.FulfillmentStateDeleting},
		{ID: "failed-1", State: domain.FulfillmentStateFailed},
	}
	h := setupResourceQueryHarness(t, seeds)
	ctx := context.Background()

	all, err := h.client.QueryResources(ctx, &pb.QueryResourcesRequest{
		Scope:    "-",
		Filter:   `resourceType == "kind.fleetshift.io/Cluster"`,
		PageSize: 50,
		OrderBy:  "resource_type,name",
	})
	if err != nil {
		t.Fatalf("initial QueryResources: %v", err)
	}
	if len(all.Resources) != len(seeds) {
		t.Fatalf("len(resources) = %d, want %d", len(all.Resources), len(seeds))
	}

	var rich *pb.ResourceResult
	for _, r := range all.Resources {
		if structFieldString(r.Resource, "name") == "clusters/rich" {
			rich = r
			break
		}
	}
	if rich == nil || rich.Resource == nil {
		t.Fatal("rich resource missing from initial page")
	}
	body := rich.Resource

	// --- inventory of envelope + body keys ---
	_ = responseEnvelopeFields // documented; exercised in envelope_* subtests
	for key := range body.Fields {
		known := false
		for _, want := range responseBodyKeys {
			if key == want {
				known = true
				break
			}
		}
		if !known {
			t.Errorf("unexpected Struct field %q (update responseBodyKeys / round-trip coverage)", key)
		}
	}

	// --- envelope fields (ResourceResult.name / resourceType) ---
	// Round-trip every hit so casing/format mismatches cannot hide on
	// a single fixture row.
	t.Run("envelope_name", func(t *testing.T) {
		for _, r := range all.Resources {
			if r.Name == "" {
				t.Fatalf("empty envelope name for body name %q", structFieldString(r.Resource, "name"))
			}
			if !strings.HasPrefix(r.Name, "//") {
				t.Errorf("envelope name %q: want canonical //service/collection/id form", r.Name)
			}
			filter := mustCelEq(t, "name", structpb.NewStringValue(r.Name))
			got := queryOne(t, h, filter)
			if got.Name != r.Name {
				t.Errorf("name round-trip: filter %s → %q, want %q", filter, got.Name, r.Name)
			}
		}
	})
	t.Run("envelope_resourceType", func(t *testing.T) {
		for _, r := range all.Resources {
			if r.ResourceType == "" {
				t.Fatal("empty resourceType")
			}
			// Disambiguate among same-type siblings with the envelope name.
			filter := mustCelEq(t, "resourceType", structpb.NewStringValue(r.ResourceType)) +
				" && " + mustCelEq(t, "name", structpb.NewStringValue(r.Name))
			got := queryOne(t, h, filter)
			if got.ResourceType != r.ResourceType {
				t.Errorf("resourceType round-trip: got %q, want %q", got.ResourceType, r.ResourceType)
			}
			if got.Name != r.Name {
				t.Errorf("resourceType+name round-trip: got name %q, want %q", got.Name, r.Name)
			}
		}
		// Type alone should return the full same-type page.
		typeFilter := mustCelEq(t, "resourceType", structpb.NewStringValue(rich.ResourceType))
		if n := queryCount(t, h, typeFilter); n != len(seeds) {
			t.Fatalf("%s: got %d, want %d", typeFilter, n, len(seeds))
		}
	})
	t.Run("envelope_resourceType_in", func(t *testing.T) {
		filter := fmt.Sprintf(`resourceType in [%q]`, rich.ResourceType)
		if n := queryCount(t, h, filter); n != len(seeds) {
			t.Fatalf("%s: got %d, want %d", filter, n, len(seeds))
		}
		// A type that is not activated must fail closed, not silently
		// return an empty page that looks like "no matches."
		_, err := h.client.QueryResources(context.Background(), &pb.QueryResourcesRequest{
			Scope:  "-",
			Filter: `resourceType in ["other.fleetshift.io/Widget"]`,
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("inactive type in-list: err = %v, want InvalidArgument", err)
		}
	})
	t.Run("envelope_name_in", func(t *testing.T) {
		if len(all.Resources) < 2 {
			t.Fatal("need at least 2 results for name in-list")
		}
		a, b := all.Resources[0].Name, all.Resources[1].Name
		filter := fmt.Sprintf(`name in [%q, %q]`, a, b)
		if n := queryCount(t, h, filter); n != 2 {
			t.Fatalf("%s: got %d, want 2", filter, n)
		}
	})

	// --- filterable body fields (verbatim from Struct) ---
	t.Run("resource.name", func(t *testing.T) {
		v := structField(body, "name")
		got := queryOne(t, h, mustCelEq(t, "resource.name", v))
		if structFieldString(got.Resource, "name") != v.GetStringValue() {
			t.Errorf("name mismatch after round-trip")
		}
	})
	t.Run("resource.uid", func(t *testing.T) {
		v := structField(body, "uid")
		if v == nil || v.GetStringValue() == "" {
			t.Fatal("uid missing from body")
		}
		got := queryOne(t, h, mustCelEq(t, "resource.uid", v))
		if structFieldString(got.Resource, "uid") != v.GetStringValue() {
			t.Errorf("uid mismatch after round-trip")
		}
	})
	t.Run("resource.state", func(t *testing.T) {
		// Every seeded state: API enum spelling must filter.
		seen := map[string]bool{}
		for _, r := range all.Resources {
			apiState := structFieldString(r.Resource, "state")
			seen[apiState] = true
			filter := mustCelEq(t, "resource.state", structField(r.Resource, "state"))
			if n := queryCount(t, h, filter); n != 1 {
				t.Fatalf("%s: got %d, want 1", filter, n)
			}
		}
		for _, want := range []string{"CREATING", "ACTIVE", "DELETING", "FAILED"} {
			if !seen[want] {
				t.Errorf("missing API state %q", want)
			}
		}
	})
	t.Run("resource.state_in", func(t *testing.T) {
		filter := `resource.state in ["ACTIVE", "FAILED"]`
		if n := queryCount(t, h, filter); n != 2 {
			t.Fatalf("%s: got %d, want 2", filter, n)
		}
	})
	t.Run("resource.pauseReason", func(t *testing.T) {
		v := structField(body, "pauseReason")
		if v == nil || v.GetStringValue() == "" {
			t.Fatal("pauseReason missing from body")
		}
		got := queryOne(t, h, mustCelEq(t, "resource.pauseReason", v))
		if structFieldString(got.Resource, "pauseReason") != v.GetStringValue() {
			t.Errorf("pauseReason mismatch after round-trip")
		}
	})
	t.Run("resource.intentVersion", func(t *testing.T) {
		v := structField(body, "intentVersion")
		if v == nil {
			t.Fatal("intentVersion missing from body")
		}
		// protojson may encode int64 as a string; use that kind verbatim.
		got := queryOne(t, h, mustCelEq(t, "resource.intentVersion", v)+
			` && resource.name == "clusters/rich"`)
		if !structValuesEqual(structField(got.Resource, "intentVersion"), v) {
			t.Errorf("intentVersion mismatch: got %#v, want %#v",
				structField(got.Resource, "intentVersion"), v)
		}
	})
	t.Run("resource.generation", func(t *testing.T) {
		v := structField(body, "generation")
		if v == nil {
			t.Fatal("generation missing from body")
		}
		got := queryOne(t, h, mustCelEq(t, "resource.generation", v)+
			` && resource.name == "clusters/rich"`)
		if !structValuesEqual(structField(got.Resource, "generation"), v) {
			t.Errorf("generation mismatch after round-trip")
		}
	})
	t.Run("resource.spec", func(t *testing.T) {
		spec := structField(body, "spec")
		if spec == nil || spec.GetStructValue() == nil {
			t.Fatal("spec missing from body")
		}
		specName := structField(spec.GetStructValue(), "name")
		if specName == nil {
			t.Fatal("spec.name missing")
		}
		// Type-specific paths require a resourceType guard.
		filter := `resourceType == "kind.fleetshift.io/Cluster" && ` +
			mustCelEq(t, "resource.spec.name", specName)
		got := queryOne(t, h, filter)
		gotSpec := structField(got.Resource, "spec").GetStructValue()
		if structFieldString(gotSpec, "name") != specName.GetStringValue() {
			t.Errorf("spec.name mismatch after round-trip")
		}
		// Nested list element from the API body.
		nodes := structField(spec.GetStructValue(), "nodes")
		if nodes == nil || len(nodes.GetListValue().GetValues()) == 0 {
			t.Fatal("spec.nodes missing from body")
		}
		role := structField(nodes.GetListValue().GetValues()[0].GetStructValue(), "role")
		if role == nil {
			t.Fatal("spec.nodes[0].role missing")
		}
		// Indexing into repeated fields is not in the CEL subset; filter
		// the scalar we can express, and assert the list shape stayed.
		if role.GetStringValue() != "control-plane" {
			t.Errorf("spec.nodes[0].role = %q", role.GetStringValue())
		}
	})

	// --- response fields that are NOT on the CEL surface today ---
	// Assert they appear (or correctly omit) so a silent drop is caught,
	// and that filtering them is rejected rather than silently ignored.
	t.Run("non_filterable_reconciling", func(t *testing.T) {
		// protojson omits false bools; assert on a creating resource
		// where Reconciling() is true so the field is present.
		var creating *pb.ResourceResult
		for _, r := range all.Resources {
			if structFieldString(r.Resource, "state") == "CREATING" {
				creating = r
				break
			}
		}
		if creating == nil {
			t.Fatal("CREATING resource missing")
		}
		v := structField(creating.Resource, "reconciling")
		if v == nil {
			t.Fatal("reconciling missing from CREATING body (want true)")
		}
		if !v.GetBoolValue() {
			t.Fatalf("reconciling = false, want true for CREATING")
		}
		assertFilterUnsupported(t, h, "resource.reconciling == true")
	})
	t.Run("non_filterable_create_time", func(t *testing.T) {
		v := structField(body, "createTime")
		if v == nil || v.GetStringValue() == "" {
			t.Fatal("createTime missing from body")
		}
		assertFilterUnsupported(t, h, fmt.Sprintf(`resource.createTime == %q`, v.GetStringValue()))
	})
	t.Run("non_filterable_update_time", func(t *testing.T) {
		v := structField(body, "updateTime")
		if v == nil || v.GetStringValue() == "" {
			t.Fatal("updateTime missing from body")
		}
		assertFilterUnsupported(t, h, fmt.Sprintf(`resource.updateTime == %q`, v.GetStringValue()))
	})
	t.Run("non_filterable_delete_time", func(t *testing.T) {
		// Soft-delete unset on this fixture: field may be absent or empty.
		if v := structField(body, "deleteTime"); v != nil && v.GetStringValue() != "" {
			t.Errorf("deleteTime unexpectedly set: %q", v.GetStringValue())
		}
		assertFilterUnsupported(t, h, `resource.deleteTime == "2026-01-01T00:00:00Z"`)
	})
	t.Run("non_filterable_etag", func(t *testing.T) {
		v := structField(body, "etag")
		if v == nil || v.GetStringValue() == "" {
			t.Fatal("etag missing from body")
		}
		assertFilterUnsupported(t, h, mustCelEq(t, "resource.etag", v))
	})
	t.Run("non_filterable_provenance", func(t *testing.T) {
		v := structField(body, "provenance")
		if v == nil || v.GetStructValue() == nil {
			t.Fatal("provenance missing from body (seeded on rich resource)")
		}
		assertFilterUnsupported(t, h, `resource.provenance != null`)
	})
}

func assertFilterUnsupported(t *testing.T, h *resourceQueryHarness, filter string) {
	t.Helper()
	_, err := h.client.QueryResources(context.Background(), &pb.QueryResourcesRequest{
		Scope: "-", Filter: filter, PageSize: 10,
	})
	if err == nil {
		t.Fatalf("filter %q: want error (field not on CEL surface), got success", filter)
	}
	// Transport maps domain.ErrInvalidArgument → InvalidArgument.
	if !strings.Contains(err.Error(), "InvalidArgument") && !strings.Contains(strings.ToLower(err.Error()), "unsupported") {
		t.Fatalf("filter %q: err = %v, want InvalidArgument/unsupported", filter, err)
	}
}

func structValuesEqual(a, b *structpb.Value) bool {
	if a == nil || b == nil {
		return a == b
	}
	// Normalize int64-as-string vs number for protojson variance.
	as, aStr := numericAsString(a)
	bs, bStr := numericAsString(b)
	if aStr && bStr {
		return as == bs
	}
	return a.String() == b.String()
}

func numericAsString(v *structpb.Value) (string, bool) {
	switch k := v.Kind.(type) {
	case *structpb.Value_StringValue:
		if _, err := strconv.ParseInt(k.StringValue, 10, 64); err == nil {
			return k.StringValue, true
		}
	case *structpb.Value_NumberValue:
		if k.NumberValue == float64(int64(k.NumberValue)) {
			return strconv.FormatInt(int64(k.NumberValue), 10), true
		}
	}
	return "", false
}

// TestResourceQuery_PresenceRoundTrip asserts has() / container-key in
// match if and only if the corresponding key appears on
// ResourceResult.resource for reachable writer shapes (including
// condition default omission and missing inventory).
func TestResourceQuery_PresenceRoundTrip(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	registry := extensionresource.NewActiveResourceRegistry()
	store := &sqlite.Store{DB: db, SchemaProvider: registry}

	cfg := kindClusterConfig(t)
	cfg.Capabilities.Inventory = &extensionresource.InventoryCapabilityConfig{}
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
		domain.WithInventory(),
	)
	if err := tx.ExtensionResources().CreateType(ctx, typeDef); err != nil {
		tx.Rollback()
		t.Fatalf("CreateType: %v", err)
	}

	// Managed cluster with intent + pauseReason, no inventory report.
	fID := domain.FulfillmentID("ful-presence")
	if err := tx.Fulfillments().Create(ctx, domain.FulfillmentFromSnapshot(domain.FulfillmentSnapshot{
		ID: fID, State: domain.FulfillmentStateActive, PauseReason: "waiting",
		Generation: 2, CreatedAt: now, UpdatedAt: now,
	})); err != nil {
		tx.Rollback()
		t.Fatalf("Create fulfillment: %v", err)
	}
	managedUID := domain.NewExtensionResourceUID()
	managed := domain.NewExtensionResource(managedUID, cfg.ResourceType,
		domain.ResourceName("clusters/presence"), now,
		domain.WithManagedState(fID),
		domain.WithExtensionLabels(map[string]string{"team": "platform"}),
	)
	if _, err := managed.RecordIntent(json.RawMessage(`{"name":"presence"}`), now); err != nil {
		tx.Rollback()
		t.Fatalf("RecordIntent: %v", err)
	}
	if err := tx.ExtensionResources().Create(ctx, managed); err != nil {
		tx.Rollback()
		t.Fatalf("Create managed: %v", err)
	}

	// Second managed cluster with inventory report (default-omitted condition fields).
	fID2 := domain.FulfillmentID("ful-presence-inv")
	if err := tx.Fulfillments().Create(ctx, domain.FulfillmentFromSnapshot(domain.FulfillmentSnapshot{
		ID: fID2, State: domain.FulfillmentStateActive, Generation: 1, CreatedAt: now, UpdatedAt: now,
	})); err != nil {
		tx.Rollback()
		t.Fatalf("Create fulfillment 2: %v", err)
	}
	invUID := domain.NewExtensionResourceUID()
	withInv := domain.NewExtensionResource(invUID, cfg.ResourceType,
		domain.ResourceName("clusters/with-inv"), now, domain.WithManagedState(fID2))
	if _, err := withInv.RecordIntent(json.RawMessage(`{"name":"with-inv"}`), now); err != nil {
		tx.Rollback()
		t.Fatalf("RecordIntent 2: %v", err)
	}
	if err := tx.ExtensionResources().Create(ctx, withInv); err != nil {
		tx.Rollback()
		t.Fatalf("Create with-inv: %v", err)
	}
	ready, err := domain.NewCondition("Ready", domain.ConditionTrue, "", "", time.Time{})
	if err != nil {
		tx.Rollback()
		t.Fatalf("NewCondition: %v", err)
	}
	obs := json.RawMessage(`{}`)
	if err := tx.ExtensionResources().ReplaceInventory(ctx, []domain.InventoryReplacement{{
		ResourceType: cfg.ResourceType,
		Name:         domain.ResourceName("clusters/with-inv"),
		CandidateUID: invUID,
		Labels:       map[string]string{"node-role": "worker"},
		Observation:  &obs,
		Conditions:   []domain.Condition{ready},
		ObservedAt:   now,
		ReceivedAt:   now,
	}}); err != nil {
		tx.Rollback()
		t.Fatalf("ReplaceInventory: %v", err)
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

	query := func(t *testing.T, filter string) []*pb.ResourceResult {
		t.Helper()
		page, err := client.QueryResources(context.Background(), &pb.QueryResourcesRequest{
			Scope: "-", Filter: filter, PageSize: 50,
		})
		if err != nil {
			t.Fatalf("QueryResources(%s): %v", filter, err)
		}
		return page.Resources
	}
	assertBodyKey := func(t *testing.T, body *structpb.Struct, key string, wantPresent bool) {
		t.Helper()
		_, present := body.Fields[key]
		if present != wantPresent {
			t.Fatalf("body key %q present=%v, want %v; fields=%v", key, present, wantPresent, body.Fields)
		}
	}

	t.Run("managed_no_inventory", func(t *testing.T) {
		results := query(t, `name == "//kind.fleetshift.io/clusters/presence"`)
		if len(results) != 1 {
			t.Fatalf("len=%d", len(results))
		}
		body := results[0].Resource
		assertBodyKey(t, body, "spec", true)
		assertBodyKey(t, body, "pauseReason", true)
		assertBodyKey(t, body, "observation", false)
		assertBodyKey(t, body, "localLabels", false)
		assertBodyKey(t, body, "conditions", false)

		if n := len(query(t, `has(resource.spec) && name == "//kind.fleetshift.io/clusters/presence"`)); n != 1 {
			t.Fatalf("has(spec)=%d", n)
		}
		if n := len(query(t, `"spec" in resource && name == "//kind.fleetshift.io/clusters/presence"`)); n != 1 {
			t.Fatalf("spec in resource=%d", n)
		}
		if n := len(query(t, `has(resource.pauseReason) && name == "//kind.fleetshift.io/clusters/presence"`)); n != 1 {
			t.Fatalf("has(pauseReason)=%d", n)
		}
		if n := len(query(t, `has(resource.observation) && name == "//kind.fleetshift.io/clusters/presence"`)); n != 0 {
			t.Fatalf("has(observation) on managed-only=%d", n)
		}
		if n := len(query(t, `!has(resource.observation) && name == "//kind.fleetshift.io/clusters/presence"`)); n != 1 {
			t.Fatalf("!has(observation)=%d", n)
		}
	})

	t.Run("inventory_condition_defaults", func(t *testing.T) {
		results := query(t, `name == "//kind.fleetshift.io/clusters/with-inv"`)
		if len(results) != 1 {
			t.Fatalf("len=%d", len(results))
		}
		body := results[0].Resource
		assertBodyKey(t, body, "observation", true)
		assertBodyKey(t, body, "conditions", true)
		conds := body.Fields["conditions"].GetStructValue()
		if conds == nil {
			t.Fatal("conditions not a struct")
		}
		ready := conds.Fields["Ready"].GetStructValue()
		if ready == nil {
			t.Fatal("Ready condition missing")
		}
		if _, ok := ready.Fields["status"]; !ok {
			t.Fatal("status missing from projected Ready")
		}
		if _, ok := ready.Fields["reason"]; ok {
			t.Fatal("empty reason should be omitted from projected Ready")
		}
		if _, ok := ready.Fields["message"]; ok {
			t.Fatal("empty message should be omitted from projected Ready")
		}
		if _, ok := ready.Fields["lastTransitionTime"]; ok {
			t.Fatal("zero lastTransitionTime should be omitted from projected Ready")
		}

		if n := len(query(t, `has(resource.conditions.Ready.status) && name == "//kind.fleetshift.io/clusters/with-inv"`)); n != 1 {
			t.Fatalf("has(status)=%d", n)
		}
		if n := len(query(t, `has(resource.conditions.Ready.reason) && name == "//kind.fleetshift.io/clusters/with-inv"`)); n != 0 {
			t.Fatalf("has(reason) with empty=%d", n)
		}
		if n := len(query(t, `"reason" in resource.conditions.Ready && name == "//kind.fleetshift.io/clusters/with-inv"`)); n != 0 {
			t.Fatalf("reason in Ready=%d", n)
		}
		if n := len(query(t, `has(resource.observation) && name == "//kind.fleetshift.io/clusters/with-inv"`)); n != 1 {
			t.Fatalf("has(empty observation)=%d", n)
		}
	})
}
