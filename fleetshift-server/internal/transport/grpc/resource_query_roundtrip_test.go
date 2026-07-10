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
		// protojson encodes int64 as a decimal string; emit a CEL int
		// literal so the value still comes from the API while matching
		// integer SQL columns (intent_version, generation).
		if i, err := strconv.ParseInt(k.StringValue, 10, 64); err == nil {
			return fmt.Sprintf(`%s == %d`, field, i), nil
		}
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
var responseEnvelopeFields = []string{"name", "resource_type", "resource"}

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
		Filter:   `resource_type == "kind.fleetshift.io/Cluster"`,
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

	// --- envelope fields (ResourceResult.name / resource_type) ---
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
	t.Run("envelope_resource_type", func(t *testing.T) {
		for _, r := range all.Resources {
			if r.ResourceType == "" {
				t.Fatal("empty resource_type")
			}
			// Disambiguate among same-type siblings with the envelope name.
			filter := mustCelEq(t, "resource_type", structpb.NewStringValue(r.ResourceType)) +
				" && " + mustCelEq(t, "name", structpb.NewStringValue(r.Name))
			got := queryOne(t, h, filter)
			if got.ResourceType != r.ResourceType {
				t.Errorf("resource_type round-trip: got %q, want %q", got.ResourceType, r.ResourceType)
			}
			if got.Name != r.Name {
				t.Errorf("resource_type+name round-trip: got name %q, want %q", got.Name, r.Name)
			}
		}
		// Type alone should return the full same-type page.
		typeFilter := mustCelEq(t, "resource_type", structpb.NewStringValue(rich.ResourceType))
		if n := queryCount(t, h, typeFilter); n != len(seeds) {
			t.Fatalf("%s: got %d, want %d", typeFilter, n, len(seeds))
		}
	})
	t.Run("envelope_resource_type_in", func(t *testing.T) {
		filter := fmt.Sprintf(`resource_type in [%q]`, rich.ResourceType)
		if n := queryCount(t, h, filter); n != len(seeds) {
			t.Fatalf("%s: got %d, want %d", filter, n, len(seeds))
		}
		// A type that is not activated must fail closed, not silently
		// return an empty page that looks like "no matches."
		_, err := h.client.QueryResources(context.Background(), &pb.QueryResourcesRequest{
			Scope:  "-",
			Filter: `resource_type in ["other.fleetshift.io/Widget"]`,
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
	t.Run("resource.pause_reason", func(t *testing.T) {
		v := structField(body, "pauseReason")
		if v == nil || v.GetStringValue() == "" {
			t.Fatal("pauseReason missing from body")
		}
		got := queryOne(t, h, mustCelEq(t, "resource.pause_reason", v))
		if structFieldString(got.Resource, "pauseReason") != v.GetStringValue() {
			t.Errorf("pauseReason mismatch after round-trip")
		}
	})
	t.Run("resource.intent_version", func(t *testing.T) {
		v := structField(body, "intentVersion")
		if v == nil {
			t.Fatal("intentVersion missing from body")
		}
		// protojson may encode int64 as a string; use that kind verbatim.
		got := queryOne(t, h, mustCelEq(t, "resource.intent_version", v)+
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
		// Type-specific paths require a resource_type guard.
		filter := `resource_type == "kind.fleetshift.io/Cluster" && ` +
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
		assertFilterUnsupported(t, h, fmt.Sprintf(`resource.create_time == %q`, v.GetStringValue()))
	})
	t.Run("non_filterable_update_time", func(t *testing.T) {
		v := structField(body, "updateTime")
		if v == nil || v.GetStringValue() == "" {
			t.Fatal("updateTime missing from body")
		}
		assertFilterUnsupported(t, h, fmt.Sprintf(`resource.update_time == %q`, v.GetStringValue()))
	})
	t.Run("non_filterable_delete_time", func(t *testing.T) {
		// Soft-delete unset on this fixture: field may be absent or empty.
		if v := structField(body, "deleteTime"); v != nil && v.GetStringValue() != "" {
			t.Errorf("deleteTime unexpectedly set: %q", v.GetStringValue())
		}
		assertFilterUnsupported(t, h, `resource.delete_time == "2026-01-01T00:00:00Z"`)
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
