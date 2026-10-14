package extensionresource_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"buf.build/go/protovalidate"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/extensionresource"
)

// TestBuildHTTPHandler_UsesRegisteredPrefix proves that the prefix
// passed to BuildHTTPHandler is what gets trimmed for ID extraction.
// Activator's replace path must pass the *new* canonical prefix; using
// the old one leaves a path fragment in the ID, so Get looks up the
// wrong resource name.
func TestBuildHTTPHandler_UsesRegisteredPrefix(t *testing.T) {
	svc := buildFullClusterService(t)
	addr, conn, _ := serveGRPCOverTCP(t, svc)
	_ = addr

	const (
		oldPrefix = "/apis/kind.fleetshift.io/v1/clusters"
		newPrefix = "/apis/kindv2.fleetshift.io/v1/clusters"
		id        = "prefix-parse-id"
	)

	// Seed a resource under the clean ID via the correct handler.
	create := extensionresource.BuildHTTPHandler(svc, conn, newPrefix)
	createReq := httptest.NewRequest(http.MethodPost, newPrefix+"?cluster_id="+id,
		strings.NewReader(`{"spec":{"name":"`+id+`"}}`))
	createRec := httptest.NewRecorder()
	create(createRec, createReq)
	if createRec.Code != http.StatusOK && createRec.Code != http.StatusCreated {
		t.Fatalf("create status = %d body = %s", createRec.Code, createRec.Body.String())
	}

	// Correct prefix: ID trims to "prefix-parse-id" → found.
	right := extensionresource.BuildHTTPHandler(svc, conn, newPrefix)
	reqOK := httptest.NewRequest(http.MethodGet, newPrefix+"/"+id, nil)
	recOK := httptest.NewRecorder()
	right(recOK, reqOK)
	bodyOK, _ := io.ReadAll(recOK.Result().Body)
	if recOK.Code != http.StatusOK {
		t.Fatalf("correct-prefix GET status = %d body = %s, want 200", recOK.Code, bodyOK)
	}
	if !strings.Contains(string(bodyOK), "clusters/"+id) {
		t.Fatalf("correct-prefix GET body = %s, want name clusters/%s", bodyOK, id)
	}

	// Wrong prefix (the Activate bug): TrimPrefix does not strip, so the
	// ID becomes "apis/kindv2.../prefix-parse-id" and Get misses.
	wrong := extensionresource.BuildHTTPHandler(svc, conn, oldPrefix)
	reqBad := httptest.NewRequest(http.MethodGet, newPrefix+"/"+id, nil)
	recBad := httptest.NewRecorder()
	wrong(recBad, reqBad)
	bodyBad, _ := io.ReadAll(recBad.Result().Body)
	if recBad.Code != http.StatusNotFound {
		t.Fatalf("wrong-prefix GET status = %d body = %s, want 404 (malformed ID lookup)", recBad.Code, bodyBad)
	}
}

func buildInventoryOnlyNodeService(t *testing.T) *extensionresource.RegisteredService {
	t.Helper()
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("protovalidate.New: %v", err)
	}
	cfg := &extensionresource.ResourceTypeConfig{
		CollectionConfig: dynamicapi.CollectionConfig{
			Version:      "v1",
			CollectionID: "nodes",
			Singular:     "Node",
			Plural:       "Nodes",
		},
		ResourceType: "test.fleetshift.io/Node",
		ProtoPackage: "test.fleetshift.v1",
		Capabilities: extensionresource.ResourceCapabilities{
			Inventory: &extensionresource.InventoryCapabilityConfig{},
		},
	}
	svc, err := extensionresource.Build(cfg, extensionresource.Deps{Validator: validator})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return svc
}

// TestBuildHTTPHandler_InventoryOnlyUnsupportedVerbs404 proves that
// Create/Delete/Resume on an inventory-only collection return 404 from
// the capability-aware handler without panicking on nil request
// descriptors.
func TestBuildHTTPHandler_InventoryOnlyUnsupportedVerbs404(t *testing.T) {
	svc := buildInventoryOnlyNodeService(t)
	// No live gRPC needed — unsupported verbs must 404 before proxying.
	handler := extensionresource.BuildHTTPHandler(svc, nil, svc.Config.CanonicalHTTPPrefix())
	prefix := svc.Config.CanonicalHTTPPrefix()

	cases := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"create", http.MethodPost, prefix + "?node_id=n1", `{}`},
		{"delete", http.MethodDelete, prefix + "/n1", ""},
		{"resume", http.MethodPost, prefix + "/n1:resume", `{}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var body io.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}
			req := httptest.NewRequest(tc.method, tc.path, body)
			rec := httptest.NewRecorder()
			handler(rec, req)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("status = %d body = %s, want 404", rec.Code, rec.Body.String())
			}
		})
	}

	// Managed types still expose Create (smoke: descriptor present).
	managed := buildFullClusterService(t)
	if managed.Descriptors.CreateRequest == nil {
		t.Fatal("managed CreateRequest should be non-nil")
	}
	if len(managed.Desc.Methods) != 5 {
		t.Fatalf("managed method count = %d, want 5", len(managed.Desc.Methods))
	}
	if len(svc.Desc.Methods) != 2 {
		t.Fatalf("inventory-only method count = %d, want 2 (Get/List)", len(svc.Desc.Methods))
	}
}
