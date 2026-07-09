package managedresource_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/managedresource"
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
	create := managedresource.BuildHTTPHandler(svc, conn, newPrefix)
	createReq := httptest.NewRequest(http.MethodPost, newPrefix+"?cluster_id="+id,
		strings.NewReader(`{"spec":{"name":"`+id+`"}}`))
	createRec := httptest.NewRecorder()
	create(createRec, createReq)
	if createRec.Code != http.StatusOK && createRec.Code != http.StatusCreated {
		t.Fatalf("create status = %d body = %s", createRec.Code, createRec.Body.String())
	}

	// Correct prefix: ID trims to "prefix-parse-id" → found.
	right := managedresource.BuildHTTPHandler(svc, conn, newPrefix)
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
	wrong := managedresource.BuildHTTPHandler(svc, conn, oldPrefix)
	reqBad := httptest.NewRequest(http.MethodGet, newPrefix+"/"+id, nil)
	recBad := httptest.NewRecorder()
	wrong(recBad, reqBad)
	bodyBad, _ := io.ReadAll(recBad.Result().Body)
	if recBad.Code != http.StatusNotFound {
		t.Fatalf("wrong-prefix GET status = %d body = %s, want 404 (malformed ID lookup)", recBad.Code, bodyBad)
	}
}
