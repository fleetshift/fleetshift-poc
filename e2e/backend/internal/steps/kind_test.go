package steps

import (
	"context"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/e2e/backend/internal/harness"
)

func TestUniqueKindClusterID(t *testing.T) {
	t.Parallel()
	got := UniqueKindClusterID(t)
	if !strings.HasPrefix(got, harness.KindClusterIDPrefix) {
		t.Fatalf("UniqueKindClusterID() = %q, want prefix %q", got, harness.KindClusterIDPrefix)
	}
	suffix := strings.TrimPrefix(got, harness.KindClusterIDPrefix)
	if len(suffix) != 8 {
		t.Fatalf("UniqueKindClusterID() suffix %q, want 8 hex chars", suffix)
	}
}

func TestProbeKindOIDC_OK(t *testing.T) {
	t.Parallel()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != kindOIDCProbePath {
			t.Errorf("path = %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer tok" {
			t.Errorf("Authorization = %q", got)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"kind":"NamespaceList"}`)
	}))
	t.Cleanup(srv.Close)
	if err := probeKindOIDC(context.Background(), srv.URL, tlsServerCA(t, srv), "tok"); err != nil {
		t.Fatal(err)
	}
}

func TestProbeKindOIDC_Unauthorized(t *testing.T) {
	t.Parallel()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"message":"the server has asked for the client to provide credentials"}`)
	}))
	t.Cleanup(srv.Close)
	err := probeKindOIDC(context.Background(), srv.URL, tlsServerCA(t, srv), "tok")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Fatalf("error = %v, want 401", err)
	}
	if !strings.Contains(err.Error(), "provide credentials") {
		t.Fatalf("error = %v, want credentials message", err)
	}
}

func TestProbeKindOIDC_EmptyToken(t *testing.T) {
	t.Parallel()
	if err := probeKindOIDC(context.Background(), "https://127.0.0.1:6443", []byte("x"), ""); err == nil {
		t.Fatal("expected error")
	}
}

func TestProbeKindOIDC_BadCA(t *testing.T) {
	t.Parallel()
	err := probeKindOIDC(context.Background(), "https://127.0.0.1:6443", []byte("not pem"), "tok")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "cluster CA") {
		t.Fatalf("error = %v, want cluster CA", err)
	}
}

func tlsServerCA(t *testing.T, srv *httptest.Server) []byte {
	t.Helper()
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
}
