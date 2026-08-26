package steps

import (
	"context"
	"encoding/pem"
	"errors"
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
	srv := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("must not send a request with an empty token")
	}))
	t.Cleanup(srv.Close)
	err := probeKindOIDC(context.Background(), srv.URL, tlsServerCA(t, srv), "")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "empty access token") {
		t.Fatalf("error = %v, want empty access token", err)
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

func TestParseMetadataName(t *testing.T) {
	t.Parallel()
	got, err := parseMetadataName([]byte(`{"metadata":{"name":"e2e-abcd"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if got != "e2e-abcd" {
		t.Fatalf("got %q", got)
	}
}

func TestParseMetadataName_Invalid(t *testing.T) {
	t.Parallel()
	if _, err := parseMetadataName([]byte("{")); err == nil {
		t.Fatal("expected error")
	}
}

func TestKubectlNotFound(t *testing.T) {
	t.Parallel()
	err := errors.New("exit status 1")
	if !kubectlNotFound([]byte(`Error from server (NotFound): configmaps "test-config" not found`), err) {
		t.Fatal("NotFound output must match")
	}
	if kubectlNotFound([]byte(`Error from server (NotFound): configmaps "test-config" not found`), nil) {
		t.Fatal("success is not NotFound")
	}
	if kubectlNotFound([]byte("Unable to connect to the server"), err) {
		t.Fatal("transport error must not match")
	}
	if kubectlNotFound(nil, err) {
		t.Fatal("empty output must not match")
	}
}

func TestKindAPIRequest_PostCreated(t *testing.T) {
	t.Parallel()
	const body = `{"kind":"Namespace"}`
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s", r.Method)
		}
		if r.URL.Path != kindOIDCProbePath {
			t.Errorf("path = %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer tok" {
			t.Errorf("Authorization = %q", r.Header.Get("Authorization"))
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("Content-Type = %q", r.Header.Get("Content-Type"))
		}
		got, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		if string(got) != body {
			t.Errorf("body = %q, want %q", got, body)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	t.Cleanup(srv.Close)
	st, _, err := kindAPIRequest(context.Background(), srv.URL, tlsServerCA(t, srv), "tok", http.MethodPost, kindOIDCProbePath, []byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if st != http.StatusCreated {
		t.Fatalf("status = %d", st)
	}
}

func TestKindAPIRequest_Conflict(t *testing.T) {
	t.Parallel()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(w, `{"reason":"AlreadyExists"}`)
	}))
	t.Cleanup(srv.Close)
	st, resp, err := kindAPIRequest(context.Background(), srv.URL+"/", tlsServerCA(t, srv), "tok", http.MethodPost, kindOIDCProbePath, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if st != http.StatusConflict {
		t.Fatalf("status = %d", st)
	}
	if !strings.Contains(string(resp), "AlreadyExists") {
		t.Fatalf("body = %q", resp)
	}
}

func TestKindAPIRequest_Forbidden(t *testing.T) {
	t.Parallel()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") != "" {
			t.Errorf("GET Content-Type = %q, want empty", r.Header.Get("Content-Type"))
		}
		w.WriteHeader(http.StatusForbidden)
	}))
	t.Cleanup(srv.Close)
	st, _, err := kindAPIRequest(context.Background(), srv.URL, tlsServerCA(t, srv), "tok", http.MethodGet, kindOIDCProbePath, nil)
	if err != nil {
		t.Fatal(err)
	}
	if st != http.StatusForbidden {
		t.Fatalf("status = %d", st)
	}
}

func TestKindAPIRequest_Canceled(t *testing.T) {
	t.Parallel()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("must not send a request with a canceled context")
	}))
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := kindAPIRequest(ctx, srv.URL, tlsServerCA(t, srv), "tok", http.MethodGet, kindOIDCProbePath, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

func tlsServerCA(t *testing.T, srv *httptest.Server) []byte {
	t.Helper()
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
}
