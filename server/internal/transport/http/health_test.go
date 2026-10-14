package http_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	transporthttp "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/http"
)

func TestHealthRoutes_LivenessAlwaysOK(t *testing.T) {
	readiness := &transporthttp.Readiness{}
	mux := http.NewServeMux()
	transporthttp.RegisterHealthRoutes(mux, readiness)

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		req := httptest.NewRequest(method, "/livez", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s /livez status = %d, want 200", method, rec.Code)
		}
		if got := rec.Header().Get("Cache-Control"); got != "no-store" {
			t.Fatalf("Cache-Control = %q, want no-store", got)
		}
		if got := rec.Header().Get("Content-Type"); got != "text/plain; charset=utf-8" {
			t.Fatalf("Content-Type = %q", got)
		}
		body, _ := io.ReadAll(rec.Body)
		if method == http.MethodGet {
			if string(body) != "ok" {
				t.Fatalf("GET /livez body = %q, want ok", body)
			}
		} else if len(body) != 0 {
			t.Fatalf("HEAD /livez body = %q, want empty", body)
		}
	}
}

func TestHealthRoutes_Readiness(t *testing.T) {
	readiness := &transporthttp.Readiness{}
	mux := http.NewServeMux()
	transporthttp.RegisterHealthRoutes(mux, readiness)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("before ready: status = %d, want 503", rec.Code)
	}

	readiness.MarkReady()
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("after ready: status = %d, want 200", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	if string(body) != "ok" {
		t.Fatalf("body = %q, want ok", body)
	}

	readiness.ClearReady()
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("after clear: status = %d, want 503", rec.Code)
	}
}

func TestHealthRoutes_MethodNotAllowed(t *testing.T) {
	readiness := &transporthttp.Readiness{}
	mux := http.NewServeMux()
	transporthttp.RegisterHealthRoutes(mux, readiness)

	for _, path := range []string{"/livez", "/readyz"} {
		req := httptest.NewRequest(http.MethodPost, path, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("POST %s status = %d, want 405", path, rec.Code)
		}
		if got := rec.Header().Get("Allow"); got != "GET, HEAD" {
			t.Fatalf("Allow = %q, want GET, HEAD", got)
		}
		if got := rec.Header().Get("Cache-Control"); got != "no-store" {
			t.Fatalf("Cache-Control = %q, want no-store", got)
		}
	}
}
