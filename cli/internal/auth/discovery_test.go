package auth_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
)

func TestDiscoverEndpoints_OK(t *testing.T) {
	var testIssuer string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/dex/.well-known/openid-configuration" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{
			"issuer":                 testIssuer,
			"authorization_endpoint": testIssuer + "/auth",
			"token_endpoint":         testIssuer + "/token",
		})
	}))
	t.Cleanup(srv.Close)
	testIssuer = srv.URL + "/dex"

	got, err := auth.DiscoverEndpoints(context.Background(), testIssuer, srv.Client())
	if err != nil {
		t.Fatalf("DiscoverEndpoints: %v", err)
	}
	if got.AuthorizationEndpoint != testIssuer+"/auth" {
		t.Errorf("AuthorizationEndpoint = %q, want %q", got.AuthorizationEndpoint, testIssuer+"/auth")
	}
	if got.TokenEndpoint != testIssuer+"/token" {
		t.Errorf("TokenEndpoint = %q, want %q", got.TokenEndpoint, testIssuer+"/token")
	}
}

func TestDiscoverEndpoints_TrailingSlashIssuer(t *testing.T) {
	var testIssuer string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/.well-known/openid-configuration") {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{
			"issuer":                 strings.TrimRight(testIssuer, "/"),
			"authorization_endpoint": strings.TrimRight(testIssuer, "/") + "/auth",
			"token_endpoint":         strings.TrimRight(testIssuer, "/") + "/token",
		})
	}))
	t.Cleanup(srv.Close)
	testIssuer = srv.URL + "/dex/"

	got, err := auth.DiscoverEndpoints(context.Background(), testIssuer, srv.Client())
	if err != nil {
		t.Fatalf("DiscoverEndpoints: %v", err)
	}
	wantAuth := strings.TrimRight(testIssuer, "/") + "/auth"
	if got.AuthorizationEndpoint != wantAuth {
		t.Errorf("AuthorizationEndpoint = %q, want %q", got.AuthorizationEndpoint, wantAuth)
	}
}

func TestDiscoverEndpoints_IssuerMismatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"issuer":                 "https://other.example/dex",
			"authorization_endpoint": "https://other.example/dex/auth",
			"token_endpoint":         "https://other.example/dex/token",
		})
	}))
	t.Cleanup(srv.Close)

	_, err := auth.DiscoverEndpoints(context.Background(), srv.URL+"/dex", srv.Client())
	if err == nil {
		t.Fatal("expected issuer mismatch error")
	}
}

func TestDiscoverEndpoints_MissingEndpoints(t *testing.T) {
	var testIssuer string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"issuer": testIssuer,
		})
	}))
	t.Cleanup(srv.Close)
	testIssuer = srv.URL + "/dex"

	_, err := auth.DiscoverEndpoints(context.Background(), testIssuer, srv.Client())
	if err == nil {
		t.Fatal("expected missing endpoint error")
	}
}

func TestDiscoverEndpoints_EmptyIssuer(t *testing.T) {
	_, err := auth.DiscoverEndpoints(context.Background(), "  ", nil)
	if err == nil {
		t.Fatal("expected error for empty issuer")
	}
}

func TestDiscoverEndpoints_NonOKStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)

	_, err := auth.DiscoverEndpoints(context.Background(), srv.URL+"/dex", srv.Client())
	if err == nil {
		t.Fatal("expected non-OK status error")
	}
	if !strings.Contains(err.Error(), "503") {
		t.Fatalf("error = %v, want status 503", err)
	}
}

func TestDiscoverEndpoints_InvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{not-json`))
	}))
	t.Cleanup(srv.Close)

	_, err := auth.DiscoverEndpoints(context.Background(), srv.URL+"/dex", srv.Client())
	if err == nil {
		t.Fatal("expected decode error")
	}
	if !strings.Contains(err.Error(), "decode") {
		t.Fatalf("error = %v, want decode failure", err)
	}
}

func TestDiscoverEndpoints_HonorsCallerCancellation(t *testing.T) {
	started := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)

	// Cancel only after the handler has been entered so cancellation cannot
	// race with dial/scheduling and miss the server (false "handler was not
	// reached"). Bounded waits also prevent a hang if context is not wired
	// into the request.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := auth.DiscoverEndpoints(ctx, srv.URL+"/dex", srv.Client())
		errCh <- err
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not reached")
	}

	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("DiscoverEndpoints did not return after cancel")
	}
}
