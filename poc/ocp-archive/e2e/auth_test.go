//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestDiscoverOIDC(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/.well-known/openid-configuration" {
			t.Errorf("unexpected path: %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		json.NewEncoder(w).Encode(OIDCDiscovery{
			TokenEndpoint:               srv.URL + "/token",
			DeviceAuthorizationEndpoint: srv.URL + "/device",
		})
	}))
	defer srv.Close()

	doc, err := discoverOIDC(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("discoverOIDC failed: %v", err)
	}
	if doc.DeviceAuthorizationEndpoint != srv.URL+"/device" {
		t.Errorf("device endpoint = %q, want %q", doc.DeviceAuthorizationEndpoint, srv.URL+"/device")
	}
	if doc.TokenEndpoint != srv.URL+"/token" {
		t.Errorf("token endpoint = %q, want %q", doc.TokenEndpoint, srv.URL+"/token")
	}
}

func TestDiscoverOIDC_NoDeviceEndpoint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(OIDCDiscovery{
			TokenEndpoint: "https://example.com/token",
			// DeviceAuthorizationEndpoint intentionally omitted
		})
	}))
	defer srv.Close()

	_, err := discoverOIDC(context.Background(), srv.URL)
	if err == nil {
		t.Fatal("expected error when device_authorization_endpoint is missing")
	}
	if got := err.Error(); !strings.Contains(got, "device_authorization_endpoint") {
		t.Errorf("error = %q, should mention device_authorization_endpoint", got)
	}
}

func TestPollForToken_Success(t *testing.T) {
	var calls atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n <= 2 {
			// First two calls: authorization_pending
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "authorization_pending",
			})
			return
		}
		// Third call: success
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(TokenResponse{
			AccessToken: "test-access-token",
			TokenType:   "Bearer",
			IDToken:     "test-id-token",
			ExpiresIn:   3600,
		})
	}))
	defer srv.Close()

	token, err := pollForToken(context.Background(), srv.URL+"/token", "test-client", "test-device-code", "", 0)
	if err != nil {
		t.Fatalf("pollForToken failed: %v", err)
	}
	if token.AccessToken != "test-access-token" {
		t.Errorf("access_token = %q, want test-access-token", token.AccessToken)
	}
	if token.IDToken != "test-id-token" {
		t.Errorf("id_token = %q, want test-id-token", token.IDToken)
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("expected 3 calls to token endpoint, got %d", got)
	}
}

func TestPollForToken_Expired(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "expired_token",
		})
	}))
	defer srv.Close()

	_, err := pollForToken(context.Background(), srv.URL+"/token", "test-client", "test-device-code", "", 0)
	if err == nil {
		t.Fatal("expected error for expired token")
	}
	if got := err.Error(); !strings.Contains(got, "expired") {
		t.Errorf("error = %q, should mention expiration", got)
	}
}

