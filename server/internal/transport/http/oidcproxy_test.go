package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// newDexStub returns an httptest server that mimics the peer Dex endpoints the
// browser touches: a discovery document with absolute endpoint URLs, a keys
// endpoint, and an /auth endpoint that 302s with an absolute Location.
func newDexStub(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	var origin string // captured after the server starts
	mux.HandleFunc("/dex/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		doc := map[string]any{
			"issuer":                 origin + "/dex",
			"authorization_endpoint": origin + "/dex/auth",
			"token_endpoint":         origin + "/dex/token",
			"jwks_uri":               origin + "/dex/keys",
			"userinfo_endpoint":      origin + "/dex/userinfo",
			"scopes_supported":       []string{"openid", "email"},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(doc)
	})
	mux.HandleFunc("/dex/keys", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"keys":[]}`))
	})
	mux.HandleFunc("/dex/auth", func(w http.ResponseWriter, r *http.Request) {
		// Dex sometimes emits an absolute Location and a Secure cookie.
		w.Header().Set("Location", origin+"/dex/approval?req=abc")
		w.Header().Set("Set-Cookie", "dex_session=xyz; Path=/dex; Secure; HttpOnly")
		w.WriteHeader(http.StatusFound)
	})
	ts := httptest.NewServer(mux)
	origin = ts.URL
	t.Cleanup(ts.Close)
	return ts
}

func TestOIDCLoopbackProxyDiscoveryRewrite(t *testing.T) {
	ts := newDexStub(t)
	upstream, _ := url.Parse(ts.URL + "/dex")
	const publicOrigin = "http://127.0.0.1:8085"

	proxy := NewOIDCLoopbackProxy(upstream, publicOrigin, nil)

	req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8085/dex/.well-known/openid-configuration", nil)
	rec := httptest.NewRecorder()
	proxy.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("discovery status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var doc map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatalf("decode discovery: %v", err)
	}

	// issuer identity must be preserved (matches token iss + browser authority).
	if got, want := doc["issuer"], ts.URL+"/dex"; got != want {
		t.Errorf("issuer = %v, want %v (must NOT be rewritten)", got, want)
	}
	// endpoints must be rewritten to the public HTTP origin, path preserved.
	for _, tc := range []struct{ key, want string }{
		{"authorization_endpoint", publicOrigin + "/dex/auth"},
		{"token_endpoint", publicOrigin + "/dex/token"},
		{"jwks_uri", publicOrigin + "/dex/keys"},
		{"userinfo_endpoint", publicOrigin + "/dex/userinfo"},
	} {
		if got := doc[tc.key]; got != tc.want {
			t.Errorf("%s = %v, want %v", tc.key, got, tc.want)
		}
	}
	// non-URL fields left intact.
	if _, ok := doc["scopes_supported"]; !ok {
		t.Errorf("scopes_supported dropped from discovery doc")
	}
}

func TestOIDCLoopbackProxyPassthroughAndLocationRewrite(t *testing.T) {
	ts := newDexStub(t)
	upstream, _ := url.Parse(ts.URL + "/dex")
	const publicOrigin = "http://127.0.0.1:8085"
	proxy := NewOIDCLoopbackProxy(upstream, publicOrigin, nil)

	t.Run("body passthrough", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8085/dex/keys", nil)
		rec := httptest.NewRecorder()
		proxy.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("keys status = %d", rec.Code)
		}
		if body := rec.Body.String(); body != `{"keys":[]}` {
			t.Errorf("keys body = %q, want verbatim upstream", body)
		}
	})

	t.Run("location + secure cookie", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8085/dex/auth", nil)
		rec := httptest.NewRecorder()
		proxy.ServeHTTP(rec, req)
		if rec.Code != http.StatusFound {
			t.Fatalf("auth status = %d, want 302", rec.Code)
		}
		if got, want := rec.Header().Get("Location"), publicOrigin+"/dex/approval?req=abc"; got != want {
			t.Errorf("Location = %q, want %q", got, want)
		}
		cookie := rec.Header().Get("Set-Cookie")
		if cookie == "" {
			t.Fatalf("Set-Cookie dropped")
		}
		// Secure must be stripped (served over http on loopback).
		for _, part := range splitCookie(cookie) {
			if part == "Secure" {
				t.Errorf("Set-Cookie still marked Secure: %q", cookie)
			}
		}
	})
}

// splitCookie splits a Set-Cookie header into trimmed attributes for assertion.
func splitCookie(c string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(c); i++ {
		if i == len(c) || c[i] == ';' {
			part := c[start:i]
			for len(part) > 0 && part[0] == ' ' {
				part = part[1:]
			}
			out = append(out, part)
			start = i + 1
		}
	}
	return out
}
