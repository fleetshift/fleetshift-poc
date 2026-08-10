package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestExtractBearer(t *testing.T) {
	tests := []struct {
		header string
		want   string
	}{
		{"Bearer abc123", "abc123"},
		{"Bearer ", ""},
		{"Basic abc", ""},
		{"", ""},
	}
	for _, tt := range tests {
		r := httptest.NewRequest("GET", "/", nil)
		if tt.header != "" {
			r.Header.Set("Authorization", tt.header)
		}
		got := extractBearer(r)
		if got != tt.want {
			t.Errorf("extractBearer(%q) = %q, want %q", tt.header, got, tt.want)
		}
	}
}

func TestConstantTimeEqual(t *testing.T) {
	if !constantTimeEqual("abc", "abc") {
		t.Error("equal strings should match")
	}
	if constantTimeEqual("abc", "def") {
		t.Error("different strings should not match")
	}
}

func TestAuthorize(t *testing.T) {
	p := &cacheProxy{
		readToken:  "read-tok",
		writeToken: "write-tok",
	}

	tests := []struct {
		name         string
		token        string
		requireWrite bool
		want         bool
	}{
		{"no token", "", false, false},
		{"read token for read", "read-tok", false, true},
		{"write token for read", "write-tok", false, true},
		{"read token for write", "read-tok", true, false},
		{"write token for write", "write-tok", true, true},
		{"wrong token", "bad-tok", false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/", nil)
			if tt.token != "" {
				r.Header.Set("Authorization", "Bearer "+tt.token)
			}
			got := p.authorize(r, tt.requireWrite)
			if got != tt.want {
				t.Errorf("authorize(token=%q, write=%v) = %v, want %v",
					tt.token, tt.requireWrite, got, tt.want)
			}
		})
	}
}

func TestHandleGetNoAuth(t *testing.T) {
	p := &cacheProxy{readToken: "tok", writeToken: "wtok"}
	r := httptest.NewRequest("GET", "/v1/cache/abc123", nil)
	r.SetPathValue("hash", "abc123")
	w := httptest.NewRecorder()

	p.handleGet(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", w.Code)
	}
}

func TestHandlePutReadOnlyToken(t *testing.T) {
	p := &cacheProxy{readToken: "read-tok", writeToken: "write-tok"}
	r := httptest.NewRequest("PUT", "/v1/cache/abc123", strings.NewReader("data"))
	r.SetPathValue("hash", "abc123")
	r.Header.Set("Authorization", "Bearer read-tok")
	r.Header.Set("Content-Length", "4")
	w := httptest.NewRecorder()

	p.handlePut(w, r)

	if w.Code != http.StatusForbidden {
		t.Errorf("expected 403, got %d", w.Code)
	}
}

func TestHandleGetMissingHash(t *testing.T) {
	p := &cacheProxy{readToken: "tok", writeToken: "wtok"}
	r := httptest.NewRequest("GET", "/v1/cache/", nil)
	r.SetPathValue("hash", "")
	r.Header.Set("Authorization", "Bearer tok")
	w := httptest.NewRecorder()

	p.handleGet(w, r)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestHealthz(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	r := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	_ = io.Discard
}
