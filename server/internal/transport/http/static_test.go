package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func setupTestWebDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	os.WriteFile(filepath.Join(dir, "index.html"), []byte("<html>app</html>"), 0644)
	os.WriteFile(filepath.Join(dir, "app.abc123.js"), []byte("console.log()"), 0644)

	registry := pluginRegistry{
		Plugins: map[string]pluginEntry{
			"core-plugin": {
				Name:  "core-plugin",
				Key:   "core",
				Label: "Clusters",
				PluginManifest: pluginManifest{
					Extensions: []extension{
						{
							Type: "fleetshift.module",
							Properties: map[string]interface{}{
								"label":     "Clusters",
								"component": map[string]interface{}{"$codeRef": "ClustersModule.default"},
							},
						},
					},
				},
			},
		},
	}
	data, _ := json.Marshal(registry)
	os.WriteFile(filepath.Join(dir, "plugin-registry.json"), data, 0644)

	return dir
}

func TestStaticHandler_ServesStaticFile(t *testing.T) {
	dir := setupTestWebDir(t)
	handler := NewStaticHandler(dir)

	req := httptest.NewRequest("GET", "/app.abc123.js", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 for static file, got %d", rec.Code)
	}
	if rec.Header().Get("Cache-Control") != "public, max-age=31536000, immutable" {
		t.Errorf("expected immutable cache for static file, got %q", rec.Header().Get("Cache-Control"))
	}
}

func TestStaticHandler_KnownRoute_Returns200(t *testing.T) {
	dir := setupTestWebDir(t)
	handler := NewStaticHandler(dir)

	for _, path := range []string{"/", "/clusters", "/clusters/some-id", "/setup", "/debug", "/auth/callback"} {
		req := httptest.NewRequest("GET", path, nil)
		req.Header.Set("Accept", "text/html")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("expected 200 for known route %s, got %d", path, rec.Code)
		}
		if path == "/auth/callback" && !strings.Contains(rec.Body.String(), "app") {
			t.Errorf("expected SPA body for %s", path)
		}
	}
}

func TestStaticHandler_UnknownRoute_Returns404(t *testing.T) {
	dir := setupTestWebDir(t)
	handler := NewStaticHandler(dir)

	for _, path := range []string{"/nonexistent", "/foo/bar", "/something-random"} {
		req := httptest.NewRequest("GET", path, nil)
		req.Header.Set("Accept", "text/html")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Errorf("expected 404 for unknown route %s, got %d", path, rec.Code)
		}
		if body := rec.Body.String(); body == "" {
			t.Errorf("expected index.html body for unknown route %s, got empty", path)
		}
	}
}

func TestStaticHandler_UnknownRoute_NoHTML_Returns404(t *testing.T) {
	dir := setupTestWebDir(t)
	handler := NewStaticHandler(dir)

	req := httptest.NewRequest("GET", "/nonexistent.json", nil)
	req.Header.Set("Accept", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404 for non-HTML request, got %d", rec.Code)
	}
}

func TestPrefixUIAssetPath(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{in: "", want: "/app/"},
		{in: "/", want: "/app/"},
		{in: "/app", want: "/app"},
		{in: "/app/", want: "/app/"},
		{in: "/app/plugins/core/plugin-manifest.json", want: "/app/plugins/core/plugin-manifest.json"},
		{in: "/plugins/core/plugin-manifest.json", want: "/app/plugins/core/plugin-manifest.json"},
		{in: "plugins/core/plugin.js", want: "plugins/core/plugin.js"},
	}
	for _, tt := range tests {
		if got := PrefixUIAssetPath(tt.in); got != tt.want {
			t.Errorf("PrefixUIAssetPath(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestMountUI_RedirectsAndServes(t *testing.T) {
	dir := setupTestWebDir(t)
	mux := http.NewServeMux()
	MountUI(mux, dir)

	for _, path := range []string{"/", "/app"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusFound {
			t.Errorf("%s status = %d, want %d", path, rec.Code, http.StatusFound)
		}
		if loc := rec.Header().Get("Location"); loc != "/app/" {
			t.Errorf("%s Location = %q, want /app/", path, loc)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/app/app.abc123.js", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("static file status = %d, want 200", rec.Code)
	}

	for _, path := range []string{"/app/", "/app/setup", "/app/auth/callback"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Accept", "text/html")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("%s status = %d, want 200", path, rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "app") {
			t.Errorf("%s body missing SPA html", path)
		}
	}

	req = httptest.NewRequest(http.MethodGet, "/setup", nil)
	req.Header.Set("Accept", "text/html")
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("unprefixed SPA path status = %d, want 404", rec.Code)
	}
}
