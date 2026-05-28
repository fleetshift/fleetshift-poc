package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

	for _, path := range []string{"/", "/clusters", "/clusters/some-id", "/setup", "/debug"} {
		req := httptest.NewRequest("GET", path, nil)
		req.Header.Set("Accept", "text/html")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("expected 200 for known route %s, got %d", path, rec.Code)
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
