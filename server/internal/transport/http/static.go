package http

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// knownRoutes caches first-path segments that should receive SPA index.html
// with HTTP 200 (including plugin pages and staticKnownPrefixes).
type knownRoutes struct {
	mu       sync.RWMutex
	prefixes []string
	loadedAt time.Time
	webDir   string
	cacheTTL time.Duration
}

// newKnownRoutes builds a route cache rooted at webDir.
func newKnownRoutes(webDir string) *knownRoutes {
	return &knownRoutes{
		webDir:   webDir,
		cacheTTL: 30 * time.Second,
	}
}

// isKnown reports whether urlPath's first segment is a known SPA route.
func (kr *knownRoutes) isKnown(urlPath string) bool {
	kr.mu.RLock()
	stale := time.Since(kr.loadedAt) > kr.cacheTTL
	prefixes := kr.prefixes
	kr.mu.RUnlock()

	if stale || prefixes == nil {
		prefixes = kr.reload()
	}

	path := strings.TrimPrefix(urlPath, "/")
	seg := strings.SplitN(path, "/", 2)[0]

	for _, prefix := range prefixes {
		if seg == prefix {
			return true
		}
	}
	return false
}

// staticKnownPrefixes are first-path segments that receive SPA index.html with
// HTTP 200 (product shell routes; "auth" covers /auth/callback and other /auth/*).
var staticKnownPrefixes = []string{"setup", "debug", "auth"}

// reload refreshes known prefixes: always includes staticKnownPrefixes, then
// adds first-path segments from plugin-registry.json page paths (via
// generatePluginPages, including builtin pages). Falls back to
// staticKnownPrefixes alone when the registry is missing or invalid.
func (kr *knownRoutes) reload() []string {
	data, err := os.ReadFile(filepath.Join(kr.webDir, "plugin-registry.json"))
	if err != nil {
		kr.mu.Lock()
		kr.prefixes = staticKnownPrefixes
		kr.loadedAt = time.Now()
		kr.mu.Unlock()
		return kr.prefixes
	}

	var registry pluginRegistry
	if err := json.Unmarshal(data, &registry); err != nil {
		kr.mu.Lock()
		kr.prefixes = staticKnownPrefixes
		kr.loadedAt = time.Now()
		kr.mu.Unlock()
		return kr.prefixes
	}

	pages := generatePluginPages(registry)
	seen := make(map[string]bool)
	for _, p := range staticKnownPrefixes {
		seen[p] = true
	}

	prefixes := append([]string{}, staticKnownPrefixes...)
	for _, page := range pages {
		seg := strings.SplitN(page.Path, "/", 2)[0]
		if !seen[seg] {
			seen[seg] = true
			prefixes = append(prefixes, seg)
		}
	}

	kr.mu.Lock()
	kr.prefixes = prefixes
	kr.loadedAt = time.Now()
	kr.mu.Unlock()
	return prefixes
}

// UIPathPrefix is the public URL prefix for the SPA and its static assets.
const UIPathPrefix = "/app"

// PrefixUIAssetPath rewrites an origin-rooted asset path so it is served under
// UIPathPrefix. Relative paths and paths already under the prefix are unchanged
// except empty and "/" which become "/app/".
func PrefixUIAssetPath(p string) string {
	if p == "" || p == "/" {
		return UIPathPrefix + "/"
	}
	if p == UIPathPrefix || strings.HasPrefix(p, UIPathPrefix+"/") {
		return p
	}
	if strings.HasPrefix(p, "/") {
		return UIPathPrefix + p
	}
	return p
}

// redirectToUI issues a 302 to /app/.
func redirectToUI(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, UIPathPrefix+"/", http.StatusFound)
}

// MountUI registers GET/HEAD redirects from / and /app to /app/, and serves
// webDir under /app/.
func MountUI(mux *http.ServeMux, webDir string) {
	mux.HandleFunc("GET /{$}", redirectToUI)
	mux.HandleFunc("HEAD /{$}", redirectToUI)
	mux.HandleFunc("GET "+UIPathPrefix, redirectToUI)
	mux.HandleFunc("HEAD "+UIPathPrefix, redirectToUI)
	mux.Handle(UIPathPrefix+"/", http.StripPrefix(UIPathPrefix, NewStaticHandler(webDir)))
}

// NewStaticHandler serves frontend assets from webDir with SPA fallback for
// HTML document requests. Unknown client routes return index.html with 404.
// Paths are the stripped SPA paths (e.g. /setup), not the public /app prefix.
func NewStaticHandler(webDir string) http.Handler {
	absWebDir, err := filepath.Abs(webDir)
	if err != nil {
		absWebDir = webDir
	}

	fs := http.Dir(absWebDir)
	fileServer := http.FileServer(fs)
	routes := newKnownRoutes(absWebDir)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		urlPath := filepath.Clean(r.URL.Path)
		if urlPath == "." {
			urlPath = "/"
		}

		filePath := filepath.Join(absWebDir, urlPath)
		if !strings.HasPrefix(filePath, absWebDir) {
			http.NotFound(w, r)
			return
		}

		info, err := os.Stat(filePath)
		if err == nil && !info.IsDir() {
			setCacheHeaders(w, urlPath)
			fileServer.ServeHTTP(w, r)
			return
		}

		// SPA fallback for document requests
		if acceptsHTML(r) {
			w.Header().Set("Cache-Control", "no-cache")
			if urlPath != "/" && !routes.isKnown(urlPath) {
				w.WriteHeader(http.StatusNotFound)
			}
			http.ServeFile(w, r, filepath.Join(absWebDir, "index.html"))
			return
		}

		http.NotFound(w, r)
	})
}

// setCacheHeaders applies Cache-Control: no-cache for index.html and manifests,
// and a long immutable cache for all other served files.
func setCacheHeaders(w http.ResponseWriter, path string) {
	base := filepath.Base(path)

	switch {
	case base == "index.html":
		w.Header().Set("Cache-Control", "no-cache")
	case strings.HasSuffix(base, "-manifest.json"):
		w.Header().Set("Cache-Control", "no-cache")
	case base == "plugin-registry.json":
		w.Header().Set("Cache-Control", "no-cache")
	default:
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	}
}

// acceptsHTML reports whether the Accept header includes text/html or */*
// (q-values and preference order are not considered).
func acceptsHTML(r *http.Request) bool {
	accept := r.Header.Get("Accept")
	for _, part := range strings.Split(accept, ",") {
		mediaType := strings.TrimSpace(strings.SplitN(part, ";", 2)[0])
		if mediaType == "text/html" || mediaType == "*/*" {
			return true
		}
	}
	return false
}
