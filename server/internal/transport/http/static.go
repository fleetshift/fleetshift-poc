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

type knownRoutes struct {
	mu       sync.RWMutex
	prefixes []string
	loadedAt time.Time
	webDir   string
	cacheTTL time.Duration
}

func newKnownRoutes(webDir string) *knownRoutes {
	return &knownRoutes{
		webDir:   webDir,
		cacheTTL: 30 * time.Second,
	}
}

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

var staticKnownPrefixes = []string{"setup", "debug"}

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
