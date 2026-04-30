package http

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

func NewStaticHandler(webDir string) http.Handler {
	absWebDir, err := filepath.Abs(webDir)
	if err != nil {
		absWebDir = webDir
	}

	fs := http.Dir(absWebDir)
	fileServer := http.FileServer(fs)

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

		// File doesn't exist — SPA fallback for document requests
		if acceptsHTML(r) {
			w.Header().Set("Cache-Control", "no-cache")
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
