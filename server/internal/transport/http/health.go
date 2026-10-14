package http

import (
	"net/http"
	"sync/atomic"
)

// Readiness is a process-local ready signal for /readyz.
// /livez succeeds while the process can answer; /readyz succeeds after MarkReady.
type Readiness struct {
	ready atomic.Bool
}

// MarkReady sets /readyz to success. Safe for concurrent use.
func (r *Readiness) MarkReady() {
	if r == nil {
		return
	}
	r.ready.Store(true)
}

// ClearReady revokes /readyz success (e.g. on shutdown). Safe for concurrent use.
func (r *Readiness) ClearReady() {
	if r == nil {
		return
	}
	r.ready.Store(false)
}

// Ready reports whether /readyz should succeed.
func (r *Readiness) Ready() bool {
	if r == nil {
		return false
	}
	return r.ready.Load()
}

// RegisterHealthRoutes mounts unauthenticated GET/HEAD /livez and /readyz on mux.
// Other methods return 405 with Allow: GET, HEAD. Bodies are plain text "ok".
func RegisterHealthRoutes(mux *http.ServeMux, readiness *Readiness) {
	if readiness == nil {
		readiness = &Readiness{}
	}
	mux.Handle("/livez", livezHandler())
	mux.Handle("/readyz", readyzHandler(readiness))
}

func livezHandler() http.Handler {
	return healthProbe(func() bool { return true })
}

func readyzHandler(readiness *Readiness) http.Handler {
	return healthProbe(readiness.Ready)
}

// healthProbe serves GET/HEAD probe responses. ok reports whether the probe
// should succeed; bodies are plain text "ok".
func healthProbe(ok func() bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		switch r.Method {
		case http.MethodGet, http.MethodHead:
		default:
			w.Header().Set("Allow", "GET, HEAD")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if !ok() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte("ok"))
		}
	})
}
