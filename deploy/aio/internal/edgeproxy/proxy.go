// Package edgeproxy is the AIO-only TLS edge: one public origin, two fixed
// loopback upstreams (peer Dex and FleetShift).
package edgeproxy

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"
)

const (
	readHeaderTimeout = 10 * time.Second
	idleTimeout       = 120 * time.Second
	shutdownTimeout   = 5 * time.Second
	maxHeaderBytes    = 1 << 20
)

var forwardingHeaders = []string{
	"Forwarded",
	"X-Forwarded-For",
	"X-Forwarded-Host",
	"X-Forwarded-Proto",
	"X-Forwarded-Port",
	"X-Forwarded-Ssl",
	"X-Forwarded-Prefix",
	"X-Real-IP",
	"X-Forwarded-User",
	"X-Forwarded-Email",
	"X-Remote-User",
	"Remote-User",
	"X-Auth-Request-User",
	"X-Auth-Request-Email",
	"X-Forwarded-Client-Cert",
}

// Config is the sealed AIO proxy configuration. Upstream URLs must be fixed
// loopback http(s) targets; this process is not a general reverse proxy.
type Config struct {
	ListenAddr    string
	CertFile      string
	KeyFile       string
	PublicOrigin  string
	CanonicalHost string
	DexURL        *url.URL
	AppURL        *url.URL
}

// Proxy terminates the AIO gateway certificate and multiplexes /dex to Dex
// and every other path to FleetShift.
type Proxy struct {
	listenAddr    string
	certFile      string
	keyFile       string
	publicOrigin  string
	canonicalHost string
	dex           *httputil.ReverseProxy
	app           *httputil.ReverseProxy
	server        *http.Server
}

// New constructs a Proxy for cfg. DexURL and AppURL must be non-nil absolute
// http or https URLs without userinfo.
func New(cfg Config) (*Proxy, error) {
	if cfg.CanonicalHost == "" {
		return nil, fmt.Errorf("canonical host is required")
	}
	if cfg.PublicOrigin == "" {
		return nil, fmt.Errorf("public origin is required")
	}
	if err := checkUpstream("dex", cfg.DexURL); err != nil {
		return nil, err
	}
	if err := checkUpstream("app", cfg.AppURL); err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	p := &Proxy{
		listenAddr:    cfg.ListenAddr,
		certFile:      cfg.CertFile,
		keyFile:       cfg.KeyFile,
		publicOrigin:  cfg.PublicOrigin,
		canonicalHost: cfg.CanonicalHost,
		dex:           newUpstream(cfg.DexURL, transport),
		app:           newUpstream(cfg.AppURL, transport),
	}
	p.server = &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           p,
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
		TLSConfig:         &tls.Config{MinVersion: tls.VersionTLS12},
	}
	return p, nil
}

// ServeHTTP routes a request to Dex or FleetShift after Host and WebSocket
// Origin checks. Encoded paths are classified with EscapedPath so a cleaned
// path cannot cross the /dex routing boundary.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Host != p.canonicalHost {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMisdirectedRequest)
		fmt.Fprintf(w, "use %s\n", p.publicOrigin)
		return
	}
	if isWebSocket(r) {
		origin := r.Header.Get("Origin")
		if origin != "" && origin != p.publicOrigin {
			http.Error(w, "origin not allowed", http.StatusForbidden)
			return
		}
	}
	path := r.URL.EscapedPath()
	if path == "/dex" || strings.HasPrefix(path, "/dex/") {
		p.dex.ServeHTTP(w, r)
		return
	}
	p.app.ServeHTTP(w, r)
}

// ListenAndServe serves HTTPS on the configured address until ctx is cancelled.
func (p *Proxy) ListenAndServe(ctx context.Context) error {
	if p.listenAddr == "" {
		return fmt.Errorf("listen address is required")
	}
	ln, err := net.Listen("tcp", p.listenAddr)
	if err != nil {
		return err
	}
	errc := make(chan error, 1)
	go func() {
		err := p.server.ServeTLS(ln, p.certFile, p.keyFile)
		if err == http.ErrServerClosed {
			err = nil
		}
		errc <- err
	}()
	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		_ = p.server.Shutdown(shutCtx)
		return <-errc
	case err := <-errc:
		return err
	}
}

func checkUpstream(name string, u *url.URL) error {
	if u == nil {
		return fmt.Errorf("%s upstream is required", name)
	}
	if u.User != nil {
		return fmt.Errorf("%s upstream must not include userinfo", name)
	}
	switch u.Scheme {
	case "http", "https":
	default:
		return fmt.Errorf("%s upstream scheme must be http or https", name)
	}
	if u.Host == "" {
		return fmt.Errorf("%s upstream host is required", name)
	}
	return nil
}

func newUpstream(target *url.URL, transport http.RoundTripper) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			dropForwardingHeaders(pr.Out.Header)
			pr.SetURL(target)
			pr.Out.URL.RawQuery = pr.In.URL.RawQuery
			pr.Out.Host = pr.In.Host
		},
		Transport:     transport,
		FlushInterval: -1,
		ModifyResponse: func(resp *http.Response) error {
			resp.Header.Del("Strict-Transport-Security")
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			log.Printf("aio-proxy: upstream error method=%s path=%s: %v", r.Method, r.URL.Path, err)
			http.Error(w, "Bad Gateway", http.StatusBadGateway)
		},
	}
}

func dropForwardingHeaders(h http.Header) {
	for _, name := range forwardingHeaders {
		h.Del(name)
	}
}

func isWebSocket(r *http.Request) bool {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return false
	}
	for _, v := range r.Header.Values("Connection") {
		for _, part := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(part), "upgrade") {
				return true
			}
		}
	}
	return false
}
