// Package aioproxy terminates the AIO gateway certificate and reverse-proxies
// peer Dex and FleetShift on one public origin.
package aioproxy

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
	idpPath           = "/idp"
	idpPathSlash      = "/idp/"
)

// ForwardingHeaders are extra hop and identity headers a client can spoof
// (address, proto, or authenticated user). ReverseProxy.Rewrite already
// drops Forwarded and X-Forwarded-For/Host/Proto; these names are not in
// that set and must not reach Dex or FleetShift.
var ForwardingHeaders = []string{
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

// Config is the AIO TLS-edge configuration. DexURL and FleetShiftURL are the
// Dex and FleetShift loopback upstreams. The accepted Host header is derived
// from PublicOrigin.
type Config struct {
	ListenAddr    string
	CertFile      string
	KeyFile       string
	PublicOrigin  string
	DexURL        *url.URL
	FleetShiftURL *url.URL
}

// Proxy terminates the AIO gateway certificate and multiplexes /idp to Dex
// and every other path to FleetShift.
type Proxy struct {
	certFile      string
	keyFile       string
	publicOrigin  string
	canonicalHost string
	dex           *httputil.ReverseProxy
	fleetshift    *httputil.ReverseProxy
	server        *http.Server
}

// New constructs a Proxy for cfg. DexURL and FleetShiftURL must be non-nil
// absolute http or https URLs without userinfo. PublicOrigin must be an
// absolute http or https origin; its host is the only accepted Host header.
func New(cfg Config) (*Proxy, error) {
	origin, host, err := parsePublicOrigin(cfg.PublicOrigin)
	if err != nil {
		return nil, err
	}
	if err := checkUpstream("dex", cfg.DexURL); err != nil {
		return nil, err
	}
	if err := checkUpstream("fleetshift", cfg.FleetShiftURL); err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	p := &Proxy{
		certFile:      cfg.CertFile,
		keyFile:       cfg.KeyFile,
		publicOrigin:  origin,
		canonicalHost: host,
		dex:           newUpstream(cfg.DexURL, transport),
		fleetshift:    newUpstream(cfg.FleetShiftURL, transport),
	}
	p.server = &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           p,
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
		TLSConfig:         &tls.Config{MinVersion: tls.VersionTLS12},
	}
	return p, nil
}

// ServeHTTP routes a request to Dex or FleetShift after Host and WebSocket
// Origin checks. Encoded paths are classified with EscapedPath so a cleaned
// path cannot cross the /idp routing boundary.
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
	if path == idpPath || strings.HasPrefix(path, idpPathSlash) {
		p.dex.ServeHTTP(w, r)
		return
	}
	p.fleetshift.ServeHTTP(w, r)
}

// ListenAndServe serves HTTPS on the configured address until ctx is cancelled.
func (p *Proxy) ListenAndServe(ctx context.Context) error {
	if p.server.Addr == "" {
		return fmt.Errorf("listen address is required")
	}
	ln, err := net.Listen("tcp", p.server.Addr)
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

// parsePublicOrigin validates that raw is an absolute http or https origin
// with no userinfo, query, fragment, or path other than "/". It returns the
// canonical origin (no trailing slash) and the URL host (name + port).
func parsePublicOrigin(raw string) (origin, host string, err error) {
	if raw == "" {
		return "", "", fmt.Errorf("public origin is required")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", fmt.Errorf("public origin: %w", err)
	}
	switch u.Scheme {
	case "http", "https":
	default:
		return "", "", fmt.Errorf("public origin scheme must be http or https")
	}
	if u.User != nil {
		return "", "", fmt.Errorf("public origin must not include userinfo")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "", "", fmt.Errorf("public origin must not include query or fragment")
	}
	if u.Host == "" {
		return "", "", fmt.Errorf("public origin host is required")
	}
	if u.Path != "" && u.Path != "/" {
		return "", "", fmt.Errorf("public origin path must be empty or /")
	}
	return u.Scheme + "://" + u.Host, u.Host, nil
}

// checkUpstream returns an error unless u is a non-nil http(s) URL with a
// host and no userinfo.
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

// newUpstream returns a reverse proxy that sends requests to target, restores
// the incoming query and Host, drops client forwarding and identity headers,
// and strips HSTS from responses.
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

// dropForwardingHeaders removes client-supplied forwarding and identity headers from h.
func dropForwardingHeaders(h http.Header) {
	for _, name := range ForwardingHeaders {
		h.Del(name)
	}
}

// isWebSocket reports whether r is a WebSocket upgrade request.
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
