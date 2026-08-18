package http

import (
	"encoding/json"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
)

// NewOIDCLoopbackProxy returns a handler that reverse-proxies an internal
// loopback OIDC issuer (the AIO's peer Dex, served over HTTPS with a
// self-signed sandbox CA) under the server's own HTTP origin, so browsers reach
// the issuer over plain HTTP and never need the sandbox CA in their trust store.
//
// It is mounted at the issuer's own path prefix (e.g. /dex), so Dex's
// path-absolute UI links (/dex/auth, /dex/theme, ...) resolve through the proxy
// unchanged and only scheme+host need rewriting: the discovery document's
// absolute endpoint URLs and any upstream Location headers are rewritten from
// the internal issuer origin to publicOrigin. The discovery `issuer` value is
// deliberately left untouched so the ID token `iss` claim, the browser
// `authority`, and server-side token validation all keep one stable identity
// (the internal HTTPS issuer, which the server — not the browser — validates
// against directly).
//
//   - upstream is the full internal issuer URL (scheme://host/path).
//   - publicOrigin is the browser-facing origin (scheme://host, no path).
//   - transport carries the CA trust for the internal HTTPS hop (nil →
//     http.DefaultTransport).
func NewOIDCLoopbackProxy(upstream *url.URL, publicOrigin string, transport http.RoundTripper) http.Handler {
	if transport == nil {
		transport = http.DefaultTransport
	}
	upstreamOrigin := upstream.Scheme + "://" + upstream.Host
	basePath := strings.TrimRight(upstream.Path, "/")
	discoveryPath := basePath + "/.well-known/openid-configuration"

	rp := &httputil.ReverseProxy{
		Transport: transport,
		Director: func(req *http.Request) {
			// The handler is mounted at the issuer path prefix, so the incoming
			// path already maps 1:1 onto the upstream — only scheme/host/Host
			// need pointing at the internal issuer.
			req.URL.Scheme = upstream.Scheme
			req.URL.Host = upstream.Host
			req.Host = upstream.Host
		},
		ModifyResponse: func(resp *http.Response) error {
			if rest, ok := strings.CutPrefix(resp.Header.Get("Location"), upstreamOrigin); ok {
				resp.Header.Set("Location", publicOrigin+rest)
			}
			// Served over plain HTTP on loopback: a Secure cookie would never be
			// sent back by the browser, breaking any cookie-based step.
			if cookies := resp.Header.Values("Set-Cookie"); len(cookies) > 0 {
				stripped := make([]string, len(cookies))
				for i, c := range cookies {
					stripped[i] = stripSecureCookieAttr(c)
				}
				resp.Header.Del("Set-Cookie")
				for _, c := range stripped {
					resp.Header.Add("Set-Cookie", c)
				}
			}
			return nil
		},
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The discovery document carries absolute endpoint URLs in its body, so
		// it needs a JSON rewrite the streaming reverse proxy can't do; every
		// other endpoint only needs header/scheme rewriting.
		if r.URL.Path == discoveryPath {
			serveRewrittenDiscovery(w, r, upstreamOrigin+discoveryPath, upstreamOrigin, publicOrigin, transport)
			return
		}
		rp.ServeHTTP(w, r)
	})
}

// serveRewrittenDiscovery fetches the upstream OIDC discovery document and
// rewrites every absolute endpoint URL from upstreamOrigin to publicOrigin,
// leaving `issuer` (the token/authority identity) untouched.
func serveRewrittenDiscovery(w http.ResponseWriter, r *http.Request, discoveryURL, upstreamOrigin, publicOrigin string, transport http.RoundTripper) {
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, discoveryURL, nil)
	if err != nil {
		http.Error(w, "oidc discovery unavailable", http.StatusBadGateway)
		return
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "identity") // keep the body decoded for rewriting
	resp, err := transport.RoundTrip(req)
	if err != nil {
		http.Error(w, "oidc discovery unavailable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		http.Error(w, "oidc discovery unavailable", http.StatusBadGateway)
		return
	}
	var doc map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		http.Error(w, "oidc discovery invalid", http.StatusBadGateway)
		return
	}
	for k, v := range doc {
		if k == "issuer" {
			continue // keep issuer identity stable (matches token iss + authority)
		}
		s, ok := v.(string)
		if !ok {
			continue
		}
		if rest, ok := strings.CutPrefix(s, upstreamOrigin); ok {
			doc[k] = publicOrigin + rest
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(doc)
}

// stripSecureCookieAttr removes a bare `Secure` attribute from a Set-Cookie
// value, preserving all other attributes and their order.
func stripSecureCookieAttr(cookie string) string {
	parts := strings.Split(cookie, ";")
	out := parts[:0]
	for _, p := range parts {
		if strings.EqualFold(strings.TrimSpace(p), "Secure") {
			continue
		}
		out = append(out, p)
	}
	return strings.Join(out, ";")
}
