package aioproxy_test

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/deploy/aio/internal/aioproxy"
)

const (
	publicOrigin  = "https://fleetshift-sandbox.localhost:8085"
	canonicalHost = "fleetshift-sandbox.localhost:8085"
)

func TestProxy_New_PublicOrigin(t *testing.T) {
	dex := mustURL(t, "http://127.0.0.1:1")
	app := mustURL(t, "http://127.0.0.1:1")
	p, err := aioproxy.New(aioproxy.Config{
		PublicOrigin: publicOrigin + "/",
		DexURL:       dex,
		AppURL:       app,
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "https://"+canonicalHost+"/", nil)
	req.Host = canonicalHost
	rr := httptest.NewRecorder()
	p.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("derived host rejected canonical request: status=%d", rr.Code)
	}
}

func TestProxy_New_RejectsBadConfig(t *testing.T) {
	dex := mustURL(t, "http://127.0.0.1:1")
	app := mustURL(t, "http://127.0.0.1:1")
	tests := []struct {
		name string
		cfg  aioproxy.Config
		want string
	}{
		{
			name: "missing origin",
			cfg:  aioproxy.Config{DexURL: dex, AppURL: app},
			want: "public origin is required",
		},
		{
			name: "origin path",
			cfg: aioproxy.Config{
				PublicOrigin: publicOrigin + "/dex",
				DexURL:       dex,
				AppURL:       app,
			},
			want: "path must be empty",
		},
		{
			name: "origin userinfo",
			cfg: aioproxy.Config{
				PublicOrigin: "https://u:p@" + canonicalHost,
				DexURL:       dex,
				AppURL:       app,
			},
			want: "userinfo",
		},
		{
			name: "origin scheme",
			cfg: aioproxy.Config{
				PublicOrigin: "ftp://" + canonicalHost,
				DexURL:       dex,
				AppURL:       app,
			},
			want: "scheme must be http or https",
		},
		{
			name: "nil dex",
			cfg:  aioproxy.Config{PublicOrigin: publicOrigin, AppURL: app},
			want: "dex upstream is required",
		},
		{
			name: "nil app",
			cfg:  aioproxy.Config{PublicOrigin: publicOrigin, DexURL: dex},
			want: "app upstream is required",
		},
		{
			name: "dex userinfo",
			cfg: aioproxy.Config{
				PublicOrigin: publicOrigin,
				DexURL:       mustURL(t, "http://u:p@127.0.0.1:1"),
				AppURL:       app,
			},
			want: "userinfo",
		},
		{
			name: "dex scheme",
			cfg: aioproxy.Config{
				PublicOrigin: publicOrigin,
				DexURL:       mustURL(t, "ftp://127.0.0.1:1"),
				AppURL:       app,
			},
			want: "scheme must be http or https",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := aioproxy.New(tt.cfg)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("New() = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestProxy_RoutesDexAndApp(t *testing.T) {
	dex, app := recordingUpstreams(t)
	p := newTestProxy(t, dex.URL, app.URL)

	tests := []struct {
		path string
		want string
	}{
		{path: "/dex", want: "dex"},
		{path: "/dex/", want: "dex"},
		{path: "/dex/.well-known/openid-configuration", want: "dex"},
		{path: "/dex/auth", want: "dex"},
		{path: "/dexevil", want: "app"},
		{path: "/", want: "app"},
		{path: "/api/ui/config", want: "app"},
		{path: "/auth/callback", want: "app"},
		{path: "/assets/app.js", want: "app"},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			rr := doProxy(t, p, http.MethodGet, tt.path, nil, nil)
			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d, body %s", rr.Code, rr.Body.Bytes())
			}
			if got := rr.Header().Get("X-Upstream"); got != tt.want {
				t.Fatalf("X-Upstream = %q, want %q", got, tt.want)
			}
			if rr.Header().Get("X-Seen-Path") != tt.path {
				t.Fatalf("upstream path = %q, want %q", rr.Header().Get("X-Seen-Path"), tt.path)
			}
		})
	}
}

func TestProxy_PreservesQueryPOSTCookiesRedirects(t *testing.T) {
	dex := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("X-Query", r.URL.RawQuery)
		w.Header().Set("X-Body", string(body))
		w.Header().Set("Set-Cookie", "dex-session=abc; Path=/dex; HttpOnly; Secure; SameSite=Lax")
		w.Header().Set("Location", publicOrigin+"/dex/approval")
		w.WriteHeader(http.StatusFound)
	}))
	t.Cleanup(dex.Close)
	app := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("app should not be called")
	}))
	t.Cleanup(app.Close)
	p := newTestProxy(t, dex.URL, app.URL)

	rr := doProxy(t, p, http.MethodPost, "/dex/token", strings.NewReader("a=1&b=2"), map[string]string{
		"Content-Type": "application/x-www-form-urlencoded",
	}, withQuery("code=x&state=y"))
	if rr.Code != http.StatusFound {
		t.Fatalf("status = %d", rr.Code)
	}
	if rr.Header().Get("X-Query") != "code=x&state=y" {
		t.Fatalf("query = %q", rr.Header().Get("X-Query"))
	}
	if rr.Header().Get("X-Body") != "a=1&b=2" {
		t.Fatalf("body = %q", rr.Header().Get("X-Body"))
	}
	if !strings.Contains(rr.Header().Get("Set-Cookie"), "dex-session=abc") {
		t.Fatalf("Set-Cookie = %q", rr.Header().Get("Set-Cookie"))
	}
	if rr.Header().Get("Location") != publicOrigin+"/dex/approval" {
		t.Fatalf("Location = %q", rr.Header().Get("Location"))
	}
}

func TestProxy_PreservesEncodedPath(t *testing.T) {
	dex := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Escaped", r.URL.EscapedPath())
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dex.Close)
	app := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("app should not be called")
	}))
	t.Cleanup(app.Close)
	p := newTestProxy(t, dex.URL, app.URL)

	req := httptest.NewRequest(http.MethodGet, "https://"+canonicalHost+"/dex/foo%2Fbar", nil)
	req.Host = canonicalHost
	rr := httptest.NewRecorder()
	p.ServeHTTP(rr, req)
	if rr.Header().Get("X-Escaped") != "/dex/foo%2Fbar" {
		t.Fatalf("escaped path = %q", rr.Header().Get("X-Escaped"))
	}
}

func TestProxy_EncodedDexSlashStaysOnApp(t *testing.T) {
	dex, app := recordingUpstreams(t)
	p := newTestProxy(t, dex.URL, app.URL)

	req := httptest.NewRequest(http.MethodGet, "https://"+canonicalHost+"/dex%2Fauth", nil)
	req.Host = canonicalHost
	rr := httptest.NewRecorder()
	p.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rr.Code, rr.Body.Bytes())
	}
	if got := rr.Header().Get("X-Upstream"); got != "app" {
		t.Fatalf("X-Upstream = %q, want app", got)
	}
}

func TestProxy_DropsSpoofedForwardingHeaders(t *testing.T) {
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, h := range aioproxy.ForwardingHeaders {
			if v := r.Header.Get(h); v != "" {
				t.Errorf("upstream saw %s=%q", h, v)
			}
		}
		if r.Host != canonicalHost {
			t.Errorf("Host = %q, want public host", r.Host)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(app.Close)
	dex := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("dex should not be called")
	}))
	t.Cleanup(dex.Close)
	p := newTestProxy(t, dex.URL, app.URL)

	headers := make(map[string]string, len(aioproxy.ForwardingHeaders))
	for _, h := range aioproxy.ForwardingHeaders {
		headers[h] = "spoofed"
	}
	rr := doProxy(t, p, http.MethodGet, "/api/ui/config", nil, headers)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
}

func TestProxy_WrongHost421(t *testing.T) {
	p := newTestProxy(t, "http://127.0.0.1:1", "http://127.0.0.1:1")
	req := httptest.NewRequest(http.MethodGet, "https://127.0.0.1:8085/", nil)
	req.Host = "127.0.0.1:8085"
	rr := httptest.NewRecorder()
	p.ServeHTTP(rr, req)
	if rr.Code != http.StatusMisdirectedRequest {
		t.Fatalf("status = %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), publicOrigin) {
		t.Fatalf("body = %q, want public origin", rr.Body.String())
	}
	if rr.Header().Get("Strict-Transport-Security") != "" {
		t.Fatal("421 must not emit HSTS")
	}
}

func TestProxy_StripsHSTS(t *testing.T) {
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(app.Close)
	dex := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(dex.Close)
	p := newTestProxy(t, dex.URL, app.URL)
	rr := doProxy(t, p, http.MethodGet, "/", nil, nil)
	if rr.Header().Get("Strict-Transport-Security") != "" {
		t.Fatalf("HSTS leaked: %q", rr.Header().Get("Strict-Transport-Security"))
	}
}

func TestProxy_UpstreamDown502(t *testing.T) {
	p := newTestProxy(t, "http://127.0.0.1:1", "http://127.0.0.1:1")
	rr := doProxy(t, p, http.MethodGet, "/", nil, nil)
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rr.Code)
	}
	if rr.Header().Get("Strict-Transport-Security") != "" {
		t.Fatal("502 must not emit HSTS")
	}
}

func TestProxy_StreamsResponse(t *testing.T) {
	firstSent := make(chan struct{})
	releaseSecond := make(chan struct{})
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fl, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "no flush", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		if _, err := io.WriteString(w, "chunk1"); err != nil {
			return
		}
		fl.Flush()
		close(firstSent)
		select {
		case <-releaseSecond:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, "chunk2")
	}))
	t.Cleanup(app.Close)
	dex := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(dex.Close)
	p := newTestProxy(t, dex.URL, app.URL)

	edge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Host = canonicalHost
		p.ServeHTTP(w, r)
	}))
	t.Cleanup(edge.Close)

	req, err := http.NewRequest(http.MethodGet, edge.URL+"/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = canonicalHost
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	first := make([]byte, 6)
	readFirst := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(resp.Body, first)
		readFirst <- err
	}()
	select {
	case err := <-readFirst:
		if err != nil {
			t.Fatalf("read first chunk: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive first chunk; response is not streaming")
	}
	if string(first) != "chunk1" {
		t.Fatalf("first chunk = %q", first)
	}
	select {
	case <-firstSent:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream did not flush first chunk")
	}
	close(releaseSecond)
	rest, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(rest) != "chunk2" {
		t.Fatalf("second chunk = %q", rest)
	}
}

func TestProxy_WebSocketOrigin(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Fatal("hijack unsupported")
		}
		conn, bufrw, err := hj.Hijack()
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_, _ = bufrw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
		_ = bufrw.Flush()
		buf := make([]byte, 4)
		n, _ := conn.Read(buf)
		_, _ = conn.Write(buf[:n])
	}))
	t.Cleanup(upstream.Close)
	p := newTestProxy(t, upstream.URL, upstream.URL)

	t.Run("rejects hostile origin", func(t *testing.T) {
		rr := doProxy(t, p, http.MethodGet, "/api/ui/events/ws", nil, map[string]string{
			"Connection": "Upgrade",
			"Upgrade":    "websocket",
			"Origin":     "https://evil.example",
		})
		if rr.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want 403", rr.Code)
		}
	})

	t.Run("allows canonical origin", func(t *testing.T) {
		status, err := websocketRoundTrip(t, p, publicOrigin)
		if err != nil {
			t.Fatal(err)
		}
		if status != http.StatusSwitchingProtocols {
			t.Fatalf("status = %d", status)
		}
	})

	t.Run("allows missing origin", func(t *testing.T) {
		status, err := websocketRoundTrip(t, p, "")
		if err != nil {
			t.Fatal(err)
		}
		if status != http.StatusSwitchingProtocols {
			t.Fatalf("status = %d", status)
		}
	})
}

func TestProxy_IgnoresHTTPProxyEnv(t *testing.T) {
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("http_proxy", "http://127.0.0.1:1")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(app.Close)
	dex := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(dex.Close)
	p := newTestProxy(t, dex.URL, app.URL)
	rr := doProxy(t, p, http.MethodGet, "/", nil, nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("HTTP_PROXY redirected loopback upstream: status=%d", rr.Code)
	}
}

func TestProxy_ListenAndServeTLS(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := writeGatewayCert(t, dir)
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	t.Cleanup(app.Close)
	dex := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(dex.Close)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	p, err := aioproxy.New(aioproxy.Config{
		ListenAddr:   addr,
		CertFile:     certFile,
		KeyFile:      keyFile,
		PublicOrigin: publicOrigin,
		DexURL:       mustURL(t, dex.URL),
		AppURL:       mustURL(t, app.URL),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- p.ListenAndServe(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-errc
	})

	caPool := x509.NewCertPool()
	pemBytes, err := os.ReadFile(certFile)
	if err != nil {
		t.Fatal(err)
	}
	if !caPool.AppendCertsFromPEM(pemBytes) {
		t.Fatal("append leaf as test trust root")
	}
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    caPool,
				ServerName: "fleetshift-sandbox.localhost",
			},
		},
	}
	var resp *http.Response
	deadline := time.Now().Add(2 * time.Second)
	for {
		req, err := http.NewRequest(http.MethodGet, "https://"+addr+"/", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Host = canonicalHost
		resp, err = client.Do(req)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("https request: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if resp.Header.Get("Strict-Transport-Security") != "" {
		t.Fatal("TLS response emitted HSTS")
	}
}

func recordingUpstreams(t *testing.T) (dex, app *httptest.Server) {
	t.Helper()
	handler := func(name string) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Upstream", name)
			w.Header().Set("X-Seen-Path", r.URL.Path)
			w.WriteHeader(http.StatusOK)
		})
	}
	dex = httptest.NewServer(handler("dex"))
	app = httptest.NewServer(handler("app"))
	t.Cleanup(dex.Close)
	t.Cleanup(app.Close)
	return dex, app
}

func newTestProxy(t *testing.T, dexURL, appURL string) *aioproxy.Proxy {
	t.Helper()
	p, err := aioproxy.New(aioproxy.Config{
		PublicOrigin: publicOrigin,
		DexURL:       mustURL(t, dexURL),
		AppURL:       mustURL(t, appURL),
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

type reqOption func(*http.Request)

func withQuery(q string) reqOption {
	return func(r *http.Request) {
		r.URL.RawQuery = q
	}
}

func doProxy(t *testing.T, p *aioproxy.Proxy, method, path string, body io.Reader, headers map[string]string, opts ...reqOption) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, "https://"+canonicalHost+path, body)
	req.Host = canonicalHost
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	for _, opt := range opts {
		opt(req)
	}
	rr := httptest.NewRecorder()
	p.ServeHTTP(rr, req)
	return rr
}

func websocketRoundTrip(t *testing.T, p http.Handler, origin string) (int, error) {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	t.Cleanup(func() { _ = server.Close() })

	go func() {
		rw := &pipeResponseWriter{conn: server, header: make(http.Header)}
		req := httptest.NewRequest(http.MethodGet, "https://"+canonicalHost+"/api/ui/events/ws", nil)
		req.Host = canonicalHost
		req.Header.Set("Connection", "Upgrade")
		req.Header.Set("Upgrade", "websocket")
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		p.ServeHTTP(rw, req)
	}()

	br := bufio.NewReader(client)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}

type pipeResponseWriter struct {
	conn        net.Conn
	header      http.Header
	wroteHeader bool
	hijacked    bool
}

func (w *pipeResponseWriter) Header() http.Header { return w.header }

func (w *pipeResponseWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.conn.Write(p)
}

func (w *pipeResponseWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	fmt.Fprintf(w.conn, "HTTP/1.1 %d %s\r\n", status, http.StatusText(status))
	_ = w.header.Write(w.conn)
	_, _ = w.conn.Write([]byte("\r\n"))
}

func (w *pipeResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.hijacked = true
	w.wroteHeader = true
	return w.conn, bufio.NewReadWriter(bufio.NewReader(w.conn), bufio.NewWriter(w.conn)), nil
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func writeGatewayCert(t *testing.T, dir string) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "FleetShift AIO Gateway"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"fleetshift-sandbox.localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certFile = filepath.Join(dir, "server.crt")
	keyFile = filepath.Join(dir, "server.key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0400); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}
