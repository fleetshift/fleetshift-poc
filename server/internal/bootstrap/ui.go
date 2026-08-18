package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	transporthttp "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/http"
)

// DeriveUIOrigin returns the trusted uiOrigin for a configured HTTP listen
// address. Unspecified hosts map to 127.0.0.1. The result has no path, query,
// fragment, or trailing slash.
func DeriveUIOrigin(httpAddr string) (string, error) {
	host, port, err := splitHostPort(httpAddr)
	if err != nil {
		return "", fmt.Errorf("derive UI origin: %w", err)
	}
	if host == "" || host == "0.0.0.0" || host == "::" || host == "[::]" {
		host = "127.0.0.1"
	}
	host = strings.TrimPrefix(host, "[")
	host = strings.TrimSuffix(host, "]")
	return "http://" + net.JoinHostPort(host, port), nil
}

// splitHostPort parses an HTTP listen address into host and port. A leading
// colon form (":8080") yields an empty host.
func splitHostPort(addr string) (host, port string, err error) {
	if after, ok := strings.CutPrefix(addr, ":"); ok {
		port = after
		if _, err := strconv.Atoi(port); err != nil {
			return "", "", fmt.Errorf("invalid port in %q", addr)
		}
		return "", port, nil
	}
	return net.SplitHostPort(addr)
}

// authMethodLister is the read surface needed for /api/ui/config.
type authMethodLister interface {
	List(ctx context.Context) ([]domain.AuthMethod, error)
}

// uiAuthFunc builds the /api/ui/config AuthSnapshot callback from persisted
// AuthMethods. Zero methods → unconfigured; one OIDC method with issuer and
// authorization endpoint → configured; any other shape is unavailable.
func uiAuthFunc(methods authMethodLister) func(context.Context) (authority, authorizationEndpoint string, configured bool, err error) {
	return func(ctx context.Context) (string, string, bool, error) {
		list, err := methods.List(ctx)
		if err != nil {
			return "", "", false, err
		}
		if len(list) == 0 {
			return "", "", false, nil
		}
		if len(list) != 1 {
			return "", "", false, fmt.Errorf("expected at most one auth method for UI config, found %d", len(list))
		}
		m := list[0]
		if m.Type() != domain.AuthMethodTypeOIDC || m.OIDC() == nil {
			return "", "", false, fmt.Errorf("active auth method is not OIDC")
		}
		oidc := m.OIDC()
		issuer := string(oidc.IssuerURL)
		authz := string(oidc.AuthorizationEndpoint)
		if issuer == "" || authz == "" {
			return "", "", false, fmt.Errorf("OIDC auth method missing issuer or authorization endpoint")
		}
		return issuer, authz, true, nil
	}
}

// uiHTTPDeps are the Start-time inputs needed to mount UI HTTP routes.
type uiHTTPDeps struct {
	cfg           Config
	logger        *slog.Logger
	authMethods   *application.AuthMethodService
	verifier      domain.OIDCTokenVerifier
	store         domain.Store
	provenanceSvc *domain.ProvenanceService
	setupHub      *transporthttp.SetupHub
	eventHub      *transporthttp.EventHub
	// oidcHTTPClient carries the OIDC issuer's CA trust; reused as the internal
	// transport for the browser loopback OIDC proxy. Nil when no CA bundle.
	oidcHTTPClient *http.Client
}

// loopbackOIDCProxyUpstream reports whether the configured OIDC issuer is a
// self-signed loopback HTTPS issuer (i.e. the AIO's peer Dex) that the browser
// cannot reach without importing the sandbox CA — the case the loopback proxy
// exists to remove. It returns the issuer URL (path trimmed of trailing slash)
// to mount the proxy under. Non-loopback / external issuers, http issuers, and
// issuers without a CA bundle or path prefix are left untouched.
func loopbackOIDCProxyUpstream(cfg Config) (*url.URL, bool) {
	if len(cfg.OIDCCABundle) == 0 || cfg.OIDCIssuer == "" {
		return nil, false
	}
	u, err := url.Parse(cfg.OIDCIssuer)
	if err != nil {
		return nil, false
	}
	if strings.ToLower(u.Scheme) != "https" || !isLoopbackHost(u.Hostname()) {
		return nil, false
	}
	p := strings.TrimRight(u.Path, "/")
	if p == "" {
		return nil, false // need a path prefix to mount the proxy under
	}
	u.Path = p
	return u, true
}

// registerUIHTTP mounts /api/ui/* routes and optional SPA static assets on topMux.
// The SPA is served under /app; GET/HEAD / and /app redirect to /app/.
func registerUIHTTP(topMux *http.ServeMux, deps uiHTTPDeps) error {
	// HTTP auth middleware — mirrors the gRPC authn interceptor: if
	// auth methods are configured require a valid OIDC Bearer token,
	// otherwise allow anonymous (setup mode). Applied selectively to
	// endpoints that need protection; /api/ui/config,
	// /api/ui/setup/ws, and /api/ui/events/ws intentionally remain
	// unauthenticated (events/ws because the browser WebSocket API
	// cannot set Authorization headers — see TODO below).
	httpAuthn := &transporthttp.AuthnMiddleware{
		Methods:  deps.authMethods,
		Verifier: deps.verifier,
		Logger:   deps.logger.With("component", "authn-http"),
	}
	topMux.HandleFunc("GET /api/ui/setup/ws", deps.setupHub.HandleWS)
	// TODO(auth): Browser WebSocket API cannot set custom HTTP headers, so
	// wrapping this endpoint with httpAuthn.Wrap would always 401 once OIDC
	// is configured. Proper WS auth requires a short-lived OTP/ticket
	// handshake — passing the JWT as a query param leaks into logs,
	// referrer, and browser history. Leave unauthenticated for now.
	topMux.HandleFunc("GET /api/ui/events/ws", deps.eventHub.HandleWS)
	topMux.Handle("GET /api/ui/github-signing-keys/{username}", httpAuthn.Wrap(http.HandlerFunc(transporthttp.HandleGitHubSigningKeys)))
	topMux.Handle("POST /api/ui/verify-sign", &transporthttp.VerifySignHandler{
		AuthMethods: deps.authMethods, Verifier: deps.verifier, Store: deps.store, ProvenanceSvc: deps.provenanceSvc,
	})

	uiOrigin, err := DeriveUIOrigin(deps.cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("derive UI origin: %w", err)
	}

	// Loopback OIDC proxy: when the issuer is the self-signed loopback Dex,
	// serve its endpoints under our own HTTP origin so the browser never has to
	// trust the sandbox CA. Mounted at the issuer's own path prefix so Dex's
	// path-absolute links resolve unchanged.
	var oidcMetadataPath string
	if upstream, ok := loopbackOIDCProxyUpstream(deps.cfg); ok {
		var transport http.RoundTripper
		if deps.oidcHTTPClient != nil {
			transport = deps.oidcHTTPClient.Transport
		}
		proxy := transporthttp.NewOIDCLoopbackProxy(upstream, uiOrigin, transport)
		topMux.Handle(upstream.Path+"/", proxy)
		oidcMetadataPath = upstream.Path + "/.well-known/openid-configuration"
		deps.logger.Info("serving loopback OIDC proxy for browser",
			"prefix", upstream.Path, "issuer", deps.cfg.OIDCIssuer)
	}

	uiMux := transporthttp.NewUIConfigMux(transporthttp.UIConfigOptions{
		WebDir:           deps.cfg.WebDir,
		UIOrigin:         uiOrigin,
		OIDCUIClientID:   deps.cfg.OIDCUIClientID,
		OIDCUIScope:      deps.cfg.OIDCUIScope,
		OIDCMetadataPath: oidcMetadataPath,
		Logger:           deps.logger,
		AuthMiddleware:   httpAuthn.Wrap,
		AuthSnapshot:     uiAuthFunc(deps.authMethods),
	})
	topMux.Handle("/api/ui/", uiMux)
	if deps.cfg.WebDir != "" {
		transporthttp.MountUI(topMux, deps.cfg.WebDir)
		deps.logger.Info("serving frontend assets", "web-dir", deps.cfg.WebDir, "path", transporthttp.UIPathPrefix)
	}
	return nil
}
