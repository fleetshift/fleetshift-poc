package http

import (
	"log/slog"
	"net/http"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// AuthnMiddleware enforces OIDC authentication on HTTP endpoints using the
// same rules as the gRPC [grpc.AuthnInterceptor]: if auth methods are
// configured, a valid Bearer token is required; otherwise requests pass
// through anonymously (setup mode).
//
// Endpoints that must remain unauthenticated (e.g. /api/ui/config for
// frontend bootstrap) should NOT be wrapped with this middleware.
type AuthnMiddleware struct {
	Methods  *application.AuthMethodService
	Verifier domain.OIDCTokenVerifier
	Logger   *slog.Logger
}

// log returns the middleware's logger, falling back to the default logger
// when none is configured (avoids nil-checks at every call site).
func (a *AuthnMiddleware) log() *slog.Logger {
	if a.Logger != nil {
		return a.Logger
	}
	return slog.Default()
}

// Wrap returns an [http.Handler] that authenticates the request before
// calling next. On success an [application.AuthorizationContext] is
// attached to the request context.
func (a *AuthnMiddleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger := a.log()
		endpoint := r.Method + " " + r.URL.Path

		methods, err := a.Methods.List(r.Context())
		if err != nil {
			logger.ErrorContext(r.Context(), "failed to load auth methods", "endpoint", endpoint, "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "load auth methods"})
			return
		}

		// Pre-filter to OIDC methods — only these participate in
		// bearer-token authentication on HTTP routes.
		oidcMethods := make([]domain.AuthMethod, 0, len(methods))
		for _, m := range methods {
			if m.Type() == domain.AuthMethodTypeOIDC && m.OIDC() != nil {
				oidcMethods = append(oidcMethods, m)
			}
		}

		// Setup mode: no OIDC auth methods configured — allow anonymous.
		// Non-OIDC methods (e.g. future token-exchange, mTLS) do not
		// trigger enforced mode on these HTTP routes.
		if len(oidcMethods) == 0 {
			logger.DebugContext(r.Context(), "no OIDC auth methods configured, allowing anonymous", "endpoint", endpoint)
			next.ServeHTTP(w, r)
			return
		}

		// Auth enforced: require a valid Bearer token.
		token := extractBearer(r)
		if token == "" {
			logger.DebugContext(r.Context(), "missing bearer token", "endpoint", endpoint, "peer", r.RemoteAddr)
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing bearer token"})
			return
		}

		var subject *domain.SubjectClaims
		var matchedAudience []domain.Audience

		for _, m := range oidcMethods {
			claims, verifyErr := a.Verifier.Verify(r.Context(), *m.OIDC(), token)
			if verifyErr != nil {
				logger.DebugContext(r.Context(), "token verification failed for method",
					"endpoint", endpoint, "method_id", m.ID(), "audience", m.OIDC().Audience, "error", verifyErr)
				continue
			}
			subject = &claims
			matchedAudience = []domain.Audience{m.OIDC().Audience}
			break
		}

		if subject == nil {
			logger.DebugContext(r.Context(), "no auth method accepted the token",
				"endpoint", endpoint, "peer", r.RemoteAddr, "methods_checked", len(oidcMethods))
			w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "token verification failed"})
			return
		}

		logger.DebugContext(r.Context(), "request authenticated",
			"endpoint", endpoint, "issuer", subject.Issuer)

		// Propagate the same AuthorizationContext that the gRPC layer
		// uses, so downstream application services see a consistent
		// identity regardless of transport.
		var client *application.ClientClaims
		if azp, ok := subject.Extra["azp"]; ok && len(azp) > 0 {
			client = &application.ClientClaims{ID: application.ClientID(azp[0])}
		}
		authzCtx := &application.AuthorizationContext{
			Subject:  subject,
			Client:   client,
			Audience: matchedAudience,
			Token:    domain.RawToken(token),
			Request: application.RequestClaims{
				Method:   r.Method + " " + r.URL.Path,
				PeerAddr: r.RemoteAddr,
			},
		}
		ctx := application.ContextWithAuth(r.Context(), authzCtx)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
