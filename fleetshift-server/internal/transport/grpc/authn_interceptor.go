package grpc

import (
	"context"
	"strings"
	"sync"
	"time"

	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// AuthnInterceptor extracts credentials from incoming requests, validates
// them against configured authentication methods, and attaches an
// [application.AuthorizationContext] to the request context.
type AuthnInterceptor struct {
	methods  *application.AuthMethodService
	verifier domain.OIDCTokenVerifier
	observer domain.AuthnObserver

	cacheMu      sync.RWMutex
	cachedAt     time.Time
	cachedResult []domain.AuthMethod
	cacheTTL     time.Duration
}

// NewAuthnInterceptor creates an interceptor that authenticates requests
// using the given services.
func NewAuthnInterceptor(methods *application.AuthMethodService, verifier domain.OIDCTokenVerifier, observer domain.AuthnObserver) *AuthnInterceptor {
	return &AuthnInterceptor{
		methods:  methods,
		verifier: verifier,
		observer: observer,
		cacheTTL: 30 * time.Second,
	}
}

// Unary returns a unary server interceptor.
func (a *AuthnInterceptor) Unary() grpclib.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpclib.UnaryServerInfo,
		handler grpclib.UnaryHandler,
	) (any, error) {
		ctx, err := a.authenticate(ctx, info.FullMethod)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// Stream returns a stream server interceptor.
func (a *AuthnInterceptor) Stream() grpclib.StreamServerInterceptor {
	return func(
		srv any,
		ss grpclib.ServerStream,
		info *grpclib.StreamServerInfo,
		handler grpclib.StreamHandler,
	) error {
		ctx, err := a.authenticate(ss.Context(), info.FullMethod)
		if err != nil {
			return err
		}
		return handler(srv, &wrappedStream{ServerStream: ss, ctx: ctx})
	}
}

func (a *AuthnInterceptor) authenticate(ctx context.Context, fullMethod string) (context.Context, error) {
	reqInfo := domain.AuthnRequestInfo{Method: fullMethod}
	if p, ok := peer.FromContext(ctx); ok {
		reqInfo.PeerAddr = p.Addr.String()
	}

	ctx, probe := a.observer.Authenticate(ctx, reqInfo)
	defer probe.End()

	methods, err := a.loadMethods(ctx)
	if err != nil {
		probe.Error(err)
		return ctx, status.Errorf(codes.Internal, "load auth methods: %v", err)
	}
	probe.MethodsLoaded(len(methods))

	var subject *domain.SubjectClaims
	var client *application.ClientClaims
	var matchedAudience []domain.Audience
	var matchedToken domain.RawToken
	var matchedType domain.AuthMethodType

	for _, m := range methods {
		switch m.Type {
		case domain.AuthMethodTypeOIDC:
			token := extractBearerToken(ctx)
			if token == "" {
				probe.CredentialMissing(m.Type)
				continue
			}
			probe.VerifyingCredential(m.ID, m.Type)
			claims, verifyErr := a.verifier.Verify(ctx, *m.OIDC, token)
			if verifyErr != nil {
				probe.Error(verifyErr)
				return ctx, status.Errorf(codes.Unauthenticated, "token verification failed: %v", verifyErr)
			}
			subject = &claims
			matchedType = m.Type
			matchedAudience = []domain.Audience{m.OIDC.Audience}
			matchedToken = domain.RawToken(token)
			// TODO: this is inappropriate here
			if azp, ok := claims.Extra["azp"]; ok && len(azp) > 0 {
				client = &application.ClientClaims{ID: application.ClientID(azp[0])}
			}
		}
		if subject != nil {
			break
		}
	}

	if subject != nil {
		probe.Authenticated(matchedType, *subject)
	} else {
		probe.Anonymous()
	}

	reqClaims := application.RequestClaims{Method: fullMethod}
	reqClaims.PeerAddr = reqInfo.PeerAddr

	authzCtx := &application.AuthorizationContext{
		Subject:  subject,
		Client:   client,
		Audience: matchedAudience,
		Token:    matchedToken,
		Request:  reqClaims,
	}
	return application.ContextWithAuth(ctx, authzCtx), nil
}

// InvalidateMethodCache forces the next authentication attempt to
// reload auth methods from the store. Call after creating or
// modifying auth methods.
func (a *AuthnInterceptor) InvalidateMethodCache() {
	a.cacheMu.Lock()
	a.cachedAt = time.Time{}
	a.cachedResult = nil
	a.cacheMu.Unlock()
}

func (a *AuthnInterceptor) loadMethods(ctx context.Context) ([]domain.AuthMethod, error) {
	a.cacheMu.RLock()
	if time.Since(a.cachedAt) < a.cacheTTL {
		result := a.cachedResult
		a.cacheMu.RUnlock()
		return result, nil
	}
	a.cacheMu.RUnlock()

	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()

	if time.Since(a.cachedAt) < a.cacheTTL {
		return a.cachedResult, nil
	}

	methods, err := a.methods.List(ctx)
	if err != nil {
		return nil, err
	}
	a.cachedResult = methods
	a.cachedAt = time.Now()
	return methods, nil
}

func extractBearerToken(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return ""
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(vals[0], prefix) {
		return ""
	}
	return vals[0][len(prefix):]
}

type wrappedStream struct {
	grpclib.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context {
	return w.ctx
}
