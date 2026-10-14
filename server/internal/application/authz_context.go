package application

import (
	"context"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

type authzContextKey struct{}

// AuthorizationContext is the full authentication result for a request.
// Transport builds it; application services consume it.
type AuthorizationContext struct {
	Subject  *domain.SubjectClaims // nil if anonymous
	Client   *ClientClaims         // nil if no client info
	Audience []domain.Audience     // token audience (aud claim)
	Token    domain.RawToken       // raw verified JWT; empty if no token-based auth
	Request  RequestClaims
}

// ClientID identifies an OAuth client.
type ClientID string

// ClientClaims identifies the OAuth client that presented the credential.
type ClientClaims struct {
	ID ClientID
}

// RequestClaims captures protocol-level details for later authorization.
// Purely between transport and application -- not a domain concept.
type RequestClaims struct {
	Method   string // gRPC full method, e.g. "/fleetshift.v1.DeploymentService/CreateDeployment"
	PeerAddr string // client address
}

// ContextWithAuth returns a context carrying the given [AuthorizationContext].
func ContextWithAuth(ctx context.Context, ac *AuthorizationContext) context.Context {
	return context.WithValue(ctx, authzContextKey{}, ac)
}

// AuthFromContext retrieves the [AuthorizationContext] from the context.
// Returns nil if no authentication was performed (anonymous request).
func AuthFromContext(ctx context.Context) *AuthorizationContext {
	ac, _ := ctx.Value(authzContextKey{}).(*AuthorizationContext)
	return ac
}
