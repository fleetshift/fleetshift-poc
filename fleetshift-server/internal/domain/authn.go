package domain

import "context"

// AuthMethodID uniquely identifies a configured authentication method.
type AuthMethodID string

// AuthMethodType enumerates supported authentication protocols.
type AuthMethodType string

const (
	// AuthMethodTypeOIDC represents OpenID Connect token-based authentication.
	AuthMethodTypeOIDC AuthMethodType = "oidc"
)

// AuthMethod is a configured authentication method. Exactly one of the
// protocol-specific config fields is set, corresponding to Type.
type AuthMethod struct {
	ID   AuthMethodID
	Type AuthMethodType
	OIDC *OIDCConfig // non-nil when Type == AuthMethodTypeOIDC
}

// Validate checks that exactly one protocol config is set for the given type.
func (m AuthMethod) Validate() error {
	switch m.Type {
	case AuthMethodTypeOIDC:
		if m.OIDC == nil {
			return ErrInvalidArgument
		}
	default:
		return ErrInvalidArgument
	}
	return nil
}

// IssuerURL identifies an OIDC issuer.
type IssuerURL string

// Audience identifies the intended recipient of a token (the aud claim).
type Audience string

// EndpointURL is a URL for an OIDC protocol endpoint (JWKS, authorize, token).
type EndpointURL string

// OIDCConfig holds the configuration for an OIDC authentication method.
type OIDCConfig struct {
	IssuerURL             IssuerURL
	Audience              Audience
	JWKSURI               EndpointURL // resolved from discovery
	AuthorizationEndpoint EndpointURL // resolved from discovery
	TokenEndpoint         EndpointURL // resolved from discovery
}

// OIDCMetadata is the resolved OIDC discovery document.
type OIDCMetadata struct {
	Issuer                IssuerURL
	AuthorizationEndpoint EndpointURL
	TokenEndpoint         EndpointURL
	JWKSURI               EndpointURL
}

// RawToken is a verified JWT string. It has been validated by the
// platform's authn layer and may be passed through to target APIs.
//
// TODO: encrypt at rest when persisted on deployments.
type RawToken string

// SubjectID uniquely identifies an authenticated subject.
type SubjectID string

// SubjectClaims represents the identity claims produced by authenticating
// a credential via any supported protocol.
type SubjectClaims struct {
	ID     SubjectID
	Issuer IssuerURL
	Extra  map[string][]string // groups, email, custom claims
}

// AuthMethodRepository persists configured authentication methods.
type AuthMethodRepository interface {
	Save(ctx context.Context, method AuthMethod) error
	Get(ctx context.Context, id AuthMethodID) (AuthMethod, error)
	List(ctx context.Context) ([]AuthMethod, error)
}

// OIDCTokenVerifier validates a JWT against OIDC configuration.
//
// The implementation requires a JWT library for parsing, signature
// verification, key management, and algorithm negotiation. The port
// isolates the domain from that library dependency. Infrastructure
// manages JWKS fetching and caching internally.
type OIDCTokenVerifier interface {
	Verify(ctx context.Context, config OIDCConfig, rawToken string) (SubjectClaims, error)
}

// OIDCDiscoveryClient fetches OIDC provider metadata.
//
// Used by the application service during auth method creation to resolve
// the discovery document and populate [OIDCConfig] with endpoints.
type OIDCDiscoveryClient interface {
	FetchMetadata(ctx context.Context, issuerURL IssuerURL) (OIDCMetadata, error)
}
