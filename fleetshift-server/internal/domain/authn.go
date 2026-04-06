package domain

import (
	"context"
	"time"
)

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
	IssuerURL              IssuerURL
	Audience               Audience
	JWKSURI                EndpointURL // resolved from discovery
	AuthorizationEndpoint  EndpointURL // resolved from discovery
	TokenEndpoint          EndpointURL // resolved from discovery
	KeyEnrollmentAudience  Audience    // audience for signing key enrollment ID tokens
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

// SubjectID identifies an authenticated subject within a single issuer.
// On its own it is ambiguous; use [FederatedIdentity] when an
// unambiguous cross-issuer reference is needed.
type SubjectID string

// FederatedIdentity unambiguously identifies a subject by pairing
// the issuer-scoped [SubjectID] with the [IssuerURL] that vouches
// for it. Two subjects with the same SubjectID from different issuers
// are distinct identities.
type FederatedIdentity struct {
	Subject SubjectID
	Issuer  IssuerURL
}

// SubjectClaims represents the identity claims produced by authenticating
// a credential via any supported protocol.
type SubjectClaims struct {
	FederatedIdentity
	Extra map[string][]string // groups, email, custom claims
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

// SigningKeyBindingID uniquely identifies a signing key binding.
type SigningKeyBindingID string

// SigningKeyBinding ties a user's ECDSA signing public key to their
// IdP identity via a self-certifying key binding bundle. The bundle
// is verified at enrollment time and stored for later use by the
// delivery agent during attestation validation.
type SigningKeyBinding struct {
	ID SigningKeyBindingID
	FederatedIdentity
	PublicKeyJWK        []byte
	Algorithm           string // e.g. "ES256"
	KeyBindingDoc       []byte // canonical JSON signed by the user
	KeyBindingSignature []byte // ECDSA signature over KeyBindingDoc
	IdentityToken       RawToken
	CreatedAt           time.Time
	ExpiresAt           time.Time
}

// SigningKeyBindingRepository persists signing key bindings.
type SigningKeyBindingRepository interface {
	Create(ctx context.Context, binding SigningKeyBinding) error
	Get(ctx context.Context, id SigningKeyBindingID) (SigningKeyBinding, error)
	ListBySubject(ctx context.Context, identity FederatedIdentity) ([]SigningKeyBinding, error)
}
