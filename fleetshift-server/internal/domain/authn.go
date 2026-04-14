package domain

import (
	"context"
	"crypto"
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
	IssuerURL             IssuerURL
	Audience              Audience
	JWKSURI               EndpointURL // resolved from discovery
	AuthorizationEndpoint EndpointURL // resolved from discovery
	TokenEndpoint         EndpointURL // resolved from discovery
	KeyEnrollmentAudience Audience    // audience for signer enrollment ID tokens
	// TODO: this could/should probably be multiple not one
	RegistrySubjectMapping *RegistrySubjectMapping
}

// KeyRegistryID identifies a known external key registry.
type KeyRegistryID string

// KeyRegistryType enumerates the supported external key registry protocols.
type KeyRegistryType string

const (
	// KeyRegistryTypeGitHub represents the GitHub SSH signing-key API.
	KeyRegistryTypeGitHub KeyRegistryType = "github"
)

// KeyRegistry describes a known external key registry.
type KeyRegistry struct {
	ID       KeyRegistryID
	Type     KeyRegistryType
	Endpoint string // e.g. "https://api.github.com"
}

// BuiltInKeyRegistries returns the set of known public registries.
// For the prototype only github.com is supported.
func BuiltInKeyRegistries() map[KeyRegistryID]KeyRegistry {
	return map[KeyRegistryID]KeyRegistry{
		"github.com": {
			ID:       "github.com",
			Type:     KeyRegistryTypeGitHub,
			Endpoint: "https://api.github.com",
		},
	}
}

// RegistrySubjectMapping configures how an ID token's claims are
// mapped to a registry subject. The CEL expression is evaluated over
// the token's claims map and must produce a string (for example a
// GitHub username).
type RegistrySubjectMapping struct {
	RegistryID KeyRegistryID
	Expression string // CEL expression; input variable is "claims" (map[string]any)
}

// TrustBundleEntry is a single issuer's trust configuration as
// delivered to agents via the idp-trust-bundle resource type. Agents
// use it to verify attestation identity tokens and derive registry
// subjects. The kind agent serializes these into provisioned target
// properties; the kubernetes agent deserializes them to build verifiers.
type TrustBundleEntry struct {
	IssuerURL              IssuerURL               `json:"issuer_url"`
	JWKSURI                EndpointURL             `json:"jwks_uri"`
	EnrollmentAudience     Audience                `json:"enrollment_audience"`
	RegistrySubjectMapping *RegistrySubjectMapping `json:"registry_subject_mapping,omitempty"`
}

// TrustBundleResourceType is the [ResourceType] for IdP trust bundle
// manifests delivered to agents.
const TrustBundleResourceType ResourceType = "idp-trust-bundle"

// OIDCMetadata is the resolved OIDC discovery document.
type OIDCMetadata struct {
	Issuer                IssuerURL
	AuthorizationEndpoint EndpointURL
	TokenEndpoint         EndpointURL
	JWKSURI               EndpointURL
}

// RawToken is a verified JWT string. It has been validated by the
// platform's authn layer and may be passed through to target APIs.
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

// RegistrySubject identifies a user within an external key registry
// (e.g. a GitHub username). Derived from IdP claims via the configured
// CEL [RegistrySubjectMapping] at enrollment time.
type RegistrySubject string

// RegistryClient fetches signing public keys for a subject from an
// external key registry. Infrastructure implements this for each
// [KeyRegistryType] (e.g. GitHub SSH signing keys).
type RegistryClient interface {
	FetchSigningKeys(ctx context.Context, endpoint string, subject RegistrySubject) ([]crypto.PublicKey, error)
}

// SignerEnrollmentID uniquely identifies a signer enrollment.
type SignerEnrollmentID string

// SignerEnrollment records that a user enrolled their signing identity
// with the platform. The external key registry (identified by
// RegistryID) is the authority for the user's public keys — there is
// no self-signed key bundle.
type SignerEnrollment struct {
	ID SignerEnrollmentID
	FederatedIdentity
	IdentityToken   RawToken        // purpose-scoped enrollment ID token
	RegistrySubject RegistrySubject // derived from CEL mapping at enrollment time
	RegistryID      KeyRegistryID   // which registry holds the user's signing keys
	CreatedAt       time.Time
	ExpiresAt       time.Time
}

// SignerEnrollmentRepository persists signer enrollments.
type SignerEnrollmentRepository interface {
	Create(ctx context.Context, enrollment SignerEnrollment) error
	Get(ctx context.Context, id SignerEnrollmentID) (SignerEnrollment, error)
	ListBySubject(ctx context.Context, identity FederatedIdentity) ([]SignerEnrollment, error)
}
