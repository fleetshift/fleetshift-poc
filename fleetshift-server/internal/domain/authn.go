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
//
// Construct new instances with [NewOIDCAuthMethod]; reconstitute from
// persistence with [AuthMethodFromSnapshot]. Read via accessor methods.
type AuthMethod struct {
	id         AuthMethodID
	authType   AuthMethodType
	oidcConfig *OIDCConfig // non-nil when authType == AuthMethodTypeOIDC
}

// NewOIDCAuthMethod creates a brand-new OIDC [AuthMethod]. The id may
// be empty when the transport layer constructs from a proto message
// before the service assigns the final identifier. Use this on
// creation paths; use [AuthMethodFromSnapshot] only for reconstituting
// from persistence.
func NewOIDCAuthMethod(id AuthMethodID, config *OIDCConfig) AuthMethod {
	return AuthMethod{
		id:         id,
		authType:   AuthMethodTypeOIDC,
		oidcConfig: config,
	}
}

// Validate checks that exactly one protocol config is set for the given type.
func (m AuthMethod) Validate() error {
	switch m.authType {
	case AuthMethodTypeOIDC:
		if m.oidcConfig == nil {
			return ErrInvalidArgument
		}
	default:
		return ErrInvalidArgument
	}
	return nil
}

// ID returns the auth method's unique identifier.
func (m AuthMethod) ID() AuthMethodID { return m.id }

// Type returns the authentication protocol type.
func (m AuthMethod) Type() AuthMethodType { return m.authType }

// OIDC returns the OIDC configuration, if any.
func (m AuthMethod) OIDC() *OIDCConfig { return m.oidcConfig }

// IssuerURL identifies an OIDC issuer.
type IssuerURL string

// Audience identifies the intended recipient of a token (the aud claim).
type Audience string

// EndpointURL is a URL for an OIDC protocol endpoint (JWKS, authorize, token).
type EndpointURL string

// OIDCConfig holds the configuration for an OIDC authentication method.
type OIDCConfig struct {
	IssuerURL                IssuerURL
	Audience                 Audience
	JWKSURI                  EndpointURL // resolved from discovery
	AuthorizationEndpoint    EndpointURL // resolved from discovery
	TokenEndpoint            EndpointURL // resolved from discovery
	KeyEnrollmentAudience    Audience    // audience for signer enrollment ID tokens
	PublicKeyClaimExpression string      // CEL expression extracting the base64 SPKI public key from ID token claims
	RegistrySubjectMapping   *RegistrySubjectMapping
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

// BuiltInKeyRegistries returns the set of known external key registries.
// OIDC key resolution is not an external registry — it extracts the
// public key directly from the enrollment ID token via a CEL expression.
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
	IssuerURL                IssuerURL               `json:"issuer_url"`
	JWKSURI                  EndpointURL             `json:"jwks_uri"`
	EnrollmentAudience       Audience                `json:"enrollment_audience"`
	PublicKeyClaimExpression string                  `json:"public_key_claim_expression,omitempty"`
	RegistrySubjectMapping   *RegistrySubjectMapping `json:"registry_subject_mapping,omitempty"`
}

// TrustBundleManifestType is the [ManifestType] for IdP trust bundle
// manifests delivered to agents.
const TrustBundleManifestType ManifestType = "idp-trust-bundle"

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
//
// Construct new instances with [NewSignerEnrollment]; reconstitute from
// persistence with [SignerEnrollmentFromSnapshot]. Read via accessor
// methods.
type SignerEnrollment struct {
	id                SignerEnrollmentID
	federatedIdentity FederatedIdentity
	identityToken     RawToken        // purpose-scoped enrollment ID token
	registrySubject   RegistrySubject // derived from CEL mapping at enrollment time
	registryID        KeyRegistryID   // which registry holds the user's signing keys
	createdAt         time.Time
	expiresAt         time.Time
}

// NewSignerEnrollment creates a brand-new [SignerEnrollment]. Use this
// on creation paths; use [SignerEnrollmentFromSnapshot] only for
// reconstituting from persistence.
func NewSignerEnrollment(id SignerEnrollmentID, identity FederatedIdentity, token RawToken, subject RegistrySubject, registryID KeyRegistryID, now time.Time, expiresAt time.Time) SignerEnrollment {
	return SignerEnrollment{
		id:                id,
		federatedIdentity: identity,
		identityToken:     token,
		registrySubject:   subject,
		registryID:        registryID,
		createdAt:         now,
		expiresAt:         expiresAt,
	}
}

// ID returns the enrollment's unique identifier.
func (e SignerEnrollment) ID() SignerEnrollmentID { return e.id }

// Subject returns the enrolled subject's identifier.
func (e SignerEnrollment) Subject() SubjectID { return e.federatedIdentity.Subject }

// Issuer returns the enrolled subject's issuer.
func (e SignerEnrollment) Issuer() IssuerURL { return e.federatedIdentity.Issuer }

// FederatedIdentity returns the full federated identity.
func (e SignerEnrollment) FederatedIdentity() FederatedIdentity { return e.federatedIdentity }

// IdentityToken returns the purpose-scoped enrollment ID token.
func (e SignerEnrollment) IdentityToken() RawToken { return e.identityToken }

// RegistrySubject returns the derived registry subject.
func (e SignerEnrollment) RegistrySubject() RegistrySubject { return e.registrySubject }

// RegistryID returns the external key registry identifier.
func (e SignerEnrollment) RegistryID() KeyRegistryID { return e.registryID }

// CreatedAt returns the creation timestamp.
func (e SignerEnrollment) CreatedAt() time.Time { return e.createdAt }

// ExpiresAt returns the enrollment expiry timestamp.
func (e SignerEnrollment) ExpiresAt() time.Time { return e.expiresAt }

// SignerEnrollmentRepository persists signer enrollments.
//
// TODO: add Delete(ctx, id) to support clean re-enrollment from the UI.
// Currently re-enrollment creates a new row; ListBySubject returns newest
// first so callers pick the right key, but old rows accumulate.
type SignerEnrollmentRepository interface {
	Create(ctx context.Context, enrollment SignerEnrollment) error
	Get(ctx context.Context, id SignerEnrollmentID) (SignerEnrollment, error)
	ListBySubject(ctx context.Context, identity FederatedIdentity) ([]SignerEnrollment, error)
}
