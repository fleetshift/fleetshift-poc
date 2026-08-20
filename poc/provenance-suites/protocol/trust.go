package protocol

import (
	"bytes"
	"fmt"
)

// Requirement states whether a mechanism is required, allowed, or unused.
type Requirement string

const (
	RequirementNone     Requirement = "none"
	RequirementAllowed  Requirement = "allowed"
	RequirementRequired Requirement = "required"
)

// TenantMapping maps a verified external tenant partition onto a FleetShift
// tenant ID. The rule runs on verified material, not on an RM-supplied label.
type TenantMapping struct {
	// StaticTenant is the FleetShift tenant for a single-tenant authority.
	// An empty value is a missing mapping rule and fails closed.
	StaticTenant TenantID `json:"static_tenant,omitempty"`
}

// Map returns the FleetShift tenant for verified hints. Zero mappings and
// ambiguous mappings fail closed. This POC implements the static single-tenant
// rule; claim-derived mappings are a later profile concern.
func (m TenantMapping) Map(partition TenantPartition) (TenantID, error) {
	if m.StaticTenant == "" {
		return "", fmt.Errorf("%w: no tenant mapping rule is configured", ErrTenantMismatch)
	}
	if partition != "" {
		return "", fmt.Errorf("%w: static mapping does not admit tenant partition %q", ErrTenantMismatch, partition)
	}
	return m.StaticTenant, nil
}

// ProfileConfig is an authenticated provenance-profile entry inside an
// AuthorityConfig. It is not named by an RM-maintained profile ID.
type ProfileConfig struct {
	ProvenanceType ProvenanceType `json:"provenance_type"`
	// Parameters are authenticated profile-specific anchors and constraints.
	// The naive direct-key profile has none beyond the type itself.
	Parameters []byte `json:"parameters,omitempty"`
}

// Digest returns the exact authenticated profile-configuration digest.
func (c ProfileConfig) Digest() (Digest, error) {
	return DigestObject(purposeProfileConfig, c)
}

// DeliveryPolicy is one deterministic policy under an authority.
type DeliveryPolicy struct {
	// Match is the bounded delivery context this policy applies to.
	Match PolicyMatch `json:"match"`
	// LiveCredential is whether live credential presentation is required
	// or allowed. This POC exercises provenance-only policies.
	LiveCredential Requirement `json:"live_credential"`
	// Provenance is whether durable provenance is required or allowed.
	Provenance Requirement `json:"provenance"`
	// Profiles is the ordered any-of list of provenance profiles.
	Profiles []ProfileConfig `json:"profiles"`
}

// PolicyMatch is the bounded context used to locate a delivery policy.
type PolicyMatch struct {
	PredicateType     PredicateType `json:"predicate_type"`
	RootAuthorization bool          `json:"root_authorization"`
}

// Matches reports whether delivery context selects this match. Tentative
// tenant and subject hints are not policy keys; they are re-checked after
// verification.
func (m PolicyMatch) Matches(ctx DeliveryContext) bool {
	return m.PredicateType == ctx.PredicateType && m.RootAuthorization == ctx.RootAuthorization
}

// AuthorityConfig is keyed by canonical principal authority, not by a
// FleetShift tenant and not by a globally named trust-domain object.
type AuthorityConfig struct {
	PrincipalAuthority PrincipalAuthority `json:"principal_authority"`
	TenantMapping      TenantMapping      `json:"tenant_mapping"`
	CredentialMethods  []string           `json:"credential_methods,omitempty"`
	ProvenanceProfiles []ProfileConfig    `json:"provenance_profiles"`
	DeliveryPolicies   []DeliveryPolicy   `json:"delivery_policies"`
}

// Digest returns the exact authenticated authority-configuration digest.
func (c AuthorityConfig) Digest() (Digest, error) {
	return DigestObject(purposeAuthorityConfig, c)
}

// TrustConfiguration is the complete authenticated trust configuration
// bootstrapped onto a verifier. Subsequent changes are trust-config-update
// deliveries, not a return to TOFU.
type TrustConfiguration struct {
	AuthorityRegistry []AuthorityConfig `json:"authority_registry"`
}

// Digest returns the exact authenticated trust-configuration digest.
func (c TrustConfiguration) Digest() (Digest, error) {
	return DigestObject(purposeTrustConfiguration, c)
}

// Authority locates the unique AuthorityConfig for a principal authority.
func (c TrustConfiguration) Authority(key PrincipalAuthority) (AuthorityConfig, error) {
	var found *AuthorityConfig
	for i := range c.AuthorityRegistry {
		cfg := &c.AuthorityRegistry[i]
		if cfg.PrincipalAuthority == key {
			if found != nil {
				return AuthorityConfig{}, fmt.Errorf("%w: overlapping authority %s %s", ErrUnknownAuthority, key.Scheme, key.Authority)
			}
			found = cfg
		}
	}
	if found == nil {
		return AuthorityConfig{}, fmt.Errorf("%w: %s %s", ErrUnknownAuthority, key.Scheme, key.Authority)
	}
	return *found, nil
}

// DeliveryCommitment is the immutable log record for one durable mutation.
// Appending it orders the mutation; it does not authorize content.
type DeliveryCommitment struct {
	Index                 uint64          `json:"index"`
	TargetID              string          `json:"target_id"`
	FulfillmentID         string          `json:"fulfillment_id"`
	Generation            uint64          `json:"generation"`
	PredicateType         PredicateType   `json:"predicate_type"`
	Evidence              []TypedEvidence `json:"evidence"`
	AuthorityConfigDigest Digest          `json:"authority_config_digest"`
}

// Digest returns the commitment digest bound into the append-only log.
func (c DeliveryCommitment) Digest() (Digest, error) {
	return DigestObject(purposeDeliveryCommitment, c)
}

// Checkpoint is a retained append-only log position. Size is the number of
// accepted commitments. This POC uses the size as a local ordering fence;
// Merkle inclusion and consistency proofs are left to later profiles.
type Checkpoint struct {
	Size uint64 `json:"size"`
}

// EmptyCheckpoint is the uninitialized log position.
func EmptyCheckpoint() Checkpoint {
	return Checkpoint{}
}

// Equal reports whether two authority configs are byte-identical after
// canonical encoding.
func (c AuthorityConfig) Equal(other AuthorityConfig) bool {
	left, err := MarshalCanonical(c)
	if err != nil {
		return false
	}
	right, err := MarshalCanonical(other)
	if err != nil {
		return false
	}
	return bytes.Equal(left, right)
}
