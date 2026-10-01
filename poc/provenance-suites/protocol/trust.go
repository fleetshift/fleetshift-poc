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

// ProfileConfig is an authenticated provenance-profile entry inside an
// AuthorityConfig. It is not named by an RM-maintained profile ID.
// Parameters are immutable once the configuration is published.
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

// DeliveryPolicy is one entry in an authority's ordered policy list.
// Its match and profile references are immutable once published.
type DeliveryPolicy struct {
	// Match is the bounded delivery context this policy applies to.
	Match PolicyMatch `json:"match"`
	// RequireEvidenceLog requires inclusion for assertions admitted by this
	// policy. A provenance mechanism can independently require the log.
	RequireEvidenceLog bool `json:"require_evidence_log"`
	// LiveCredential is whether live credential presentation is required
	// or allowed. This POC exercises provenance-only policies.
	LiveCredential Requirement `json:"live_credential"`
	// Provenance is whether durable provenance is required or allowed.
	Provenance Requirement `json:"provenance"`
	// Profiles is the ordered any-of list of authority-local configuration
	// digests. Each digest binds the complete ProfileConfig, including anchors.
	Profiles []Digest `json:"profiles"`
}

// PolicyMatch selects delivery policy by assertion purpose. The same policy
// applies when an assertion is the package root or a selected dependency.
type PolicyMatch struct {
	PredicateType PredicateType `json:"predicate_type"`
	// TenantPartition is an exact external tenant match within this authority.
	// Nil matches all tenants. A pointer to empty matches an unpartitioned
	// principal. The first matching policy wins, regardless of specificity.
	TenantPartition *TenantPartition `json:"tenant_partition,omitempty"`
}

// Matches reports whether delivery context selects this match. External
// tenant partitions are scoped by the containing authority and
// rechecked after authentication; internal routing IDs are not policy keys.
func (m PolicyMatch) Matches(ctx DeliveryContext) bool {
	return m.PredicateType == ctx.PredicateType &&
		(m.TenantPartition == nil || *m.TenantPartition == ctx.TenantPartition)
}

// AuthorityConfig is keyed by canonical principal authority, not by a
// FleetShift tenant and not by a globally named trust-domain object.
// Published configurations and their nested values are immutable. Common
// lookups return shared views; copy at an ownership boundary when needed.
type AuthorityConfig struct {
	PrincipalAuthority PrincipalAuthority `json:"principal_authority"`
	CredentialMethods  []string           `json:"credential_methods,omitempty"`
	ProvenanceProfiles []ProfileConfig    `json:"provenance_profiles"`
	// DeliveryPolicies are evaluated in order; the first matching entry owns
	// verification. Put tenant exceptions before their match-all default.
	DeliveryPolicies []DeliveryPolicy `json:"delivery_policies"`
}

// Digest returns the exact authenticated authority-configuration digest.
func (c AuthorityConfig) Digest() (Digest, error) {
	return DigestObject(purposeAuthorityConfig, c)
}

// Profile resolves an exact configuration digest within this authority.
// Missing or duplicate entries invalidate configuration. The returned value
// borrows immutable parameter bytes from this authority.
func (c AuthorityConfig) Profile(reference Digest) (ProfileConfig, error) {
	var found *ProfileConfig
	for _, profile := range c.ProvenanceProfiles {
		digest, err := profile.Digest()
		if err != nil {
			return ProfileConfig{}, fmt.Errorf("%w: profile digest: %v", ErrInvalidTrustConfiguration, err)
		}
		if digest == reference {
			if found != nil {
				return ProfileConfig{}, fmt.Errorf("%w: duplicate profile %s", ErrInvalidTrustConfiguration, reference)
			}
			copy := profile
			found = &copy
		}
	}
	if found == nil {
		return ProfileConfig{}, fmt.Errorf("%w: missing profile %s", ErrInvalidTrustConfiguration, reference)
	}
	return *found, nil
}

// TrustConfiguration is the complete authenticated trust configuration
// bootstrapped onto a verifier. Subsequent changes are trust-config-update
// deliveries, not a return to TOFU.
// The registry and all nested values are immutable once published. Common
// selection borrows them; Clone creates an independent configuration to edit.
type TrustConfiguration struct {
	AuthorityRegistry []AuthorityConfig `json:"authority_registry"`
}

// Digest returns the exact authenticated trust-configuration digest.
func (c TrustConfiguration) Digest() (Digest, error) {
	return DigestObject(purposeTrustConfiguration, c)
}

// Authority locates the unique AuthorityConfig for a principal authority.
// The returned configuration borrows immutable values from the registry.
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

// ResolveProfiles resolves an ordered policy list without allowing missing or
// duplicate references to be treated as an ordinary unsuccessful attempt.
// Each resolved entry borrows immutable parameters from this authority.
func (c AuthorityConfig) ResolveProfiles(references []Digest) ([]ProfileConfig, error) {
	out := make([]ProfileConfig, 0, len(references))
	seen := make(map[Digest]struct{}, len(references))
	for _, reference := range references {
		if _, duplicate := seen[reference]; duplicate {
			return nil, fmt.Errorf("%w: duplicate policy reference %s", ErrInvalidTrustConfiguration, reference)
		}
		seen[reference] = struct{}{}
		profile, err := c.Profile(reference)
		if err != nil {
			return nil, err
		}
		out = append(out, profile)
	}
	return out, nil
}

// Clone returns a detached authenticated configuration snapshot.
func (c TrustConfiguration) Clone() TrustConfiguration { return cloneTrustConfiguration(c) }

// SelectPolicy returns the first policy matching tentative assertion purpose
// and external tenant partition. Verification must reselect the same entry
// using authenticated content; neither hints nor lookup grant authority.
// Returned authority and policy data borrow immutable values from the registry.
func (c TrustConfiguration) SelectPolicy(hints TentativeHints) (AuthorityConfig, DeliveryPolicy, error) {
	authority, index, err := c.selectPolicy(hints)
	if err != nil {
		return AuthorityConfig{}, DeliveryPolicy{}, err
	}
	return authority, authority.DeliveryPolicies[index], nil
}

// selectPolicy retains the entry index for authenticated reselection within
// the same immutable authority snapshot. Policy entries need no separate ID.
func (c TrustConfiguration) selectPolicy(hints TentativeHints) (AuthorityConfig, int, error) {
	authority, err := c.Authority(PrincipalAuthority{Scheme: hints.Scheme, Authority: hints.Authority})
	if err != nil {
		return AuthorityConfig{}, 0, err
	}
	index, err := matchPolicy(authority, DeliveryContext{PredicateType: hints.PredicateType, TenantPartition: hints.TenantPartition})
	if err != nil {
		return AuthorityConfig{}, 0, err
	}
	return authority, index, nil
}

// Validate checks authority uniqueness, policy structure, and every profile
// reference before authenticated configuration is installed. Overlapping
// policy matches are permitted; configured order determines which applies.
func (c TrustConfiguration) Validate() error {
	if len(c.AuthorityRegistry) == 0 {
		return fmt.Errorf("%w: no authority registry", ErrInvalidTrustConfiguration)
	}
	authorities := make(map[PrincipalAuthority]struct{}, len(c.AuthorityRegistry))
	for _, authority := range c.AuthorityRegistry {
		if authority.PrincipalAuthority.Scheme == "" || authority.PrincipalAuthority.Authority == "" {
			return fmt.Errorf("%w: authority key is required", ErrInvalidTrustConfiguration)
		}
		if _, duplicate := authorities[authority.PrincipalAuthority]; duplicate {
			return fmt.Errorf("%w: duplicate authority", ErrInvalidTrustConfiguration)
		}
		authorities[authority.PrincipalAuthority] = struct{}{}
		profiles := make(map[Digest]struct{}, len(authority.ProvenanceProfiles))
		for _, profile := range authority.ProvenanceProfiles {
			if profile.ProvenanceType == "" {
				return fmt.Errorf("%w: profile type is required", ErrInvalidTrustConfiguration)
			}
			digest, err := profile.Digest()
			if err != nil {
				return err
			}
			if _, duplicate := profiles[digest]; duplicate {
				return fmt.Errorf("%w: duplicate profile %s", ErrInvalidTrustConfiguration, digest)
			}
			profiles[digest] = struct{}{}
		}
		for _, policy := range authority.DeliveryPolicies {
			if policy.Match.PredicateType == "" {
				return fmt.Errorf("%w: predicate is required", ErrInvalidTrustConfiguration)
			}
			if _, err := authority.ResolveProfiles(policy.Profiles); err != nil {
				return err
			}
			if policy.Provenance == RequirementRequired && len(policy.Profiles) == 0 {
				return fmt.Errorf("%w: required provenance has no profiles", ErrInvalidTrustConfiguration)
			}
		}
	}
	return nil
}

func cloneTrustConfiguration(in TrustConfiguration) TrustConfiguration {
	if in.AuthorityRegistry == nil {
		return TrustConfiguration{}
	}
	out := TrustConfiguration{AuthorityRegistry: make([]AuthorityConfig, len(in.AuthorityRegistry))}
	for i := range in.AuthorityRegistry {
		out.AuthorityRegistry[i] = cloneAuthorityConfig(in.AuthorityRegistry[i])
	}
	return out
}

func cloneAuthorityConfig(in AuthorityConfig) AuthorityConfig {
	out := in
	out.CredentialMethods = cloneSlice(in.CredentialMethods)
	if in.ProvenanceProfiles != nil {
		out.ProvenanceProfiles = make([]ProfileConfig, len(in.ProvenanceProfiles))
		for i := range in.ProvenanceProfiles {
			out.ProvenanceProfiles[i] = cloneProfileConfig(in.ProvenanceProfiles[i])
		}
	}
	if in.DeliveryPolicies != nil {
		out.DeliveryPolicies = make([]DeliveryPolicy, len(in.DeliveryPolicies))
		for i := range in.DeliveryPolicies {
			out.DeliveryPolicies[i] = cloneDeliveryPolicy(in.DeliveryPolicies[i])
		}
	}
	return out
}

func cloneProfileConfig(in ProfileConfig) ProfileConfig {
	out := in
	out.Parameters = cloneBytes(in.Parameters)
	return out
}

func cloneDeliveryPolicy(in DeliveryPolicy) DeliveryPolicy {
	out := in
	if in.Match.TenantPartition != nil {
		partition := *in.Match.TenantPartition
		out.Match.TenantPartition = &partition
	}
	out.Profiles = cloneSlice(in.Profiles)
	return out
}
