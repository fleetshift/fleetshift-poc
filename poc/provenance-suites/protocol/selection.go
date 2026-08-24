package protocol

import (
	"context"
	"fmt"
)

// SelectAndVerify runs the common delivery-policy selection algorithm and
// returns the first AuthenticatedEvidence that fully verifies, together with
// the inner assertion extracted from the statement's evidence.
//
// The sequence is:
//  1. Parse the untrusted provenance type, media type, and type-specific hints.
//  2. Locate the authenticated AuthorityConfig by tentative (scheme, authority).
//  3. Locate one unambiguous delivery policy from delivery context. Predicate
//     type comes from evidence hints, not from a couriered assertion.
//  4. Filter that policy's ordered profile list to the evidence's type.
//  5. Try matching profiles in authenticated policy order.
//  6. Derive the canonical principal and tenant mapping, then re-evaluate.
func SelectAndVerify(ctx context.Context, statement SignedStatement, delivery DeliveryContext, trust TrustConfiguration, lookup TargetLookup) (AuthenticatedEvidence, TypedAssertion, error) {
	evidence := statement.Evidence
	if evidence.ProvenanceType == "" || evidence.MediaType == "" {
		return AuthenticatedEvidence{}, TypedAssertion{}, fmt.Errorf("%w: provenance type and media type are required", ErrMalformedEvidence)
	}
	verifier, ok := lookup(evidence.ProvenanceType)
	if !ok || verifier.ProvenanceType() != evidence.ProvenanceType {
		return AuthenticatedEvidence{}, TypedAssertion{}, fmt.Errorf("%w: %s", ErrUnknownProvenanceType, evidence.ProvenanceType)
	}

	hints, err := verifier.ParseHints(evidence)
	if err != nil {
		return AuthenticatedEvidence{}, TypedAssertion{}, err
	}
	if hints.PredicateType == "" {
		return AuthenticatedEvidence{}, TypedAssertion{}, fmt.Errorf("%w: predicate type hint is required", ErrMalformedEvidence)
	}
	delivery.PredicateType = hints.PredicateType

	authority, err := trust.Authority(PrincipalAuthority{Scheme: hints.Scheme, Authority: hints.Authority})
	if err != nil {
		return AuthenticatedEvidence{}, TypedAssertion{}, err
	}

	policy, err := matchPolicy(authority, delivery)
	if err != nil {
		return AuthenticatedEvidence{}, TypedAssertion{}, err
	}
	if err := checkProvenanceRequirement(policy); err != nil {
		return AuthenticatedEvidence{}, TypedAssertion{}, err
	}

	var last error
	for _, profile := range policy.Profiles {
		if profile.ProvenanceType != evidence.ProvenanceType {
			continue
		}
		if !profileInstalled(authority, profile) {
			last = fmt.Errorf("%w: policy profile is not in the authority's installed set", ErrUnknownProvenanceType)
			continue
		}
		authenticated, assertion, err := verifier.Verify(ctx, VerifyRequest{
			Statement:       statement,
			ProfileConfig:   profile,
			AuthorityConfig: authority,
			DeliveryContext: delivery,
		})
		if err != nil {
			last = err
			continue
		}
		if err := reevaluate(policy, profile, delivery, hints, authority, authenticated); err != nil {
			return AuthenticatedEvidence{}, TypedAssertion{}, err
		}
		return authenticated, assertion, nil
	}
	if last != nil {
		return AuthenticatedEvidence{}, TypedAssertion{}, fmt.Errorf("%w: %w", ErrNoSuccessfulProfile, last)
	}
	return AuthenticatedEvidence{}, TypedAssertion{}, fmt.Errorf("%w: no profile of type %s", ErrNoSuccessfulProfile, evidence.ProvenanceType)
}

func matchPolicy(authority AuthorityConfig, delivery DeliveryContext) (DeliveryPolicy, error) {
	var matched []DeliveryPolicy
	for _, policy := range authority.DeliveryPolicies {
		if policy.Match.Matches(delivery) {
			matched = append(matched, policy)
		}
	}
	switch len(matched) {
	case 0:
		return DeliveryPolicy{}, fmt.Errorf("%w: predicate type %s", ErrNoMatchingPolicy, delivery.PredicateType)
	case 1:
		return matched[0], nil
	default:
		return DeliveryPolicy{}, fmt.Errorf("%w: %d policies match predicate type %s", ErrAmbiguousPolicy, len(matched), delivery.PredicateType)
	}
}

func profileInstalled(authority AuthorityConfig, profile ProfileConfig) bool {
	want, err := profile.Digest()
	if err != nil {
		return false
	}
	for _, installed := range authority.ProvenanceProfiles {
		got, err := installed.Digest()
		if err != nil {
			continue
		}
		if got == want {
			return true
		}
	}
	return false
}

func checkProvenanceRequirement(policy DeliveryPolicy) error {
	switch policy.Provenance {
	case RequirementRequired, RequirementAllowed:
		return nil
	case RequirementNone:
		return fmt.Errorf("%w: policy does not admit provenance", ErrNoMatchingPolicy)
	default:
		return fmt.Errorf("%w: unknown provenance requirement %q", ErrNoMatchingPolicy, policy.Provenance)
	}
}

func reevaluate(policy DeliveryPolicy, selected ProfileConfig, delivery DeliveryContext, hints TentativeHints, authority AuthorityConfig, authenticated AuthenticatedEvidence) error {
	if authenticated.ProvenanceType == "" {
		return fmt.Errorf("%w: provenance type is missing", ErrPolicyReevaluation)
	}
	if authenticated.ProvenanceType != selected.ProvenanceType {
		return fmt.Errorf("%w: authenticated provenance type %s, selected %s", ErrPolicyReevaluation, authenticated.ProvenanceType, selected.ProvenanceType)
	}
	if hints.PredicateType != authenticated.PredicateType {
		return fmt.Errorf("%w: authenticated predicate type %s, hint %s", ErrPolicyReevaluation, authenticated.PredicateType, hints.PredicateType)
	}
	if !policy.Match.Matches(DeliveryContext{
		PredicateType:     authenticated.PredicateType,
		RootAuthorization: delivery.RootAuthorization,
	}) {
		return fmt.Errorf("%w: authenticated predicate type %s", ErrPolicyReevaluation, authenticated.PredicateType)
	}
	if authenticated.Principal.Scheme != hints.Scheme || authenticated.Principal.Authority != hints.Authority {
		return fmt.Errorf("%w: authenticated authority does not match tentative hints", ErrPolicyReevaluation)
	}
	if hints.TenantPartition != "" && authenticated.Principal.TenantPartition != hints.TenantPartition {
		return fmt.Errorf("%w: authenticated tenant partition does not match hint", ErrPolicyReevaluation)
	}
	if hints.Subject != "" && authenticated.Principal.Subject != hints.Subject {
		return fmt.Errorf("%w: authenticated subject does not match hint", ErrPolicyReevaluation)
	}

	mapped, err := authority.TenantMapping.Map(authenticated.Principal.TenantPartition)
	if err != nil {
		return err
	}
	if authenticated.MappedFleetShiftTenant != mapped {
		return fmt.Errorf("%w: authenticated tenant %q, mapped %q", ErrTenantMismatch, authenticated.MappedFleetShiftTenant, mapped)
	}
	if delivery.ClaimedTenant != "" && delivery.ClaimedTenant != mapped {
		return fmt.Errorf("%w: claimed tenant %q, mapped %q", ErrTenantMismatch, delivery.ClaimedTenant, mapped)
	}

	authorityDigest, err := authority.Digest()
	if err != nil {
		return err
	}
	if authenticated.AuthorityConfigDigest != authorityDigest {
		return fmt.Errorf("%w: authority-config digest", ErrPolicyReevaluation)
	}

	selectedDigest, err := selected.Digest()
	if err != nil {
		return err
	}
	if authenticated.ProfileConfigDigest != selectedDigest {
		return fmt.Errorf("%w: profile-config digest is not the profile that verified", ErrPolicyReevaluation)
	}
	return nil
}
