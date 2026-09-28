package protocol

import (
	"context"
	"fmt"
)

// SelectAndVerify runs the common delivery-policy selection algorithm and
// returns the first profile attempt that fully verifies, including
// occurrence verification, timestamp coordination, window normalization,
// and the authenticated authority/profile/policy that produced the result.
//
// The sequence is:
//  1. Parse the untrusted provenance type, media type, and type-specific hints.
//  2. Locate the authenticated AuthorityConfig by tentative (scheme, authority).
//  3. Locate one unambiguous delivery policy from delivery context. Predicate
//     type comes from evidence hints, not from a couriered assertion.
//  4. Filter that policy's ordered profile list to the evidence's type.
//  5. For each matching profile: BeginVerification, Prepare, verify the
//     item's log occurrence, verify prepared timestamp bindings, Finish,
//     normalize that attempt's constraints, and check temporal validity.
//  6. Derive the canonical principal and tenant mapping, then re-evaluate.
func SelectAndVerify(ctx context.Context, item Item, delivery DeliveryContext, trust TrustConfiguration, lookup TargetLookup, temporal TemporalVerificationServices) (VerificationResult, error) {
	evidence := item.Evidence
	if evidence.ProvenanceType == "" || evidence.MediaType == "" {
		return VerificationResult{}, fmt.Errorf("%w: provenance type and media type are required", ErrMalformedEvidence)
	}
	verifier, ok := lookup(evidence.ProvenanceType)
	if !ok || verifier.ProvenanceType() != evidence.ProvenanceType {
		return VerificationResult{}, fmt.Errorf("%w: %s", ErrUnknownProvenanceType, evidence.ProvenanceType)
	}

	hints, err := verifier.ParseHints(evidence)
	if err != nil {
		return VerificationResult{}, err
	}
	if hints.PredicateType == "" {
		return VerificationResult{}, fmt.Errorf("%w: predicate type hint is required", ErrMalformedEvidence)
	}
	delivery.PredicateType = hints.PredicateType

	authority, err := trust.Authority(PrincipalAuthority{Scheme: hints.Scheme, Authority: hints.Authority})
	if err != nil {
		return VerificationResult{}, err
	}

	policy, err := matchPolicy(authority, delivery)
	if err != nil {
		return VerificationResult{}, err
	}
	if err := checkProvenanceRequirement(policy); err != nil {
		return VerificationResult{}, err
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
		result, err := verifyCandidate(ctx, verifier, item, delivery, authority, policy, profile, temporal)
		if err != nil {
			last = err
			continue
		}
		if err := reevaluate(policy, profile, delivery, hints, authority, result.Authenticated); err != nil {
			return VerificationResult{}, err
		}
		return result, nil
	}
	if last != nil {
		return VerificationResult{}, fmt.Errorf("%w: %w", ErrNoSuccessfulProfile, last)
	}
	return VerificationResult{}, fmt.Errorf("%w: no profile of type %s", ErrNoSuccessfulProfile, evidence.ProvenanceType)
}

func verifyCandidate(ctx context.Context, verifier TargetAPI, item Item, delivery DeliveryContext, authority AuthorityConfig, policy DeliveryPolicy, profile ProfileConfig, temporal TemporalVerificationServices) (VerificationResult, error) {
	session, err := verifier.BeginVerification(ctx, VerifyRequest{
		Statement:       cloneSignedStatement(item.SignedStatement),
		ProfileConfig:   profile,
		AuthorityConfig: authority,
		DeliveryContext: delivery,
	})
	if err != nil {
		return VerificationResult{}, err
	}
	prep, err := session.Prepare(ctx)
	if err != nil {
		return VerificationResult{}, err
	}

	position, err := verifyItemOccurrence(ctx, item, temporal, evidenceLogRequirement(verifier))
	if err != nil {
		return VerificationResult{}, err
	}

	observations, err := verifyPreparedTimestamps(ctx, prep.Timestamps, temporal)
	if err != nil {
		return VerificationResult{}, err
	}

	authenticated, err := session.Finish(ctx, VerifiedProvenanceTemporalInputs{Timestamps: cloneTimeObservations(observations)})
	if err != nil {
		return VerificationResult{}, err
	}

	validity, err := NormalizeValidityWindow(authenticated.Established, authenticated.Retired)
	if err != nil {
		return VerificationResult{}, err
	}
	subject := projectSubjectTemporal(position, observations)
	if err := CheckTemporalValidity(subject, validity.Window); err != nil {
		return VerificationResult{}, err
	}
	return VerificationResult{
		ProvenanceAuthenticationResult: authenticated,
		Validity:                       validity,
		Temporal:                       subject,
		Authority:                      authority,
		Profile:                        profile,
		Policy:                         policy,
	}, nil
}

// pocTenantRequiresEvidenceLog is the package-private POC tenant policy:
// every reached statement must have a verified tenant evidence-log
// occurrence. Profile RequiresEvidenceLog() is unioned with this value.
const pocTenantRequiresEvidenceLog = true

func evidenceLogRequirement(profile TargetAPI) EvidenceLogRequirement {
	if profile.RequiresEvidenceLog() || pocTenantRequiresEvidenceLog {
		return EvidenceLogRequirement{Domain: LogDomainTenantEvidenceV1}
	}
	return EvidenceLogRequirement{}
}

func verifyItemOccurrence(ctx context.Context, item Item, temporal TemporalVerificationServices, requirement EvidenceLogRequirement) (*LogPosition, error) {
	required := requirement.Domain != ""
	supplied := item.EvidenceLog != nil
	if !required && !supplied {
		return nil, nil
	}
	if temporal.Log == nil {
		return nil, fmt.Errorf("%w: ordered-log verifier is required", ErrInvalidLogInclusion)
	}
	if !supplied {
		return nil, fmt.Errorf("%w: missing evidence-log inclusion", ErrInvalidLogInclusion)
	}
	binding, err := temporal.Log.VerifyOccurrence(ctx, item.Evidence, *item.EvidenceLog)
	if err != nil {
		return nil, err
	}
	identity, err := item.Evidence.Identity()
	if err != nil {
		return nil, fmt.Errorf("%w: evidence identity: %v", ErrInvalidLogInclusion, err)
	}
	if binding.Evidence != identity {
		return nil, fmt.Errorf("%w: verified occurrence identity does not match the statement", ErrInvalidLogInclusion)
	}
	if required && binding.Position.Domain != requirement.Domain {
		return nil, fmt.Errorf("%w: occurrence domain %q, want %q", ErrTemporalValidity, binding.Position.Domain, requirement.Domain)
	}
	pos := binding.Position
	return &pos, nil
}

func verifyPreparedTimestamps(ctx context.Context, bindings []UnverifiedTimestampBinding, temporal TemporalVerificationServices) ([]VerifiedTimeObservation, error) {
	if len(bindings) == 0 {
		return nil, nil
	}
	seen := make(map[Digest]struct{}, len(bindings))
	out := make([]VerifiedTimeObservation, 0, len(bindings))
	for _, binding := range bindings {
		snapshot := binding.Clone()
		id, err := snapshot.Identity()
		if err != nil {
			return nil, err
		}
		if _, dup := seen[id]; dup {
			return nil, fmt.Errorf("duplicate timestamp binding identity %q", id)
		}
		seen[id] = struct{}{}
		if temporal.Time == nil {
			return nil, fmt.Errorf("trusted-time verifier is required")
		}
		result, err := temporal.Time.VerifyTimestamp(ctx, snapshot.Clone())
		if err != nil {
			return nil, err
		}
		out = append(out, VerifiedTimeObservation{
			Binding:   id,
			Authority: result.Authority,
			Earliest:  result.Earliest,
			Latest:    result.Latest,
		})
	}
	return out, nil
}

func projectSubjectTemporal(position *LogPosition, observations []VerifiedTimeObservation) VerifiedSubjectTemporalInfo {
	subject := VerifiedSubjectTemporalInfo{}
	if position != nil {
		pos := *position
		subject.LogPosition = &pos
	}
	if len(observations) == 0 {
		return subject
	}
	subject.Times = make([]VerifiedSubjectTime, len(observations))
	for i, obs := range observations {
		subject.Times[i] = VerifiedSubjectTime{
			Authority: obs.Authority,
			Earliest:  obs.Earliest,
			Latest:    obs.Latest,
		}
	}
	return subject
}

func cloneTimeObservations(in []VerifiedTimeObservation) []VerifiedTimeObservation {
	if in == nil {
		return nil
	}
	out := make([]VerifiedTimeObservation, len(in))
	copy(out, in)
	return out
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
