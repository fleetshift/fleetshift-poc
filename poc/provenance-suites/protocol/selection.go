package protocol

import (
	"context"
	"errors"
	"fmt"
)

const (
	maxPreparedTimestampBindings = 16
	maxPreparedTimestampBytes    = 512 * 1024
)

var errPreparedTimestampLimit = errors.New("prepared timestamp work limit exceeded")

// SelectAndVerify runs the common delivery-policy selection algorithm and
// returns the first profile attempt that fully verifies, including
// occurrence verification, timestamp coordination, window normalization,
// and the authenticated authority/profile/policy that produced the result.
// Policy context comes from evidence hints and is rechecked after authentication;
// tenant relationships belong to subsequent common semantic evaluation.
// Inputs are borrowed immutable values. Returned configuration shares trust's
// backing data. Copies are made where data crosses a profile boundary.
//
// The sequence is:
//  1. Parse the untrusted provenance type, media type, and type-specific hints.
//  2. Locate the authenticated AuthorityConfig by tentative (scheme, authority).
//  3. Select the first policy matching delivery context. Predicate type comes
//     from evidence hints, not from a couriered assertion. Policy failure does
//     not fall back to another policy.
//  4. Resolve the policy's configuration digests within this authority, then
//     filter the ordered profile list to the evidence's type.
//  5. For each matching profile: BeginVerification, Prepare, verify the
//     item's log occurrence, verify prepared timestamp bindings, Finish,
//     normalize that attempt's constraints, and check temporal validity.
//  6. Derive the canonical principal and external tenant, then re-evaluate.
func SelectAndVerify(ctx context.Context, item Item, trust TrustConfiguration, lookup TargetLookup, temporal TemporalVerificationServices) (VerificationResult, error) {
	evidence := item.Evidence
	if evidence.ProvenanceType == "" || evidence.MediaType == "" {
		return VerificationResult{}, fmt.Errorf("%w: provenance type and media type are required", ErrMalformedEvidence)
	}
	verifier, ok := lookup(evidence.ProvenanceType)
	if !ok || verifier.ProvenanceType() != evidence.ProvenanceType {
		return VerificationResult{}, fmt.Errorf("%w: %s", ErrUnknownProvenanceType, evidence.ProvenanceType)
	}

	hints, err := verifier.ParseHints(cloneTypedEvidence(evidence))
	if err != nil {
		return VerificationResult{}, err
	}
	if hints.PredicateType == "" {
		return VerificationResult{}, fmt.Errorf("%w: predicate type hint is required", ErrMalformedEvidence)
	}
	delivery := DeliveryContext{PredicateType: hints.PredicateType, TenantPartition: hints.TenantPartition}

	authority, policyIndex, err := trust.selectPolicy(hints)
	if err != nil {
		return VerificationResult{}, err
	}
	policy := authority.DeliveryPolicies[policyIndex]

	if err := checkProvenanceRequirement(policy); err != nil {
		return VerificationResult{}, err
	}

	profiles, err := authority.ResolveProfiles(policy.Profiles)
	if err != nil {
		return VerificationResult{}, err
	}
	var last error
	for _, profile := range profiles {
		if profile.ProvenanceType != evidence.ProvenanceType {
			continue
		}
		result, err := verifyCandidate(ctx, verifier, item, delivery, authority, policy, profile, temporal)
		if err != nil {
			if errors.Is(err, errPreparedTimestampLimit) {
				return VerificationResult{}, err
			}
			last = err
			continue
		}
		if err := reevaluate(policyIndex, profile, hints, authority, result.Authenticated); err != nil {
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
	// Detach the profile request from common immutable evidence and policy.
	session, err := verifier.BeginVerification(ctx, VerifyRequest{
		Statement:       cloneSignedStatement(item.SignedStatement),
		ProfileConfig:   cloneProfileConfig(profile),
		AuthorityConfig: cloneAuthorityConfig(authority),
		DeliveryContext: delivery,
	})
	if err != nil {
		return VerificationResult{}, err
	}
	prep, err := session.Prepare(ctx)
	if err != nil {
		return VerificationResult{}, err
	}
	if err := validatePreparedTimestampBounds(prep.Timestamps); err != nil {
		return VerificationResult{}, err
	}
	// Take ownership of buffers returned by the profile before common use.
	prep = cloneTemporalPreparation(prep)

	position, err := verifyItemOccurrence(ctx, item, temporal, evidenceLogRequirement(verifier, policy.RequireEvidenceLog))
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
	authenticated = cloneProvenanceAuthenticationResult(authenticated)

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

func cloneTypedEvidence(in TypedEvidence) TypedEvidence {
	return TypedEvidence{ProvenanceType: in.ProvenanceType, Encoded: in.Encoded.Clone()}
}

func cloneProvenanceAuthenticationResult(in ProvenanceAuthenticationResult) ProvenanceAuthenticationResult {
	out := in
	out.Authenticated.SatisfiedConstraints = cloneSlice(in.Authenticated.SatisfiedConstraints)
	out.Assertion.Bytes = cloneBytes(in.Assertion.Bytes)
	out.Established = cloneTemporalConstraints(in.Established)
	out.Retired = cloneTemporalConstraints(in.Retired)
	return out
}

func cloneTemporalConstraints(in []AuthenticatedTemporalConstraint) []AuthenticatedTemporalConstraint {
	if in == nil {
		return nil
	}
	out := make([]AuthenticatedTemporalConstraint, len(in))
	for i, constraint := range in {
		out[i] = AuthenticatedTemporalConstraint{
			Boundary: cloneTemporalBoundary(constraint.Boundary),
			Basis:    cloneSlice(constraint.Basis),
		}
	}
	return out
}

func cloneTemporalBoundary(in AuthenticatedTemporalBoundary) AuthenticatedTemporalBoundary {
	out := AuthenticatedTemporalBoundary{}
	if in.Log != nil {
		log := *in.Log
		out.Log = &log
	}
	if in.Time != nil {
		time := cloneTimeBoundary(*in.Time)
		out.Time = &time
	}
	return out
}

func validatePreparedTimestampBounds(bindings []UnverifiedTimestampBinding) error {
	if len(bindings) > maxPreparedTimestampBindings {
		return fmt.Errorf("%w: binding count %d exceeds %d", errPreparedTimestampLimit, len(bindings), maxPreparedTimestampBindings)
	}

	totalBytes := 0
	for i, binding := range bindings {
		if len(binding.Token) > MaxTimestampTokenBytes {
			return fmt.Errorf("%w: %w: binding %d token length %d exceeds %d", errPreparedTimestampLimit, ErrInvalidTimestampBinding, i, len(binding.Token), MaxTimestampTokenBytes)
		}
		if len(binding.Message) > MaxTimestampMessageBytes {
			return fmt.Errorf("%w: %w: binding %d message length %d exceeds %d", errPreparedTimestampLimit, ErrInvalidTimestampBinding, i, len(binding.Message), MaxTimestampMessageBytes)
		}
		bindingBytes := len(binding.Token) + len(binding.Message)
		if bindingBytes > maxPreparedTimestampBytes-totalBytes {
			return fmt.Errorf("%w: aggregate token and message bytes exceed %d", errPreparedTimestampLimit, maxPreparedTimestampBytes)
		}
		totalBytes += bindingBytes
	}
	return nil
}

func cloneTemporalPreparation(in TemporalPreparation) TemporalPreparation {
	if in.Timestamps == nil {
		return TemporalPreparation{}
	}
	out := TemporalPreparation{Timestamps: make([]UnverifiedTimestampBinding, len(in.Timestamps))}
	for i := range in.Timestamps {
		out.Timestamps[i] = in.Timestamps[i].Clone()
	}
	return out
}

func evidenceLogRequirement(profile TargetAPI, policyRequiresEvidenceLog bool) EvidenceLogRequirement {
	if profile.RequiresEvidenceLog() || policyRequiresEvidenceLog {
		return EvidenceLogRequirement{Domain: LogDomainTenantEvidenceV1}
	}
	return EvidenceLogRequirement{}
}

// verifyItemOccurrence enforces one reached statement's log requirement, the
// union of its selected source policy and mechanism requirements. It also
// verifies supplied optional inclusion, using the log verifier bound to the
// package's prepared checkpoint. Checkpoint preparation alone does not enforce
// whether this statement must have an occurrence.
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
	type identifiedBinding struct {
		binding  UnverifiedTimestampBinding
		identity Digest
	}
	identified := make([]identifiedBinding, len(bindings))
	seen := make(map[Digest]struct{}, len(bindings))
	for i, binding := range bindings {
		id, err := binding.Identity()
		if err != nil {
			return nil, err
		}
		if _, dup := seen[id]; dup {
			return nil, fmt.Errorf("duplicate timestamp binding identity %q", id)
		}
		seen[id] = struct{}{}
		identified[i] = identifiedBinding{binding: binding, identity: id}
	}
	if temporal.Time == nil {
		return nil, fmt.Errorf("trusted-time verifier is required")
	}

	out := make([]VerifiedTimeObservation, 0, len(identified))
	for _, prepared := range identified {
		result, err := temporal.Time.VerifyTimestamp(ctx, prepared.binding.Clone())
		if err != nil {
			return nil, err
		}
		out = append(out, VerifiedTimeObservation{
			Binding:   prepared.identity,
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
	return cloneSlice(in)
}

func matchPolicy(authority AuthorityConfig, delivery DeliveryContext) (int, error) {
	for i, policy := range authority.DeliveryPolicies {
		if policy.Match.Matches(delivery) {
			return i, nil
		}
	}
	return 0, fmt.Errorf("%w: predicate type %s", ErrNoMatchingPolicy, delivery.PredicateType)
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

func reevaluate(policyIndex int, selected ProfileConfig, hints TentativeHints, authority AuthorityConfig, authenticated AuthenticatedEvidence) error {
	if authenticated.ProvenanceType == "" {
		return fmt.Errorf("%w: provenance type is missing", ErrPolicyReevaluation)
	}
	if authenticated.ProvenanceType != selected.ProvenanceType {
		return fmt.Errorf("%w: authenticated provenance type %s, selected %s", ErrPolicyReevaluation, authenticated.ProvenanceType, selected.ProvenanceType)
	}
	if hints.PredicateType != authenticated.PredicateType {
		return fmt.Errorf("%w: authenticated predicate type %s, hint %s", ErrPolicyReevaluation, authenticated.PredicateType, hints.PredicateType)
	}
	authenticatedPolicyIndex, err := matchPolicy(authority, DeliveryContext{
		PredicateType:   authenticated.PredicateType,
		TenantPartition: authenticated.Principal.TenantPartition,
	})
	if err != nil {
		return fmt.Errorf("%w: %v", ErrPolicyReevaluation, err)
	}
	// Reselect the exact entry in the authenticated snapshot. A match-all
	// policy chosen through an omitted hint cannot bypass an earlier tenant rule.
	if authenticatedPolicyIndex != policyIndex {
		return fmt.Errorf("%w: authenticated tenant selects a different policy", ErrPolicyReevaluation)
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
