package directkey

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
)

// Target is the direct-key/v1 target API. It retains a public-key and user
// mapping established by applying enrollment and verifies delivery signatures
// against that mapping only.
type Target struct {
	mu       sync.Mutex
	bindings map[protocol.Principal][]byte
}

// NewTarget returns an empty verifier with no enrolled keys.
func NewTarget() *Target {
	return &Target{bindings: make(map[protocol.Principal][]byte)}
}

// ProvenanceType implements protocol.TargetAPI.
func (t *Target) ProvenanceType() protocol.ProvenanceType {
	return protocol.ProvenanceTypeDirectKeyV1
}

// Owns implements protocol.TargetAPI. Direct-key/v1 applies only
// enrollment; intent predicates are handled by the delivery agent.
func (t *Target) Owns(predicate protocol.PredicateType) bool {
	return predicate == PredicateTypeEnrollmentV1
}

// ParseHints extracts tentative principal fields from untrusted evidence.
func ParseHints(evidence protocol.TypedEvidence) (protocol.TentativeHints, error) {
	return NewTarget().ParseHints(evidence)
}

// ParseHints implements protocol.TargetAPI.
func (t *Target) ParseHints(evidence protocol.TypedEvidence) (protocol.TentativeHints, error) {
	if evidence.ProvenanceType != protocol.ProvenanceTypeDirectKeyV1 {
		return protocol.TentativeHints{}, fmt.Errorf("%w: %s", protocol.ErrUnknownProvenanceType, evidence.ProvenanceType)
	}
	switch evidence.MediaType {
	case MediaTypeEnrollment:
		body, err := parseEnrollment(evidence)
		if err != nil {
			return protocol.TentativeHints{}, err
		}
		return principalHints(body.Principal, PredicateTypeEnrollmentV1), nil
	case MediaTypeSignature:
		body, err := parseSignature(evidence)
		if err != nil {
			return protocol.TentativeHints{}, err
		}
		return principalHints(body.Principal, body.Assertion.PredicateType), nil
	default:
		return protocol.TentativeHints{}, fmt.Errorf("%w: %s", protocol.ErrUnknownMediaType, evidence.MediaType)
	}
}

func principalHints(principal protocol.Principal, predicate protocol.PredicateType) protocol.TentativeHints {
	return protocol.TentativeHints{
		Scheme:          principal.Scheme,
		Authority:       principal.Authority,
		TenantPartition: principal.TenantPartition,
		Subject:         principal.Subject,
		PredicateType:   predicate,
	}
}

// Verify implements protocol.TargetAPI. Signature verification uses the
// retained enrollment mapping, never a key from the delivery or from support
// material. Enrollment verification authenticates proof of possession and
// does not require a retained key.
func (t *Target) Verify(_ context.Context, req protocol.VerifyRequest) (protocol.AuthenticatedEvidence, protocol.TypedAssertion, error) {
	if req.ProfileConfig.ProvenanceType != protocol.ProvenanceTypeDirectKeyV1 {
		return protocol.AuthenticatedEvidence{}, protocol.TypedAssertion{}, fmt.Errorf("%w: %s", protocol.ErrUnknownProvenanceType, req.ProfileConfig.ProvenanceType)
	}
	if len(req.ProfileConfig.Parameters) != 0 {
		return protocol.AuthenticatedEvidence{}, protocol.TypedAssertion{}, fmt.Errorf("%w: direct-key/v1 has no profile parameters", protocol.ErrUnknownProvenanceType)
	}
	evidence := req.Statement.Evidence
	if evidence.ProvenanceType != protocol.ProvenanceTypeDirectKeyV1 {
		return protocol.AuthenticatedEvidence{}, protocol.TypedAssertion{}, fmt.Errorf("%w: %s", protocol.ErrUnknownProvenanceType, evidence.ProvenanceType)
	}
	switch evidence.MediaType {
	case MediaTypeEnrollment:
		return t.verifyEnrollment(req)
	case MediaTypeSignature:
		return t.verifySignature(req)
	default:
		return protocol.AuthenticatedEvidence{}, protocol.TypedAssertion{}, fmt.Errorf("%w: %s", protocol.ErrUnknownMediaType, evidence.MediaType)
	}
}

func (t *Target) verifyEnrollment(req protocol.VerifyRequest) (protocol.AuthenticatedEvidence, protocol.TypedAssertion, error) {
	body, err := parseEnrollment(req.Statement.Evidence)
	if err != nil {
		return protocol.AuthenticatedEvidence{}, protocol.TypedAssertion{}, err
	}
	if req.AuthorityConfig.PrincipalAuthority != body.Principal.PrincipalAuthority() {
		return protocol.AuthenticatedEvidence{}, protocol.TypedAssertion{}, fmt.Errorf("%w: authority config does not match authenticated principal", protocol.ErrUnknownAuthority)
	}
	if err := verify(body.PublicKey, purposeEnrollmentProof, enrollmentProofMaterial(body.Principal, body.PublicKey), body.ProofOfPossession); err != nil {
		return protocol.AuthenticatedEvidence{}, protocol.TypedAssertion{}, fmt.Errorf("%w: enrollment proof of possession: %v", protocol.ErrVerificationFailed, err)
	}
	assertion, err := enrollmentAssertion(body)
	if err != nil {
		return protocol.AuthenticatedEvidence{}, protocol.TypedAssertion{}, err
	}
	authenticated, err := authenticatedResult(req, body.Principal, assertion)
	if err != nil {
		return protocol.AuthenticatedEvidence{}, protocol.TypedAssertion{}, err
	}
	return authenticated, assertion, nil
}

func (t *Target) verifySignature(req protocol.VerifyRequest) (protocol.AuthenticatedEvidence, protocol.TypedAssertion, error) {
	body, err := parseSignature(req.Statement.Evidence)
	if err != nil {
		return protocol.AuthenticatedEvidence{}, protocol.TypedAssertion{}, err
	}
	if req.AuthorityConfig.PrincipalAuthority != body.Principal.PrincipalAuthority() {
		return protocol.AuthenticatedEvidence{}, protocol.TypedAssertion{}, fmt.Errorf("%w: authority config does not match authenticated principal", protocol.ErrUnknownAuthority)
	}
	publicKey, ok := t.lookup(body.Principal)
	if !ok {
		return protocol.AuthenticatedEvidence{}, protocol.TypedAssertion{}, fmt.Errorf("%w: no retained public key for subject %q", protocol.ErrVerificationFailed, body.Principal.Subject)
	}
	contentDigest, err := body.Assertion.Digest()
	if err != nil {
		return protocol.AuthenticatedEvidence{}, protocol.TypedAssertion{}, err
	}
	if err := verify(publicKey, purposeAssertion, signatureMaterial(body.Principal, body.Assertion.PredicateType, contentDigest), body.Signature); err != nil {
		return protocol.AuthenticatedEvidence{}, protocol.TypedAssertion{}, err
	}
	assertion := protocol.TypedAssertion{
		PredicateType: body.Assertion.PredicateType,
		Bytes:         append([]byte(nil), body.Assertion.Bytes...),
	}
	authenticated, err := authenticatedResult(req, body.Principal, assertion)
	if err != nil {
		return protocol.AuthenticatedEvidence{}, protocol.TypedAssertion{}, err
	}
	return authenticated, assertion, nil
}

func authenticatedResult(req protocol.VerifyRequest, principal protocol.Principal, assertion protocol.TypedAssertion) (protocol.AuthenticatedEvidence, error) {
	mapped, err := req.AuthorityConfig.TenantMapping.Map(principal.TenantPartition)
	if err != nil {
		return protocol.AuthenticatedEvidence{}, err
	}
	authorityDigest, err := req.AuthorityConfig.Digest()
	if err != nil {
		return protocol.AuthenticatedEvidence{}, err
	}
	profileDigest, err := req.ProfileConfig.Digest()
	if err != nil {
		return protocol.AuthenticatedEvidence{}, err
	}
	contentDigest, err := assertion.Digest()
	if err != nil {
		return protocol.AuthenticatedEvidence{}, err
	}
	return protocol.AuthenticatedEvidence{
		Principal:              principal,
		MappedFleetShiftTenant: mapped,
		PredicateType:          assertion.PredicateType,
		ContentDigest:          contentDigest,
		ProvenanceType:         protocol.ProvenanceTypeDirectKeyV1,
		AuthorityConfigDigest:  authorityDigest,
		ProfileConfigDigest:    profileDigest,
	}, nil
}

// Apply implements protocol.TargetAPI. Enrollment is the mapping transition:
// first bind wins; later substitution of an established mapping is rejected.
// Unknown predicates fail closed. Intent predicates never reach Apply from
// the delivery agent.
func (t *Target) Apply(_ context.Context, req protocol.ApplyRequest) error {
	switch req.Authenticated.PredicateType {
	case PredicateTypeEnrollmentV1:
		return t.applyEnrollment(req)
	default:
		return fmt.Errorf("%w: %s", protocol.ErrUnknownPredicateType, req.Authenticated.PredicateType)
	}
}

func (t *Target) applyEnrollment(req protocol.ApplyRequest) error {
	if req.Statement.Evidence.MediaType != MediaTypeEnrollment {
		return fmt.Errorf("%w: enrollment apply requires %s", protocol.ErrUnknownMediaType, req.Statement.Evidence.MediaType)
	}
	body, err := DecodeEnrollmentAssertion(req.Assertion)
	if err != nil {
		return err
	}
	if !body.Principal.Equal(req.Authenticated.Principal) {
		return fmt.Errorf("%w: enrollment assertion principal does not match authenticated principal", protocol.ErrPolicyReevaluation)
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if existing, ok := t.bindings[body.Principal]; ok {
		if bytes.Equal(existing, body.PublicKey) {
			return nil
		}
		return fmt.Errorf("%w: principal already has a retained public key", protocol.ErrVerificationFailed)
	}
	t.bindings[body.Principal] = append([]byte(nil), body.PublicKey...)
	return nil
}

func enrollmentAssertion(body EnrollmentBody) (protocol.TypedAssertion, error) {
	encoded, err := encodeJSON(EnrollmentAssertion{
		Principal: body.Principal,
		PublicKey: append([]byte(nil), body.PublicKey...),
	})
	if err != nil {
		return protocol.TypedAssertion{}, err
	}
	return protocol.TypedAssertion{
		PredicateType: PredicateTypeEnrollmentV1,
		Bytes:         encoded,
	}, nil
}

// DecodeEnrollmentAssertion decodes a direct-key/enrollment/v1 assertion body.
func DecodeEnrollmentAssertion(assertion protocol.TypedAssertion) (EnrollmentAssertion, error) {
	if assertion.PredicateType != PredicateTypeEnrollmentV1 {
		return EnrollmentAssertion{}, fmt.Errorf("%w: %s", protocol.ErrUnknownPredicateType, assertion.PredicateType)
	}
	var body EnrollmentAssertion
	if err := decodeJSON(assertion.Bytes, &body); err != nil {
		return EnrollmentAssertion{}, err
	}
	principal, err := canonicalPrincipal(body.Principal)
	if err != nil {
		return EnrollmentAssertion{}, err
	}
	body.Principal = principal
	if len(body.PublicKey) == 0 {
		return EnrollmentAssertion{}, fmt.Errorf("%w: enrollment public key is required", protocol.ErrMalformedEvidence)
	}
	return body, nil
}

// PublicKey returns the retained public key for principal, if present.
func (t *Target) PublicKey(principal protocol.Principal) ([]byte, bool) {
	return t.lookup(principal)
}

func (t *Target) lookup(principal protocol.Principal) ([]byte, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key, ok := t.bindings[principal]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), key...), true
}

func parseEnrollment(evidence protocol.TypedEvidence) (EnrollmentBody, error) {
	if evidence.MediaType != MediaTypeEnrollment {
		return EnrollmentBody{}, fmt.Errorf("%w: %s", protocol.ErrUnknownMediaType, evidence.MediaType)
	}
	var body EnrollmentBody
	if err := decodeJSON(evidence.Bytes, &body); err != nil {
		return EnrollmentBody{}, err
	}
	principal, err := canonicalPrincipal(body.Principal)
	if err != nil {
		return EnrollmentBody{}, err
	}
	body.Principal = principal
	if len(body.PublicKey) == 0 || len(body.ProofOfPossession) == 0 {
		return EnrollmentBody{}, fmt.Errorf("%w: enrollment public key and proof of possession are required", protocol.ErrMalformedEvidence)
	}
	return body, nil
}

func parseSignature(evidence protocol.TypedEvidence) (SignatureBody, error) {
	if evidence.MediaType != MediaTypeSignature {
		return SignatureBody{}, fmt.Errorf("%w: %s", protocol.ErrUnknownMediaType, evidence.MediaType)
	}
	var body SignatureBody
	if err := decodeJSON(evidence.Bytes, &body); err != nil {
		return SignatureBody{}, err
	}
	principal, err := canonicalPrincipal(body.Principal)
	if err != nil {
		return SignatureBody{}, err
	}
	body.Principal = principal
	if body.Assertion.PredicateType == "" || len(body.Assertion.Bytes) == 0 || len(body.Signature) == 0 {
		return SignatureBody{}, fmt.Errorf("%w: signature body is missing required fields", protocol.ErrMalformedEvidence)
	}
	return body, nil
}
