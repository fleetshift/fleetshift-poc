package directkey

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
)

// Target is the direct-key/v1 target API. It retains a public-key and user
// mapping established by enrollment and verifies delivery signatures against
// that mapping only.
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

// ParseHints extracts tentative principal fields from untrusted evidence.
func ParseHints(evidence protocol.TypedEvidence) (protocol.TentativeHints, error) {
	return NewTarget().ParseHints(evidence)
}

// ParseHints implements protocol.TargetAPI.
func (t *Target) ParseHints(evidence protocol.TypedEvidence) (protocol.TentativeHints, error) {
	if evidence.ProvenanceType != protocol.ProvenanceTypeDirectKeyV1 {
		return protocol.TentativeHints{}, fmt.Errorf("%w: %s", protocol.ErrUnknownProvenanceType, evidence.ProvenanceType)
	}
	principal, err := parsePrincipalHint(evidence)
	if err != nil {
		return protocol.TentativeHints{}, err
	}
	return protocol.TentativeHints{
		Scheme:          principal.Scheme,
		Authority:       principal.Authority,
		TenantPartition: principal.TenantPartition,
		Subject:         principal.Subject,
	}, nil
}

// Verify implements protocol.TargetAPI. The verifying key is taken from
// retained enrollment state, never from the delivery or from support material.
func (t *Target) Verify(_ context.Context, req protocol.VerifyRequest) (protocol.AuthenticatedEvidence, error) {
	if req.ProfileConfig.ProvenanceType != protocol.ProvenanceTypeDirectKeyV1 {
		return protocol.AuthenticatedEvidence{}, fmt.Errorf("%w: %s", protocol.ErrUnknownProvenanceType, req.ProfileConfig.ProvenanceType)
	}
	if len(req.ProfileConfig.Parameters) != 0 {
		return protocol.AuthenticatedEvidence{}, fmt.Errorf("%w: direct-key/v1 has no profile parameters", protocol.ErrUnknownProvenanceType)
	}
	if req.Evidence.ProvenanceType != protocol.ProvenanceTypeDirectKeyV1 {
		return protocol.AuthenticatedEvidence{}, fmt.Errorf("%w: %s", protocol.ErrUnknownProvenanceType, req.Evidence.ProvenanceType)
	}
	if req.Evidence.MediaType != MediaTypeSignature {
		return protocol.AuthenticatedEvidence{}, fmt.Errorf("%w: %s", protocol.ErrUnknownMediaType, req.Evidence.MediaType)
	}
	body, err := parseSignature(req.Evidence)
	if err != nil {
		return protocol.AuthenticatedEvidence{}, err
	}
	if req.AuthorityConfig.PrincipalAuthority != body.Principal.PrincipalAuthority() {
		return protocol.AuthenticatedEvidence{}, fmt.Errorf("%w: authority config does not match authenticated principal", protocol.ErrUnknownAuthority)
	}
	publicKey, ok := t.lookup(body.Principal)
	if !ok {
		return protocol.AuthenticatedEvidence{}, fmt.Errorf("%w: no retained public key for subject %q", protocol.ErrVerificationFailed, body.Principal.Subject)
	}
	if err := verify(publicKey, purposeAssertion, signatureMaterial(body.Principal, body.ContentType, body.ContentDigest), body.Signature); err != nil {
		return protocol.AuthenticatedEvidence{}, err
	}

	mapped, err := req.AuthorityConfig.TenantMapping.Map(body.Principal.TenantPartition)
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
	return protocol.AuthenticatedEvidence{
		Principal:              body.Principal,
		MappedFleetShiftTenant: mapped,
		ContentType:            body.ContentType,
		ContentDigest:          body.ContentDigest,
		ProvenanceType:         protocol.ProvenanceTypeDirectKeyV1,
		AuthorityConfigDigest:  authorityDigest,
		ProfileConfigDigest:    profileDigest,
	}, nil
}

// AcceptEnrollment is the typed lifecycle operation that stores the public
// key and user mapping. The public key is taken from enrollment evidence, not
// from an RM-supplied profile ID.
//
// First acceptance for a principal is trust-on-first-use: this profile does
// not verify an issuer assertion that the claimant is that subject. A
// compromised resource manager can win the first bind. Later substitution of
// an established mapping is rejected.
func (t *Target) AcceptEnrollment(evidence protocol.TypedEvidence, authority protocol.AuthorityConfig) error {
	if evidence.ProvenanceType != protocol.ProvenanceTypeDirectKeyV1 {
		return fmt.Errorf("%w: %s", protocol.ErrUnknownProvenanceType, evidence.ProvenanceType)
	}
	if evidence.MediaType != MediaTypeEnrollment {
		return fmt.Errorf("%w: %s", protocol.ErrUnknownMediaType, evidence.MediaType)
	}
	if !authorityHasProfile(authority, protocol.ProvenanceTypeDirectKeyV1) {
		return fmt.Errorf("%w: authority does not install %s", protocol.ErrUnknownProvenanceType, protocol.ProvenanceTypeDirectKeyV1)
	}
	body, err := parseEnrollment(evidence)
	if err != nil {
		return err
	}
	if body.Principal.PrincipalAuthority() != authority.PrincipalAuthority {
		return fmt.Errorf("%w: enrollment principal is not under this authority", protocol.ErrUnknownAuthority)
	}
	if err := verify(body.PublicKey, purposeEnrollmentProof, enrollmentProofMaterial(body.Principal, body.PublicKey), body.ProofOfPossession); err != nil {
		return fmt.Errorf("%w: enrollment proof of possession: %v", protocol.ErrVerificationFailed, err)
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

func parsePrincipalHint(evidence protocol.TypedEvidence) (protocol.Principal, error) {
	switch evidence.MediaType {
	case MediaTypeEnrollment:
		body, err := parseEnrollment(evidence)
		if err != nil {
			return protocol.Principal{}, err
		}
		return body.Principal, nil
	case MediaTypeSignature:
		body, err := parseSignature(evidence)
		if err != nil {
			return protocol.Principal{}, err
		}
		return body.Principal, nil
	default:
		return protocol.Principal{}, fmt.Errorf("%w: %s", protocol.ErrUnknownMediaType, evidence.MediaType)
	}
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
	if body.ContentType == "" || body.ContentDigest == "" || len(body.Signature) == 0 {
		return SignatureBody{}, fmt.Errorf("%w: signature body is missing required fields", protocol.ErrMalformedEvidence)
	}
	if _, err := protocol.DecodeDigest(body.ContentDigest); err != nil {
		return SignatureBody{}, fmt.Errorf("%w: content digest: %v", protocol.ErrMalformedEvidence, err)
	}
	return body, nil
}

func authorityHasProfile(authority protocol.AuthorityConfig, pt protocol.ProvenanceType) bool {
	for _, profile := range authority.ProvenanceProfiles {
		if profile.ProvenanceType == pt && len(profile.Parameters) == 0 {
			return true
		}
	}
	return false
}
