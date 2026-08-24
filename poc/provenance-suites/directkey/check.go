package directkey

import (
	"bytes"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
)

// CheckDelivery is the resource manager's own provenance check. It uses the
// couriered enrollment map and is not a substitute for target verification.
func (m *Manager) CheckDelivery(evidence protocol.TypedEvidence) (protocol.TentativeHints, error) {
	if evidence.ProvenanceType != protocol.ProvenanceTypeDirectKeyV1 {
		return protocol.TentativeHints{}, fmt.Errorf("%w: %s", protocol.ErrUnknownProvenanceType, evidence.ProvenanceType)
	}
	body, err := parseSignature(evidence)
	if err != nil {
		return protocol.TentativeHints{}, err
	}
	contentDigest, err := body.Assertion.Digest()
	if err != nil {
		return protocol.TentativeHints{}, err
	}
	publicKey, ok := m.PublicKey(body.Principal)
	if !ok {
		return protocol.TentativeHints{}, fmt.Errorf("%w: no couriered public key for subject %q", protocol.ErrVerificationFailed, body.Principal.Subject)
	}
	if err := verify(publicKey, purposeAssertion, signatureMaterial(body.Principal, body.Assertion.PredicateType, contentDigest), body.Signature); err != nil {
		return protocol.TentativeHints{}, err
	}
	return principalHints(body.Principal, body.Assertion.PredicateType), nil
}

// DecodeAssertion implements protocol.ResourceManagerAPI. It unwraps the
// inner statement from direct-key evidence without authenticating it.
func (m *Manager) DecodeAssertion(evidence protocol.TypedEvidence) (protocol.TypedAssertion, error) {
	if evidence.ProvenanceType != protocol.ProvenanceTypeDirectKeyV1 {
		return protocol.TypedAssertion{}, fmt.Errorf("%w: %s", protocol.ErrUnknownProvenanceType, evidence.ProvenanceType)
	}
	body, err := parseSignature(evidence)
	if err != nil {
		return protocol.TypedAssertion{}, err
	}
	return protocol.TypedAssertion{
		PredicateType: body.Assertion.PredicateType,
		Bytes:         append([]byte(nil), body.Assertion.Bytes...),
	}, nil
}

// CheckEnrollment checks proof of possession using the public key carried in
// enrollment evidence. It is the RM's own decision, not target acceptance.
func (m *Manager) CheckEnrollment(evidence protocol.TypedEvidence) error {
	body, err := parseEnrollment(evidence)
	if err != nil {
		return err
	}
	if existing, ok := m.PublicKey(body.Principal); ok && !bytes.Equal(existing, body.PublicKey) {
		return fmt.Errorf("principal already has a different enrolled public key")
	}
	return verify(body.PublicKey, purposeEnrollmentProof, enrollmentProofMaterial(body.Principal, body.PublicKey), body.ProofOfPossession)
}
