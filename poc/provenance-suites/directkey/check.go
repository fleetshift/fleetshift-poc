package directkey

import (
	"bytes"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
)

// CheckDelivery is the resource manager's own provenance check. It uses the
// couriered enrollment map and is not a substitute for target verification.
func (m *Manager) CheckDelivery(evidence protocol.TypedEvidence, assertion protocol.TypedAssertion) error {
	if evidence.ProvenanceType != protocol.ProvenanceTypeDirectKeyV1 {
		return fmt.Errorf("%w: %s", protocol.ErrUnknownProvenanceType, evidence.ProvenanceType)
	}
	body, err := parseSignature(evidence)
	if err != nil {
		return err
	}
	contentDigest, err := assertion.Digest()
	if err != nil {
		return err
	}
	if body.ContentType != assertion.ContentType || body.ContentDigest != contentDigest {
		return fmt.Errorf("%w: evidence does not authenticate the supplied assertion", protocol.ErrVerificationFailed)
	}
	publicKey, ok := m.PublicKey(body.Principal)
	if !ok {
		return fmt.Errorf("%w: no couriered public key for subject %q", protocol.ErrVerificationFailed, body.Principal.Subject)
	}
	return verify(publicKey, purposeAssertion, signatureMaterial(body.Principal, body.ContentType, body.ContentDigest), body.Signature)
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
