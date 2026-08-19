package directkey

import (
	"crypto/ed25519"
	"encoding/json"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
)

const (
	// MediaTypeEnrollment is the enrollment representation that directly
	// shares the public key.
	MediaTypeEnrollment protocol.MediaType = "application/vnd.fleetshift.direct-key.enrollment.v1+json"

	// MediaTypeSignature is the delivery representation: a signature and
	// user reference, not a public key.
	MediaTypeSignature protocol.MediaType = "application/vnd.fleetshift.direct-key.signature.v1+json"

	purposeEnrollmentProof protocol.Purpose = "fleetshift.dev/provenance/direct-key/v1/enrollment-proof"
	purposeAssertion       protocol.Purpose = "fleetshift.dev/provenance/direct-key/v1/assertion"
)

// EnrollmentBody is the type-specific encoding of enrollment evidence.
type EnrollmentBody struct {
	Principal         protocol.Principal `json:"principal"`
	PublicKey         []byte             `json:"public_key"`
	ProofOfPossession []byte             `json:"proof_of_possession"`
}

// SignatureBody is the type-specific encoding of delivery evidence. The
// public key is intentionally absent; the verifier uses its retained mapping.
type SignatureBody struct {
	Principal     protocol.Principal   `json:"principal"`
	ContentType   protocol.ContentType `json:"content_type"`
	ContentDigest protocol.Digest      `json:"content_digest"`
	Signature     []byte               `json:"signature"`
}

func encodeJSON(value any) ([]byte, error) {
	return protocol.MarshalCanonical(value)
}

func decodeJSON(raw []byte, dest any) error {
	if err := json.Unmarshal(raw, dest); err != nil {
		return fmt.Errorf("%w: %v", protocol.ErrMalformedEvidence, err)
	}
	return nil
}

func sign(privateKey ed25519.PrivateKey, purpose protocol.Purpose, value any) ([]byte, error) {
	payload, err := signaturePayload(purpose, value)
	if err != nil {
		return nil, err
	}
	return ed25519.Sign(privateKey, payload), nil
}

func verify(publicKey []byte, purpose protocol.Purpose, value any, signature []byte) error {
	if len(publicKey) != ed25519.PublicKeySize {
		return fmt.Errorf("%w: invalid Ed25519 public key length %d", protocol.ErrVerificationFailed, len(publicKey))
	}
	payload, err := signaturePayload(purpose, value)
	if err != nil {
		return err
	}
	if !ed25519.Verify(ed25519.PublicKey(publicKey), payload, signature) {
		return fmt.Errorf("%w: Ed25519 signature verification failed", protocol.ErrVerificationFailed)
	}
	return nil
}

func signaturePayload(purpose protocol.Purpose, value any) ([]byte, error) {
	encoded, err := protocol.MarshalCanonical(value)
	if err != nil {
		return nil, fmt.Errorf("encode signed object: %w", err)
	}
	prefix := append([]byte(purpose), 0)
	return append(prefix, encoded...), nil
}

func canonicalPrincipal(p protocol.Principal) (protocol.Principal, error) {
	canonical, err := p.Canonicalize()
	if err != nil {
		return protocol.Principal{}, fmt.Errorf("%w: %v", protocol.ErrMalformedEvidence, err)
	}
	return canonical, nil
}
