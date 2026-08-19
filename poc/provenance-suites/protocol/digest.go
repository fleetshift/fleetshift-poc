package protocol

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// Digest is a canonical SHA-256 digest in the form "sha256:" + lowercase hex.
type Digest string

// Purpose is a domain-separation tag for digests and signatures.
type Purpose string

const (
	purposeTypedEvidenceIdentity Purpose = "fleetshift.dev/provenance/typed-evidence-identity/v1"
	purposeContentDigest         Purpose = "fleetshift.dev/provenance/content-digest/v1"
	purposeAuthorityConfig       Purpose = "fleetshift.dev/provenance/authority-config/v1"
	purposeProfileConfig         Purpose = "fleetshift.dev/provenance/profile-config/v1"
	purposeTrustConfiguration    Purpose = "fleetshift.dev/provenance/trust-configuration/v1"
	purposeDeliveryCommitment    Purpose = "fleetshift.dev/provenance/delivery-commitment/v1"
)

// DigestBytes returns the canonical digest of raw bytes with no extra wrapping.
func DigestBytes(value []byte) Digest {
	sum := sha256.Sum256(value)
	return encodeDigest(sum[:])
}

// DigestObject returns a domain-separated digest of a JSON-encoded value.
func DigestObject(purpose Purpose, value any) (Digest, error) {
	encoded, err := canonicalJSON(purpose, value)
	if err != nil {
		return "", fmt.Errorf("encode digest object: %w", err)
	}
	return DigestBytes(encoded), nil
}

func canonicalJSON(purpose Purpose, value any) ([]byte, error) {
	material := struct {
		Purpose Purpose `json:"purpose"`
		Value   any     `json:"value"`
	}{Purpose: purpose, Value: value}
	return json.Marshal(material)
}

// MarshalCanonical returns the deterministic JSON encoding of value.
func MarshalCanonical(value any) ([]byte, error) {
	return json.Marshal(value)
}

// DecodeDigest checks that encoded is a canonical SHA-256 digest.
func DecodeDigest(encoded Digest) ([]byte, error) {
	return decodeDigest(encoded)
}

func decodeDigest(encoded Digest) ([]byte, error) {
	s := string(encoded)
	if len(s) != len("sha256:")+sha256.Size*2 || !strings.HasPrefix(s, "sha256:") {
		return nil, fmt.Errorf("digest %q is not a SHA-256 digest", encoded)
	}
	hash, err := hex.DecodeString(strings.TrimPrefix(s, "sha256:"))
	if err != nil {
		return nil, fmt.Errorf("decode SHA-256 digest: %w", err)
	}
	if encodeDigest(hash) != encoded {
		return nil, fmt.Errorf("digest %q is not in canonical lowercase form", encoded)
	}
	return hash, nil
}

func encodeDigest(hash []byte) Digest {
	return Digest("sha256:" + hex.EncodeToString(hash))
}
