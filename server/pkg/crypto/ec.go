// Package crypto provides shared EC cryptographic helpers used by both
// the application layer (provenance construction, key enrollment) and
// the attestation verification library.
package crypto

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
)

// ECJWK is the minimal JWK representation for an EC public key.
type ECJWK struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

// ParseECPublicKeyFromJWK parses a P-256 public key from its JWK
// JSON representation.
func ParseECPublicKeyFromJWK(raw json.RawMessage) (*ecdsa.PublicKey, error) {
	var jwk ECJWK
	if err := json.Unmarshal(raw, &jwk); err != nil {
		return nil, fmt.Errorf("unmarshal JWK: %w", err)
	}
	if jwk.Kty != "EC" {
		return nil, fmt.Errorf("unsupported key type: %s", jwk.Kty)
	}
	if jwk.Crv != "P-256" {
		return nil, fmt.Errorf("unsupported curve: %s", jwk.Crv)
	}

	xBytes, err := Base64URLDecode(jwk.X)
	if err != nil {
		return nil, fmt.Errorf("decode x: %w", err)
	}
	yBytes, err := Base64URLDecode(jwk.Y)
	if err != nil {
		return nil, fmt.Errorf("decode y: %w", err)
	}

	return &ecdsa.PublicKey{
		Curve: elliptic.P256(),
		X:     new(big.Int).SetBytes(xBytes),
		Y:     new(big.Int).SetBytes(yBytes),
	}, nil
}

// VerifyECDSASignature verifies an ECDSA-P256-SHA256 signature over
// raw document bytes.
func VerifyECDSASignature(pub *ecdsa.PublicKey, doc, sig []byte) error {
	hash := sha256.Sum256(doc)
	if !ecdsa.VerifyASN1(pub, hash[:], sig) {
		return fmt.Errorf("ECDSA signature verification failed")
	}
	return nil
}

// Base64URLDecode decodes a base64url-encoded string (no padding).
func Base64URLDecode(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(s)
}
