package keyregistry

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/x509"
	"encoding/base64"
	"fmt"
)

// ParsePublicKeyFromBase64 decodes a base64-encoded PKIX/SPKI DER
// public key and validates it is ECDSA P-256. Use this after
// evaluating a CEL expression that extracts the key claim value.
func ParsePublicKeyFromBase64(base64Key string) ([]crypto.PublicKey, error) {
	derBytes, err := base64.StdEncoding.DecodeString(base64Key)
	if err != nil {
		return nil, fmt.Errorf("decode base64 public key: %w", err)
	}

	pub, err := x509.ParsePKIXPublicKey(derBytes)
	if err != nil {
		return nil, fmt.Errorf("parse PKIX public key: %w", err)
	}

	ecPub, ok := pub.(*ecdsa.PublicKey)
	if !ok || ecPub.Curve != elliptic.P256() {
		return nil, fmt.Errorf("public key is not ECDSA P-256")
	}

	return []crypto.PublicKey{ecPub}, nil
}
