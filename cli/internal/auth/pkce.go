package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
)

// PKCEChallenge holds the code verifier and challenge for the
// OAuth2 PKCE extension (RFC 7636).
type PKCEChallenge struct {
	Verifier        string
	Challenge       string
	ChallengeMethod string
}

// GeneratePKCE generates a PKCE code verifier and S256 challenge.
func GeneratePKCE() (PKCEChallenge, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return PKCEChallenge{}, err
	}
	verifier := base64.RawURLEncoding.EncodeToString(buf)

	h := sha256.Sum256([]byte(verifier))
	challenge := base64.RawURLEncoding.EncodeToString(h[:])

	return PKCEChallenge{
		Verifier:        verifier,
		Challenge:       challenge,
		ChallengeMethod: "S256",
	}, nil
}
