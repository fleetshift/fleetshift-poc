package ocp

import (
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jwt"
)

const callbackAudience = "fleetshift-callback"

// CallbackTokenSigner generates and verifies short-lived JWTs for
// authenticating ocp-engine callback RPCs. An ephemeral ED25519 key pair
// is created at construction time and held in memory only.
type CallbackTokenSigner struct {
	privKey jwk.Key
	pubKey  jwk.Key
}

// NewCallbackTokenSigner creates a new signer with a freshly generated
// ED25519 key pair.
func NewCallbackTokenSigner() (*CallbackTokenSigner, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate ed25519 key: %w", err)
	}

	privKey, err := jwk.Import(priv)
	if err != nil {
		return nil, fmt.Errorf("import private key: %w", err)
	}

	pubKey, err := jwk.Import(pub)
	if err != nil {
		return nil, fmt.Errorf("import public key: %w", err)
	}

	return &CallbackTokenSigner{
		privKey: privKey,
		pubKey:  pubKey,
	}, nil
}

// Sign creates a signed JWT with the given clusterID as subject and the
// specified duration until expiry.
func (s *CallbackTokenSigner) Sign(clusterID string, duration time.Duration) (string, error) {
	tok, err := jwt.NewBuilder().
		Subject(clusterID).
		Audience([]string{callbackAudience}).
		IssuedAt(time.Now()).
		Expiration(time.Now().Add(duration)).
		Build()
	if err != nil {
		return "", fmt.Errorf("build token: %w", err)
	}

	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.EdDSA(), s.privKey))
	if err != nil {
		return "", fmt.Errorf("sign token: %w", err)
	}

	return string(signed), nil
}

// Verify parses and validates the token string, checking the signature,
// expiry, and audience. It returns the subject (cluster ID) on success.
func (s *CallbackTokenSigner) Verify(tokenString string) (string, error) {
	tok, err := jwt.ParseString(
		tokenString,
		jwt.WithKey(jwa.EdDSA(), s.pubKey),
		jwt.WithValidate(true),
		jwt.WithAudience(callbackAudience),
	)
	if err != nil {
		return "", fmt.Errorf("parse/validate token: %w", err)
	}

	sub, ok := tok.Subject()
	if !ok {
		return "", fmt.Errorf("token missing subject claim")
	}

	return sub, nil
}

