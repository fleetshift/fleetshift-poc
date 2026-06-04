package ocp

import (
	"strings"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwt"
)

func TestCallbackTokenSigner_RoundTrip(t *testing.T) {
	signer, err := NewCallbackTokenSigner()
	if err != nil {
		t.Fatalf("NewCallbackTokenSigner: %v", err)
	}

	const clusterID = "test-cluster-123"
	token, err := signer.Sign(clusterID, 5*time.Minute)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	got, err := signer.Verify(token)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if got != clusterID {
		t.Fatalf("subject mismatch: got %q, want %q", got, clusterID)
	}
}

func TestCallbackTokenSigner_ExpiredToken(t *testing.T) {
	signer, err := NewCallbackTokenSigner()
	if err != nil {
		t.Fatalf("NewCallbackTokenSigner: %v", err)
	}

	token, err := signer.Sign("expired-cluster", -1*time.Second)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	_, err = signer.Verify(token)
	if err == nil {
		t.Fatal("expected error for expired token, got nil")
	}
}

func TestCallbackTokenSigner_WrongAudience(t *testing.T) {
	signer, err := NewCallbackTokenSigner()
	if err != nil {
		t.Fatalf("NewCallbackTokenSigner: %v", err)
	}

	token := signWithAudience(t, signer, "aud-cluster", "wrong-audience", 5*time.Minute)

	_, err = signer.Verify(token)
	if err == nil {
		t.Fatal("expected error for wrong audience, got nil")
	}
}

func TestCallbackTokenSigner_DifferentSignerRejects(t *testing.T) {
	signer1, err := NewCallbackTokenSigner()
	if err != nil {
		t.Fatalf("NewCallbackTokenSigner signer1: %v", err)
	}
	signer2, err := NewCallbackTokenSigner()
	if err != nil {
		t.Fatalf("NewCallbackTokenSigner signer2: %v", err)
	}

	token, err := signer1.Sign("cross-cluster", 5*time.Minute)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	_, err = signer2.Verify(token)
	if err == nil {
		t.Fatal("expected error when verifying with different signer, got nil")
	}
}

// signWithAudience is a test helper that signs a token with a custom audience
// instead of the default callbackAudience. This enables wrong-audience tests.
func signWithAudience(t *testing.T, s *CallbackTokenSigner, clusterID, audience string, duration time.Duration) string {
	t.Helper()

	tok, err := jwt.NewBuilder().
		Subject(clusterID).
		Audience([]string{audience}).
		IssuedAt(time.Now()).
		Expiration(time.Now().Add(duration)).
		Build()
	if err != nil {
		t.Fatalf("signWithAudience: build token: %v", err)
	}

	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.EdDSA(), s.privKey))
	if err != nil {
		t.Fatalf("signWithAudience: sign token: %v", err)
	}

	return string(signed)
}

func TestCallbackTokenSigner_TamperedToken(t *testing.T) {
	signer, err := NewCallbackTokenSigner()
	if err != nil {
		t.Fatalf("NewCallbackTokenSigner: %v", err)
	}

	token, err := signer.Sign("tamper-cluster", 5*time.Minute)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	// Replace the signature segment with garbage to ensure verification fails.
	// A JWT has three dot-separated segments: header.payload.signature
	parts := strings.SplitN(token, ".", 3)
	if len(parts) != 3 {
		t.Fatalf("token doesn't have 3 parts: %q", token)
	}
	tampered := parts[0] + "." + parts[1] + ".INVALIDSIGNATURE"

	_, err = signer.Verify(tampered)
	if err == nil {
		t.Fatal("expected error for tampered token, got nil")
	}
}
