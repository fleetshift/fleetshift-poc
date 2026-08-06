package auth

import (
	"crypto/sha256"
	"encoding/base64"
	"testing"
)

func TestGeneratePKCE_NoError(t *testing.T) {
	challenge, err := GeneratePKCE()
	if err != nil {
		t.Fatalf("GeneratePKCE: unexpected error: %v", err)
	}
	_ = challenge // use to avoid unused variable
}

func TestGeneratePKCE_VerifierNonEmptyAndReasonableLength(t *testing.T) {
	challenge, err := GeneratePKCE()
	if err != nil {
		t.Fatalf("GeneratePKCE: %v", err)
	}
	if challenge.Verifier == "" {
		t.Error("Verifier is empty, want non-empty")
	}
	// Base64url of 32 bytes = 43 chars (no padding)
	if len(challenge.Verifier) < 43 {
		t.Errorf("Verifier length = %d, want >= 43 (base64url of 32 bytes)", len(challenge.Verifier))
	}
}

func TestGeneratePKCE_ChallengeNonEmpty(t *testing.T) {
	challenge, err := GeneratePKCE()
	if err != nil {
		t.Fatalf("GeneratePKCE: %v", err)
	}
	if challenge.Challenge == "" {
		t.Error("Challenge is empty, want non-empty")
	}
}

func TestGeneratePKCE_ChallengeMethodIsS256(t *testing.T) {
	challenge, err := GeneratePKCE()
	if err != nil {
		t.Fatalf("GeneratePKCE: %v", err)
	}
	if challenge.ChallengeMethod != "S256" {
		t.Errorf("ChallengeMethod = %q, want %q", challenge.ChallengeMethod, "S256")
	}
}

func TestGeneratePKCE_ChallengeMatchesSHA256OfVerifier(t *testing.T) {
	challenge, err := GeneratePKCE()
	if err != nil {
		t.Fatalf("GeneratePKCE: %v", err)
	}
	h := sha256.Sum256([]byte(challenge.Verifier))
	expected := base64.RawURLEncoding.EncodeToString(h[:])
	if challenge.Challenge != expected {
		t.Errorf("Challenge = %q, want SHA256(verifier) base64url = %q", challenge.Challenge, expected)
	}
}

func TestGeneratePKCE_TwoCallsProduceDifferentVerifiers(t *testing.T) {
	c1, err := GeneratePKCE()
	if err != nil {
		t.Fatalf("GeneratePKCE (first): %v", err)
	}
	c2, err := GeneratePKCE()
	if err != nil {
		t.Fatalf("GeneratePKCE (second): %v", err)
	}
	if c1.Verifier == c2.Verifier {
		t.Error("two calls produced same verifier, want different (randomness check)")
	}
	if c1.Challenge == c2.Challenge {
		t.Error("two calls produced same challenge, want different")
	}
}
