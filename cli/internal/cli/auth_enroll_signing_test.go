package cli

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/zalando/go-keyring"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
)

func TestAcquireSigningKey_Generate(t *testing.T) {
	keyring.MockInit()

	key, generated, err := acquireSigningKey(false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !generated {
		t.Error("expected generated=true for a fresh key")
	}
	if key == nil {
		t.Fatal("expected non-nil key")
	}
	if key.Curve != elliptic.P256() {
		t.Errorf("expected P-256 curve, got %v", key.Curve.Params().Name)
	}
}

func TestAcquireSigningKey_ReuseExisting(t *testing.T) {
	keyring.MockInit()

	original, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	der, err := x509.MarshalECPrivateKey(original)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	pemBlock := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der})
	if err := auth.SaveSigningKey(string(pemBlock)); err != nil {
		t.Fatalf("save signing key: %v", err)
	}

	key, generated, err := acquireSigningKey(true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if generated {
		t.Error("expected generated=false when reusing")
	}
	if !original.Equal(key) {
		t.Error("reused key should match the original")
	}
}

func TestAcquireSigningKey_ReuseNoKey(t *testing.T) {
	keyring.MockInit()

	_, _, err := acquireSigningKey(true)
	if err == nil {
		t.Fatal("expected error when reusing a non-existent key")
	}
}
