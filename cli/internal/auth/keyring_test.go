package auth_test

import (
	"context"
	"testing"
	"time"

	"github.com/zalando/go-keyring"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
)

func TestKeyringStore_SaveAndLoad(t *testing.T) {
	keyring.MockInit()

	ctx := context.Background()
	store := auth.KeyringStore{}

	tokens := auth.Tokens{
		AccessToken:  "access-123",
		RefreshToken: "refresh-456",
		IDToken:      "id-token-789",
		Expiry:       time.Date(2026, 3, 16, 12, 0, 0, 0, time.UTC),
		TokenType:    "Bearer",
	}

	if err := store.Save(ctx, tokens); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if loaded.AccessToken != tokens.AccessToken {
		t.Errorf("AccessToken: got %q, want %q", loaded.AccessToken, tokens.AccessToken)
	}
	if loaded.RefreshToken != tokens.RefreshToken {
		t.Errorf("RefreshToken: got %q, want %q", loaded.RefreshToken, tokens.RefreshToken)
	}
	if loaded.IDToken != tokens.IDToken {
		t.Errorf("IDToken: got %q, want %q", loaded.IDToken, tokens.IDToken)
	}
	if loaded.TokenType != tokens.TokenType {
		t.Errorf("TokenType: got %q, want %q", loaded.TokenType, tokens.TokenType)
	}
	if !loaded.Expiry.Equal(tokens.Expiry) {
		t.Errorf("Expiry: got %v, want %v", loaded.Expiry, tokens.Expiry)
	}
}

func TestKeyringStore_OptionalFieldsOmitted(t *testing.T) {
	keyring.MockInit()

	ctx := context.Background()
	store := auth.KeyringStore{}

	tokens := auth.Tokens{
		AccessToken: "access-only",
		Expiry:      time.Date(2026, 3, 16, 12, 0, 0, 0, time.UTC),
		TokenType:   "Bearer",
	}

	if err := store.Save(ctx, tokens); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if loaded.AccessToken != tokens.AccessToken {
		t.Errorf("AccessToken: got %q, want %q", loaded.AccessToken, tokens.AccessToken)
	}
	if loaded.RefreshToken != "" {
		t.Errorf("RefreshToken: got %q, want empty", loaded.RefreshToken)
	}
	if loaded.IDToken != "" {
		t.Errorf("IDToken: got %q, want empty", loaded.IDToken)
	}
}

func TestKeyringStore_LoadEmpty(t *testing.T) {
	keyring.MockInit()

	ctx := context.Background()
	store := auth.KeyringStore{}

	_, err := store.Load(ctx)
	if err == nil {
		t.Fatal("Load: expected error for empty store, got nil")
	}
}

func TestKeyringStore_Clear(t *testing.T) {
	keyring.MockInit()

	ctx := context.Background()
	store := auth.KeyringStore{}

	tokens := auth.Tokens{
		AccessToken: "access-123",
		Expiry:      time.Date(2026, 3, 16, 12, 0, 0, 0, time.UTC),
		TokenType:   "Bearer",
	}

	if err := store.Save(ctx, tokens); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if err := store.Clear(ctx); err != nil {
		t.Fatalf("Clear: %v", err)
	}

	_, err := store.Load(ctx)
	if err == nil {
		t.Fatal("Load after Clear: expected error, got nil")
	}
	if err := store.Clear(ctx); err != nil {
		t.Fatalf("Clear missing keys: %v", err)
	}
}

func TestKeyringStore_Overwrite(t *testing.T) {
	keyring.MockInit()

	ctx := context.Background()
	store := auth.KeyringStore{}

	first := auth.Tokens{
		AccessToken:  "first-access",
		RefreshToken: "first-refresh",
		IDToken:      "first-id",
		Expiry:       time.Date(2026, 3, 16, 12, 0, 0, 0, time.UTC),
		TokenType:    "Bearer",
	}
	second := auth.Tokens{
		AccessToken: "second-access",
		Expiry:      time.Date(2026, 3, 16, 14, 0, 0, 0, time.UTC),
		TokenType:   "Bearer",
	}

	if err := store.Save(ctx, first); err != nil {
		t.Fatalf("Save first: %v", err)
	}
	if err := store.Save(ctx, second); err != nil {
		t.Fatalf("Save second: %v", err)
	}

	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if loaded.AccessToken != second.AccessToken {
		t.Errorf("AccessToken: got %q, want %q", loaded.AccessToken, second.AccessToken)
	}
	if loaded.RefreshToken != "" {
		t.Errorf("RefreshToken: got %q, want empty (cleared by second save)", loaded.RefreshToken)
	}
	if loaded.IDToken != "" {
		t.Errorf("IDToken: got %q, want empty (cleared by second save)", loaded.IDToken)
	}
	if !loaded.Expiry.Equal(second.Expiry) {
		t.Errorf("Expiry: got %v, want %v", loaded.Expiry, second.Expiry)
	}
}

func TestKeyringStore_SigningKeyRoundTrip(t *testing.T) {
	keyring.MockInit()
	store := auth.KeyringStore{}
	const pem = "-----BEGIN EC PRIVATE KEY-----\ntest\n-----END EC PRIVATE KEY-----\n"
	if err := store.SaveSigningKey(pem); err != nil {
		t.Fatalf("SaveSigningKey: %v", err)
	}
	got, err := store.LoadSigningKey()
	if err != nil {
		t.Fatalf("LoadSigningKey: %v", err)
	}
	if got != pem {
		t.Errorf("LoadSigningKey = %q, want %q", got, pem)
	}
}

func TestKeyringStore_LoadSigningKeyEmpty(t *testing.T) {
	keyring.MockInit()
	if _, err := (auth.KeyringStore{}).LoadSigningKey(); err == nil {
		t.Fatal("LoadSigningKey: expected error for empty store")
	}
}

func TestKeyringStore_ClearLeavesSigningKey(t *testing.T) {
	keyring.MockInit()
	ctx := context.Background()
	store := auth.KeyringStore{}
	const pem = "-----BEGIN EC PRIVATE KEY-----\ntest\n-----END EC PRIVATE KEY-----\n"
	if err := store.Save(ctx, auth.Tokens{AccessToken: "access-123", TokenType: "Bearer"}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := store.SaveSigningKey(pem); err != nil {
		t.Fatalf("SaveSigningKey: %v", err)
	}
	if err := store.Clear(ctx); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if _, err := store.Load(ctx); err == nil {
		t.Fatal("Load after Clear: expected error")
	}
	got, err := store.LoadSigningKey()
	if err != nil {
		t.Fatalf("LoadSigningKey after Clear: %v", err)
	}
	if got != pem {
		t.Fatalf("signing key after Clear = %q, want preserved PEM", got)
	}
}
