package auth_test

import (
	"context"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
)

func TestInMemoryTokenStore_SaveAndLoad(t *testing.T) {
	ctx := context.Background()
	store := &auth.InMemoryTokenStore{}

	tokens := auth.Tokens{
		AccessToken:  "access-123",
		RefreshToken: "refresh-456",
		IDToken:      "id-token-789",
		Expiry:       time.Now().Add(time.Hour),
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

func TestInMemoryTokenStore_LoadEmpty(t *testing.T) {
	ctx := context.Background()
	store := &auth.InMemoryTokenStore{}

	_, err := store.Load(ctx)
	if err == nil {
		t.Fatal("Load: expected error for empty store, got nil")
	}
}

func TestInMemoryTokenStore_Clear(t *testing.T) {
	ctx := context.Background()
	store := &auth.InMemoryTokenStore{}

	tokens := auth.Tokens{
		AccessToken: "access-123",
		Expiry:      time.Now().Add(time.Hour),
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
}

func TestInMemoryTokenStore_Overwrite(t *testing.T) {
	ctx := context.Background()
	store := &auth.InMemoryTokenStore{}

	first := auth.Tokens{
		AccessToken: "first-access",
		Expiry:      time.Now().Add(time.Hour),
		TokenType:   "Bearer",
	}
	second := auth.Tokens{
		AccessToken:  "second-access",
		RefreshToken: "second-refresh",
		Expiry:       time.Now().Add(2 * time.Hour),
		TokenType:    "Bearer",
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
	if loaded.RefreshToken != second.RefreshToken {
		t.Errorf("RefreshToken: got %q, want %q", loaded.RefreshToken, second.RefreshToken)
	}
	if !loaded.Expiry.Equal(second.Expiry) {
		t.Errorf("Expiry: got %v, want %v", loaded.Expiry, second.Expiry)
	}
}
