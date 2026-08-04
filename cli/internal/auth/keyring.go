package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/zalando/go-keyring"
)

const keyringService = "fleetctl"

// Keyring entry keys. Each token field gets its own entry to stay within
// per-entry size limits imposed by the OS keychain (~3 KB on macOS).
const (
	keyAccess     = "access_token"
	keyRefresh    = "refresh_token"
	keyID         = "id_token"
	keyMeta       = "meta"
	keySigningKey = "signing_key"
)

// tokenMeta holds the small, non-JWT fields that are stored together.
type tokenMeta struct {
	Expiry    time.Time `json:"expiry"`
	TokenType string    `json:"token_type"`
}

// KeyringTokenStore persists tokens in the OS secure keychain.
// Each token field is stored as a separate keyring entry so that
// large JWTs don't exceed per-entry size limits.
type KeyringTokenStore struct{}

func (KeyringTokenStore) Save(_ context.Context, tokens Tokens) error {
	sets := []struct {
		key   string
		value string
	}{
		{keyAccess, tokens.AccessToken},
		{keyRefresh, tokens.RefreshToken},
		{keyID, tokens.IDToken},
	}
	for _, s := range sets {
		if s.value == "" {
			_ = keyring.Delete(keyringService, s.key)
			continue
		}
		if err := keyring.Set(keyringService, s.key, s.value); err != nil {
			return fmt.Errorf("save %s to keyring: %w", s.key, err)
		}
	}

	meta, err := json.Marshal(tokenMeta{
		Expiry:    tokens.Expiry,
		TokenType: tokens.TokenType,
	})
	if err != nil {
		return fmt.Errorf("marshal token metadata: %w", err)
	}
	if err := keyring.Set(keyringService, keyMeta, string(meta)); err != nil {
		return fmt.Errorf("save metadata to keyring: %w", err)
	}
	return nil
}

func (KeyringTokenStore) Load(_ context.Context) (Tokens, error) {
	access, err := keyring.Get(keyringService, keyAccess)
	if err != nil {
		return Tokens{}, fmt.Errorf("load access_token from keyring: %w", err)
	}

	rawMeta, err := keyring.Get(keyringService, keyMeta)
	if err != nil {
		return Tokens{}, fmt.Errorf("load metadata from keyring: %w", err)
	}
	var m tokenMeta
	if err := json.Unmarshal([]byte(rawMeta), &m); err != nil {
		return Tokens{}, fmt.Errorf("parse token metadata: %w", err)
	}

	tokens := Tokens{
		AccessToken: access,
		Expiry:      m.Expiry,
		TokenType:   m.TokenType,
	}

	if v, err := keyring.Get(keyringService, keyRefresh); err == nil {
		tokens.RefreshToken = v
	} else if !errors.Is(err, keyring.ErrNotFound) {
		return Tokens{}, fmt.Errorf("load refresh_token from keyring: %w", err)
	}

	if v, err := keyring.Get(keyringService, keyID); err == nil {
		tokens.IDToken = v
	} else if !errors.Is(err, keyring.ErrNotFound) {
		return Tokens{}, fmt.Errorf("load id_token from keyring: %w", err)
	}

	return tokens, nil
}

func (KeyringTokenStore) Clear(_ context.Context) error {
	if err := keyring.DeleteAll(keyringService); err != nil {
		return fmt.Errorf("clear keyring: %w", err)
	}
	return nil
}

// SaveSigningKey stores a PEM-encoded ECDSA private key in the keyring.
func SaveSigningKey(pemData string) error {
	if err := keyring.Set(keyringService, keySigningKey, pemData); err != nil {
		return fmt.Errorf("save signing key to keyring: %w", err)
	}
	return nil
}

// LoadSigningKey loads the PEM-encoded ECDSA private key from the keyring.
func LoadSigningKey() (string, error) {
	pem, err := keyring.Get(keyringService, keySigningKey)
	if err != nil {
		return "", fmt.Errorf("load signing key from keyring: %w", err)
	}
	return pem, nil
}
