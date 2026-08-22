package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const (
	fileCredentialsName = "credentials.json"
	fileSigningKeyName  = "signing_key.pem"
)

// FileStore persists OAuth tokens and the signing key as files under Dir.
// Dir must be an absolute directory owned by the caller; the store never reads
// or writes the user home, XDG paths, or the OS keyring.
type FileStore struct {
	Dir string
}

// credentialsPath returns Dir/credentials.json.
func (s FileStore) credentialsPath() string {
	return filepath.Join(s.Dir, fileCredentialsName)
}

// signingKeyPath returns Dir/signing_key.pem.
func (s FileStore) signingKeyPath() string {
	return filepath.Join(s.Dir, fileSigningKeyName)
}

// Save writes tokens to Dir/credentials.json at mode 0600.
func (s FileStore) Save(_ context.Context, tokens Tokens) error {
	if err := s.ensureDir(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(tokens, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal tokens: %w", err)
	}
	if err := os.WriteFile(s.credentialsPath(), data, 0o600); err != nil {
		return fmt.Errorf("write tokens: %w", err)
	}
	return nil
}

// Load reads tokens from Dir/credentials.json.
func (s FileStore) Load(_ context.Context) (Tokens, error) {
	data, err := os.ReadFile(s.credentialsPath())
	if err != nil {
		return Tokens{}, fmt.Errorf("load tokens: %w", err)
	}
	var tokens Tokens
	if err := json.Unmarshal(data, &tokens); err != nil {
		return Tokens{}, fmt.Errorf("parse tokens: %w", err)
	}
	return tokens, nil
}

// Clear removes the credentials file. Missing files are not an error.
func (s FileStore) Clear(_ context.Context) error {
	if err := os.Remove(s.credentialsPath()); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("clear tokens: %w", err)
	}
	return nil
}

// SaveSigningKey writes pemData to Dir/signing_key.pem at mode 0600. It does not parse PEM.
func (s FileStore) SaveSigningKey(pemData string) error {
	if err := s.ensureDir(); err != nil {
		return err
	}
	if err := os.WriteFile(s.signingKeyPath(), []byte(pemData), 0o600); err != nil {
		return fmt.Errorf("save signing key: %w", err)
	}
	return nil
}

// LoadSigningKey reads Dir/signing_key.pem. It does not parse or validate PEM.
func (s FileStore) LoadSigningKey() (string, error) {
	pem, err := os.ReadFile(s.signingKeyPath())
	if err != nil {
		return "", fmt.Errorf("load signing key: %w", err)
	}
	return string(pem), nil
}

// ensureDir creates Dir at mode 0700. It fails when Dir is empty.
func (s FileStore) ensureDir() error {
	if s.Dir == "" {
		return fmt.Errorf("file store requires a config directory")
	}
	if err := os.MkdirAll(s.Dir, 0o700); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}
	return nil
}
