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
	if err := writePrivateFile(s.credentialsPath(), data); err != nil {
		return fmt.Errorf("write tokens: %w", err)
	}
	return nil
}

// Load reads tokens from Dir/credentials.json.
func (s FileStore) Load(_ context.Context) (Tokens, error) {
	if err := s.validateDir(); err != nil {
		return Tokens{}, err
	}
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
	if err := s.validateDir(); err != nil {
		return err
	}
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
	if err := writePrivateFile(s.signingKeyPath(), []byte(pemData)); err != nil {
		return fmt.Errorf("save signing key: %w", err)
	}
	return nil
}

// LoadSigningKey reads Dir/signing_key.pem. It does not parse or validate PEM.
func (s FileStore) LoadSigningKey() (string, error) {
	if err := s.validateDir(); err != nil {
		return "", err
	}
	pem, err := os.ReadFile(s.signingKeyPath())
	if err != nil {
		return "", fmt.Errorf("load signing key: %w", err)
	}
	return string(pem), nil
}

// writePrivateFile writes data to path at mode 0600. It refuses to write if
// path already exists and is not a regular file (Lstat), so a planted symlink
// is not followed. Chmod tightens an existing regular file that was more
// permissive.
func writePrivateFile(path string, data []byte) error {
	info, err := os.Lstat(path)
	if err == nil && !info.Mode().IsRegular() {
		return fmt.Errorf("not a regular file")
	}
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return err
	}
	return os.Chmod(path, 0o600)
}

// validateDir checks that Dir is a non-empty absolute path. If Dir exists, it
// must be a directory with no group or other write bits. Lstat is used so a
// symlink is not followed and fails the directory check. Missing Dir is
// allowed; ensureDir creates it.
func (s FileStore) validateDir() error {
	if s.Dir == "" {
		return fmt.Errorf("file store requires a config directory")
	}
	if !filepath.IsAbs(s.Dir) {
		return fmt.Errorf("file store requires an absolute config directory")
	}
	info, err := os.Lstat(s.Dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat config dir: %w", err)
	}
	return checkStateDir(info)
}

// checkStateDir reports whether info is a directory without group or other
// write permission.
func checkStateDir(info os.FileInfo) error {
	if !info.IsDir() {
		return fmt.Errorf("file store config directory is not a directory")
	}
	if info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("file store config directory must not be group- or world-writable")
	}
	return nil
}

// ensureDir creates Dir at mode 0700 after validateDir succeeds, then Lstats
// the result so a swapped-in symlink fails the directory check.
func (s FileStore) ensureDir() error {
	if err := s.validateDir(); err != nil {
		return err
	}
	if err := os.MkdirAll(s.Dir, 0o700); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}
	info, err := os.Lstat(s.Dir)
	if err != nil {
		return fmt.Errorf("stat config dir: %w", err)
	}
	return checkStateDir(info)
}
