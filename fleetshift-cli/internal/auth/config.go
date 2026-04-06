package auth

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
)

// Config holds local OIDC client configuration saved by `fleetctl auth setup`.
type Config struct {
	IssuerURL             string   `json:"issuer_url"`
	ClientID              string   `json:"client_id"`
	Scopes                []string `json:"scopes"`
	AuthorizationEndpoint string   `json:"authorization_endpoint"`
	TokenEndpoint         string   `json:"token_endpoint"`
	KeyEnrollmentClientID string   `json:"key_enrollment_client_id,omitempty"`
	OIDCCAFile            string   `json:"oidc_ca_file,omitempty"`
}

// HTTPClient returns an *http.Client that trusts the CA certificate at
// cfg.OIDCCAFile (in addition to system CAs). Returns nil if OIDCCAFile
// is not set.
func (cfg Config) HTTPClient() (*http.Client, error) {
	if cfg.OIDCCAFile == "" {
		return nil, nil
	}
	caPEM, err := os.ReadFile(cfg.OIDCCAFile)
	if err != nil {
		return nil, fmt.Errorf("read CA file %s: %w", cfg.OIDCCAFile, err)
	}
	pool, err := x509.SystemCertPool()
	if err != nil {
		pool = x509.NewCertPool()
	}
	pool.AppendCertsFromPEM(caPEM)
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: pool},
		},
	}, nil
}

func configDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("get home dir: %w", err)
	}
	return filepath.Join(home, ".config", "fleetshift"), nil
}

func configPath() (string, error) {
	dir, err := configDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "auth.json"), nil
}

// SaveConfig writes the auth config to ~/.config/fleetshift/auth.json.
func SaveConfig(cfg Config) error {
	p, err := configPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	if err := os.WriteFile(p, data, 0o600); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	return nil
}

// LoadConfig reads the auth config from ~/.config/fleetshift/auth.json.
func LoadConfig() (Config, error) {
	p, err := configPath()
	if err != nil {
		return Config{}, err
	}
	data, err := os.ReadFile(p)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	return cfg, nil
}
