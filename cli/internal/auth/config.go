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

// Config is Fleetctl's local OIDC client settings (issuer, client ID, scopes,
// discovered endpoints, and optional CA path) stored in auth.json.
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
// is not set. The client uses [DefaultOIDCHTTPTimeout] so OIDC calls
// (token exchange, etc.) cannot hang indefinitely.
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
		Timeout: DefaultOIDCHTTPTimeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: pool},
		},
	}, nil
}

// configDir returns dir, or ~/.config/fleetshift when dir is empty.
func configDir(dir string) (string, error) {
	if dir != "" {
		return dir, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("get home dir: %w", err)
	}
	return filepath.Join(home, ".config", "fleetshift"), nil
}

// configPath returns the auth.json path under configDir(dir).
func configPath(dir string) (string, error) {
	dir, err := configDir(dir)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "auth.json"), nil
}

// SaveConfig writes the auth config to ~/.config/fleetshift/auth.json.
func SaveConfig(cfg Config) error {
	return SaveConfigTo("", cfg)
}

// LoadConfig reads the auth config from ~/.config/fleetshift/auth.json.
func LoadConfig() (Config, error) {
	return LoadConfigFrom("")
}

// SaveConfigTo writes the auth config under dir, or the default user
// config path when dir is empty.
func SaveConfigTo(dir string, cfg Config) error {
	p, err := configPath(dir)
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

// LoadConfigFrom reads the auth config from dir, or the default user
// config path when dir is empty.
func LoadConfigFrom(dir string) (Config, error) {
	p, err := configPath(dir)
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
