package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// EngineConfig holds ocp-engine-specific configuration (not part of install-config.yaml)
type EngineConfig struct {
	ReleaseImage              string         `yaml:"release_image"`
	PullSecretFile            string         `yaml:"pull_secret_file"`
	SSHPublicKeyFile          string         `yaml:"ssh_public_key_file"`
	AdditionalTrustBundleFile string         `yaml:"additional_trust_bundle_file"`
	Credentials               AWSCredentials `yaml:"credentials"`
}

// AWSCredentials defines various credential modes
type AWSCredentials struct {
	AccessKeyID     string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`
	SessionToken    string `yaml:"session_token"`
	CredentialsFile string `yaml:"credentials_file"`
	Profile         string `yaml:"profile"`
	RoleARN         string `yaml:"role_arn"`
}

// ClusterConfig holds the full parsed configuration
type ClusterConfig struct {
	Engine        EngineConfig
	InstallConfig map[string]any
}

// LoadConfig reads and parses a config file from disk
func LoadConfig(path string) (*ClusterConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	return ParseConfig(data)
}

// ParseConfig parses YAML config data, extracts the ocp_engine section, and validates
func ParseConfig(data []byte) (*ClusterConfig, error) {
	var raw map[string]any
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	// Extract and parse ocp_engine section
	var engine EngineConfig
	if engineRaw, ok := raw["ocp_engine"]; ok {
		engineBytes, err := yaml.Marshal(engineRaw)
		if err != nil {
			return nil, fmt.Errorf("failed to re-marshal ocp_engine section: %w", err)
		}
		if err := yaml.Unmarshal(engineBytes, &engine); err != nil {
			return nil, fmt.Errorf("failed to parse ocp_engine section: %w", err)
		}
		delete(raw, "ocp_engine")
	}

	expandPaths(&engine)

	if err := validate(&engine, raw); err != nil {
		return nil, err
	}

	return &ClusterConfig{
		Engine:        engine,
		InstallConfig: raw,
	}, nil
}

// validate checks that required fields are present
func validate(engine *EngineConfig, ic map[string]any) error {
	if engine.PullSecretFile == "" {
		return fmt.Errorf("ocp_engine.pull_secret_file is required")
	}
	if !hasCredentials(&engine.Credentials) {
		return fmt.Errorf("ocp_engine.credentials is required (at least one credential mode must be set)")
	}
	if _, ok := ic["baseDomain"]; !ok {
		return fmt.Errorf("baseDomain is required")
	}
	metadata, ok := ic["metadata"].(map[string]any)
	if !ok || metadata["name"] == nil {
		return fmt.Errorf("metadata.name is required")
	}
	platform, ok := ic["platform"].(map[string]any)
	if !ok {
		return fmt.Errorf("platform is required")
	}
	aws, ok := platform["aws"].(map[string]any)
	if !ok {
		return fmt.Errorf("platform.aws is required")
	}
	if _, ok := aws["region"]; !ok {
		return fmt.Errorf("platform.aws.region is required")
	}
	return nil
}

// expandTilde replaces a leading ~ with the user's home directory.
func expandTilde(path string) string {
	if strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return path
		}
		return filepath.Join(home, path[2:])
	}
	return path
}

// expandPaths resolves ~ in all file path fields.
func expandPaths(engine *EngineConfig) {
	engine.PullSecretFile = expandTilde(engine.PullSecretFile)
	engine.SSHPublicKeyFile = expandTilde(engine.SSHPublicKeyFile)
	engine.AdditionalTrustBundleFile = expandTilde(engine.AdditionalTrustBundleFile)
	engine.Credentials.CredentialsFile = expandTilde(engine.Credentials.CredentialsFile)
}

// hasCredentials checks if at least one credential mode is configured
func hasCredentials(c *AWSCredentials) bool {
	return c.AccessKeyID != "" ||
		c.CredentialsFile != "" ||
		c.Profile != "" ||
		c.RoleARN != ""
}

// GenerateInstallConfig produces a valid install-config.yaml by inlining
// file contents into the pass-through install-config map.
func GenerateInstallConfig(cfg *ClusterConfig) ([]byte, error) {
	ic := cfg.InstallConfig

	if _, ok := ic["apiVersion"]; !ok {
		ic["apiVersion"] = "v1"
	}

	pullSecretData, err := os.ReadFile(cfg.Engine.PullSecretFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read pull secret file: %w", err)
	}
	ic["pullSecret"] = strings.TrimSpace(string(pullSecretData))

	if cfg.Engine.SSHPublicKeyFile != "" {
		sshKeyData, err := os.ReadFile(cfg.Engine.SSHPublicKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read SSH public key file: %w", err)
		}
		ic["sshKey"] = strings.TrimSpace(string(sshKeyData))
	}

	if cfg.Engine.AdditionalTrustBundleFile != "" {
		trustBundleData, err := os.ReadFile(cfg.Engine.AdditionalTrustBundleFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read additional trust bundle file: %w", err)
		}
		ic["additionalTrustBundle"] = strings.TrimSpace(string(trustBundleData))
	}

	return yaml.Marshal(ic)
}
