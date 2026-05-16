package gcphcp

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config represents the complete gcphcp addon configuration.
type Config struct {
	Gateway GatewayConfig  `yaml:"gateway"`
	Targets []TargetConfig `yaml:"targets"`
}

// GatewayConfig holds the configuration for the HCP backend gateway.
type GatewayConfig struct {
	URL      string `yaml:"url"`
	Audience string `yaml:"audience"`
}

// TargetConfig holds the configuration for a single GCP HCP target.
type TargetConfig struct {
	ID                string `yaml:"id"`
	GCPProject        string `yaml:"gcp_project"`
	Region            string `yaml:"region"`
	WorkforcePool     string `yaml:"workforce_pool"`
	WorkforceProvider string `yaml:"workforce_provider"`
	BrokerSAEmail     string `yaml:"broker_sa_email"`
}

// TargetProperties returns the target configuration as a string map
// suitable for use with domain.TargetInfo.Properties.
func (tc TargetConfig) TargetProperties() map[string]string {
	return map[string]string{
		"id":                 tc.ID,
		"gcp_project":        tc.GCPProject,
		"region":             tc.Region,
		"workforce_pool":     tc.WorkforcePool,
		"workforce_provider": tc.WorkforceProvider,
		"broker_sa_email":    tc.BrokerSAEmail,
	}
}

// ParseConfig reads and parses a YAML configuration file.
// It validates that all required fields are present.
func ParseConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("failed to parse config YAML: %w", err)
	}

	if err := validateConfig(cfg); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// validateConfig ensures all required fields are present and valid.
func validateConfig(cfg Config) error {
	// Validate gateway
	if cfg.Gateway.URL == "" {
		return fmt.Errorf("gateway.url is required")
	}
	if cfg.Gateway.Audience == "" {
		return fmt.Errorf("gateway.audience is required")
	}

	// V1 routes fulfillment through exactly one active target.
	if len(cfg.Targets) != 1 {
		return fmt.Errorf("exactly one target is required in v1, got %d", len(cfg.Targets))
	}

	for i, target := range cfg.Targets {
		if target.ID == "" {
			return fmt.Errorf("target[%d].id is required", i)
		}
		if target.GCPProject == "" {
			return fmt.Errorf("target[%d].gcp_project is required", i)
		}
		if target.Region == "" {
			return fmt.Errorf("target[%d].region is required", i)
		}
		if target.WorkforcePool == "" {
			return fmt.Errorf("target[%d].workforce_pool is required", i)
		}
		if target.WorkforceProvider == "" {
			return fmt.Errorf("target[%d].workforce_provider is required", i)
		}
		if target.BrokerSAEmail == "" {
			return fmt.Errorf("target[%d].broker_sa_email is required", i)
		}
	}

	return nil
}
