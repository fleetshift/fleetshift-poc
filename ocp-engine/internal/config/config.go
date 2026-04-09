package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// ClusterConfig represents the complete cluster configuration
type ClusterConfig struct {
	Cluster                   ClusterSpec    `yaml:"cluster"`
	Platform                  PlatformSpec   `yaml:"platform"`
	ControlPlane              NodePoolSpec   `yaml:"control_plane"`
	Compute                   NodePoolSpec   `yaml:"compute"`
	Networking                NetworkingSpec `yaml:"networking"`
	PullSecretFile            string         `yaml:"pull_secret_file"`
	SSHPublicKeyFile          string         `yaml:"ssh_public_key_file"`
	ReleaseImage              string         `yaml:"release_image"`
	AdditionalTrustBundleFile string         `yaml:"additional_trust_bundle_file"`
	FIPS                      bool           `yaml:"fips"`
	Publish                   string         `yaml:"publish"`
}

// ClusterSpec defines cluster metadata
type ClusterSpec struct {
	Name       string `yaml:"name"`
	BaseDomain string `yaml:"base_domain"`
	Version    string `yaml:"version"`
}

// PlatformSpec defines platform-specific configuration
type PlatformSpec struct {
	AWS AWSSpec `yaml:"aws"`
}

// AWSSpec defines AWS-specific configuration
type AWSSpec struct {
	Region      string            `yaml:"region"`
	Credentials AWSCredentials    `yaml:"credentials"`
	Tags        map[string]string `yaml:"tags"`
}

// AWSCredentials defines various credential modes
type AWSCredentials struct {
	// Inline credentials
	AccessKeyID     string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`

	// File-based credentials
	CredentialsFile string `yaml:"credentials_file"`

	// Profile-based credentials
	Profile string `yaml:"profile"`

	// STS role
	RoleARN string `yaml:"role_arn"`
}

// NodePoolSpec defines a node pool (control plane or compute)
type NodePoolSpec struct {
	Replicas     *int       `yaml:"replicas"`
	InstanceType string     `yaml:"instance_type"`
	RootVolume   VolumeSpec `yaml:"root_volume"`
}

// VolumeSpec defines volume configuration
type VolumeSpec struct {
	SizeGB int    `yaml:"size_gb"`
	Type   string `yaml:"type"`
}

// NetworkingSpec defines networking configuration
type NetworkingSpec struct {
	ClusterNetwork string `yaml:"cluster_network"`
	ServiceNetwork string `yaml:"service_network"`
	MachineNetwork string `yaml:"machine_network"`
	HostPrefix     int    `yaml:"host_prefix"`
}

// LoadConfig reads and parses a config file from disk
func LoadConfig(path string) (*ClusterConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	return ParseConfig(data)
}

// ParseConfig parses YAML config data, applies defaults, and validates
func ParseConfig(data []byte) (*ClusterConfig, error) {
	cfg := &ClusterConfig{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	applyDefaults(cfg)
	expandPaths(cfg)

	if err := validate(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

func applyNodePoolDefaults(pool *NodePoolSpec) {
	if pool.Replicas == nil {
		three := 3
		pool.Replicas = &three
	}
	if pool.InstanceType == "" {
		pool.InstanceType = "m6a.xlarge"
	}
	if pool.RootVolume.SizeGB == 0 {
		pool.RootVolume.SizeGB = 120
	}
	if pool.RootVolume.Type == "" {
		pool.RootVolume.Type = "gp3"
	}
}

// applyDefaults fills in default values for unset fields
func applyDefaults(cfg *ClusterConfig) {
	applyNodePoolDefaults(&cfg.ControlPlane)
	applyNodePoolDefaults(&cfg.Compute)

	// Networking defaults
	if cfg.Networking.ClusterNetwork == "" {
		cfg.Networking.ClusterNetwork = "10.128.0.0/14"
	}
	if cfg.Networking.ServiceNetwork == "" {
		cfg.Networking.ServiceNetwork = "172.30.0.0/16"
	}
	if cfg.Networking.MachineNetwork == "" {
		cfg.Networking.MachineNetwork = "10.0.0.0/16"
	}
	if cfg.Networking.HostPrefix == 0 {
		cfg.Networking.HostPrefix = 23
	}

	// Publish default
	if cfg.Publish == "" {
		cfg.Publish = "External"
	}
}

// validate checks that all required fields are set
func validate(cfg *ClusterConfig) error {
	if cfg.Cluster.Name == "" {
		return fmt.Errorf("cluster.name is required")
	}
	if cfg.Cluster.BaseDomain == "" {
		return fmt.Errorf("cluster.base_domain is required")
	}
	if cfg.Platform.AWS.Region == "" {
		return fmt.Errorf("platform.aws.region is required")
	}
	if !hasCredentials(&cfg.Platform.AWS.Credentials) {
		return fmt.Errorf("platform.aws.credentials is required (at least one credential mode must be set)")
	}
	if cfg.PullSecretFile == "" {
		return fmt.Errorf("pull_secret_file is required")
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
func expandPaths(cfg *ClusterConfig) {
	cfg.PullSecretFile = expandTilde(cfg.PullSecretFile)
	cfg.SSHPublicKeyFile = expandTilde(cfg.SSHPublicKeyFile)
	cfg.AdditionalTrustBundleFile = expandTilde(cfg.AdditionalTrustBundleFile)
	cfg.Platform.AWS.Credentials.CredentialsFile = expandTilde(cfg.Platform.AWS.Credentials.CredentialsFile)
}

// hasCredentials checks if at least one credential mode is configured
func hasCredentials(c *AWSCredentials) bool {
	return c.AccessKeyID != "" ||
		c.CredentialsFile != "" ||
		c.Profile != "" ||
		c.RoleARN != ""
}
