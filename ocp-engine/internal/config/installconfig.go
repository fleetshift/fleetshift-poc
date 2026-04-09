package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// installConfig represents the OpenShift install-config.yaml structure
type installConfig struct {
	APIVersion string       `yaml:"apiVersion"`
	BaseDomain string       `yaml:"baseDomain"`
	Metadata   icMetadata   `yaml:"metadata"`
	Platform   icPlatform   `yaml:"platform"`
	PullSecret string       `yaml:"pullSecret"`
	SSHKey     string       `yaml:"sshKey,omitempty"`
	Publish    string       `yaml:"publish"`
	Compute    []icMachinePool `yaml:"compute"`
	ControlPlane icMachinePool `yaml:"controlPlane"`
	Networking icNetworking `yaml:"networking"`
	FIPS                  bool   `yaml:"fips,omitempty"`
	AdditionalTrustBundle string `yaml:"additionalTrustBundle,omitempty"`
}

// icMetadata represents the metadata section
type icMetadata struct {
	Name string `yaml:"name"`
}

// icPlatform represents the platform section
type icPlatform struct {
	AWS icAWS `yaml:"aws"`
}

// icAWS represents AWS platform configuration
type icAWS struct {
	Region string `yaml:"region"`
}

// icMachinePool represents a machine pool (control plane or compute)
type icMachinePool struct {
	Name     string         `yaml:"name,omitempty"`
	Replicas *int           `yaml:"replicas"`
	Platform icPoolPlatform `yaml:"platform"`
}

// icPoolPlatform represents platform-specific pool configuration
type icPoolPlatform struct {
	AWS icPoolAWS `yaml:"aws"`
}

// icPoolAWS represents AWS-specific pool configuration
type icPoolAWS struct {
	InstanceType string       `yaml:"type"`
	RootVolume   icRootVolume `yaml:"rootVolume"`
}

// icRootVolume represents root volume configuration
type icRootVolume struct {
	Size int    `yaml:"size"`
	Type string `yaml:"type"`
}

// icNetworking represents networking configuration
type icNetworking struct {
	ClusterNetwork []icCIDRBlock `yaml:"clusterNetwork"`
	ServiceNetwork []string      `yaml:"serviceNetwork"`
	MachineNetwork []icCIDRBlock `yaml:"machineNetwork"`
}

// icCIDRBlock represents a CIDR block with optional host prefix
type icCIDRBlock struct {
	CIDR       string `yaml:"cidr"`
	HostPrefix int    `yaml:"hostPrefix,omitempty"`
}

// GenerateInstallConfig takes a ClusterConfig and produces a valid install-config.yaml
func GenerateInstallConfig(cfg *ClusterConfig) ([]byte, error) {
	// Read pull secret from file
	pullSecretData, err := os.ReadFile(cfg.PullSecretFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read pull secret file: %w", err)
	}
	pullSecret := strings.TrimSpace(string(pullSecretData))

	// Optionally read SSH key from file
	var sshKey string
	if cfg.SSHPublicKeyFile != "" {
		sshKeyData, err := os.ReadFile(cfg.SSHPublicKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read SSH public key file: %w", err)
		}
		sshKey = strings.TrimSpace(string(sshKeyData))
	}

	// Optionally read additional trust bundle from file
	var additionalTrustBundle string
	if cfg.AdditionalTrustBundleFile != "" {
		trustBundleData, err := os.ReadFile(cfg.AdditionalTrustBundleFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read additional trust bundle file: %w", err)
		}
		additionalTrustBundle = strings.TrimSpace(string(trustBundleData))
	}

	// Build the install-config struct
	ic := installConfig{
		APIVersion: "v1",
		BaseDomain: cfg.Cluster.BaseDomain,
		Metadata: icMetadata{
			Name: cfg.Cluster.Name,
		},
		Platform: icPlatform{
			AWS: icAWS{
				Region: cfg.Platform.AWS.Region,
			},
		},
		PullSecret: pullSecret,
		SSHKey:     sshKey,
		Publish:    cfg.Publish,
		ControlPlane: icMachinePool{
			Name:     "master",
			Replicas: cfg.ControlPlane.Replicas,
			Platform: icPoolPlatform{
				AWS: icPoolAWS{
					InstanceType: cfg.ControlPlane.InstanceType,
					RootVolume: icRootVolume{
						Size: cfg.ControlPlane.RootVolume.SizeGB,
						Type: cfg.ControlPlane.RootVolume.Type,
					},
				},
			},
		},
		Compute: []icMachinePool{
			{
				Name:     "worker",
				Replicas: cfg.Compute.Replicas,
				Platform: icPoolPlatform{
					AWS: icPoolAWS{
						InstanceType: cfg.Compute.InstanceType,
						RootVolume: icRootVolume{
							Size: cfg.Compute.RootVolume.SizeGB,
							Type: cfg.Compute.RootVolume.Type,
						},
					},
				},
			},
		},
		Networking: icNetworking{
			ClusterNetwork: []icCIDRBlock{
				{
					CIDR:       cfg.Networking.ClusterNetwork,
					HostPrefix: cfg.Networking.HostPrefix,
				},
			},
			ServiceNetwork: []string{cfg.Networking.ServiceNetwork},
			MachineNetwork: []icCIDRBlock{
				{
					CIDR: cfg.Networking.MachineNetwork,
				},
			},
		},
		FIPS:                  cfg.FIPS,
		AdditionalTrustBundle: additionalTrustBundle,
	}

	// Marshal to YAML
	return yaml.Marshal(&ic)
}
