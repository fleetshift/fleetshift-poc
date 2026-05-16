package gcphcp

import (
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// ClusterSpec defines the declarative specification for a GCP HCP cluster.
type ClusterSpec struct {
	Name           string         `json:"name"`
	EndpointAccess string         `json:"endpointAccess,omitempty"`
	ReleaseVersion string         `json:"releaseVersion,omitempty"`
	ChannelGroup   string         `json:"channelGroup,omitempty"`
	Nodepools      []NodepoolSpec `json:"nodepools,omitempty"`
}

// NodepoolSpec defines the specification for a GCP HCP cluster nodepool.
type NodepoolSpec struct {
	Name           string `json:"name,omitempty"`
	Replicas       int    `json:"replicas,omitempty"`
	InstanceType   string `json:"instanceType,omitempty"`
	RootVolumeSize int    `json:"rootVolumeSize,omitempty"`
	RootVolumeType string `json:"rootVolumeType,omitempty"`
	AutoRepair     bool   `json:"autoRepair,omitempty"`
	UpgradeType    string `json:"upgradeType,omitempty"`
}

var clusterNamePattern = regexp.MustCompile(`^[a-z][-a-z0-9]*$`)

// ParseClusterSpec unmarshals and validates a ClusterSpec from JSON.
func ParseClusterSpec(raw json.RawMessage) (ClusterSpec, error) {
	var spec ClusterSpec
	if err := json.Unmarshal(raw, &spec); err != nil {
		return ClusterSpec{}, fmt.Errorf("failed to unmarshal cluster spec: %w", err)
	}

	if err := validateClusterSpec(&spec); err != nil {
		return ClusterSpec{}, err
	}

	return spec, nil
}

func validateClusterSpec(spec *ClusterSpec) error {
	if spec.Name == "" {
		return fmt.Errorf("%w: cluster name is required", domain.ErrInvalidArgument)
	}

	if len(spec.Name) > 15 {
		return fmt.Errorf("%w: cluster name must be 15 characters or less (got %d)",
			domain.ErrInvalidArgument, len(spec.Name))
	}

	if !clusterNamePattern.MatchString(spec.Name) {
		return fmt.Errorf("%w: cluster name must match pattern ^[a-z][-a-z0-9]*$ (got %q)",
			domain.ErrInvalidArgument, spec.Name)
	}

	for i, np := range spec.Nodepools {
		if np.Replicas < 0 {
			return fmt.Errorf("%w: nodepools[%d].replicas must be >= 0 (got %d)",
				domain.ErrInvalidArgument, i, np.Replicas)
		}
		if np.RootVolumeSize < 0 {
			return fmt.Errorf("%w: nodepools[%d].rootVolumeSize must be >= 0 (got %d)",
				domain.ErrInvalidArgument, i, np.RootVolumeSize)
		}
	}

	return nil
}

// ApplyDefaults fills in default values for unspecified fields.
func (s *ClusterSpec) ApplyDefaults() {
	if s.EndpointAccess == "" {
		s.EndpointAccess = "PublicAndPrivate"
	}

	// If no nodepools are specified, create one with defaults
	if len(s.Nodepools) == 0 {
		s.Nodepools = []NodepoolSpec{{}}
	}

	// Apply defaults to each nodepool
	for i := range s.Nodepools {
		np := &s.Nodepools[i]

		// Determine if this nodepool needs defaults by checking if Replicas is unset.
		// If Replicas == 0, assume the user didn't specify values and we should apply all defaults.
		// If Replicas != 0, assume the user set values and we should preserve them.
		wasDefault := np.Replicas == 0

		if np.Name == "" {
			np.Name = fmt.Sprintf("%s-nodepool-%d", s.Name, i+1)
		}
		if np.Replicas == 0 {
			np.Replicas = 2
		}
		if np.InstanceType == "" {
			np.InstanceType = "n1-standard-4"
		}
		if np.RootVolumeSize == 0 {
			np.RootVolumeSize = 128
		}
		if np.RootVolumeType == "" {
			np.RootVolumeType = "pd-standard"
		}
		if np.UpgradeType == "" {
			np.UpgradeType = "Replace"
		}

		// Only set AutoRepair to true if this nodepool was using default values.
		// This heuristic allows us to distinguish between:
		// - Empty/partial nodepools (Replicas==0) → apply AutoRepair=true
		// - User-specified nodepools (Replicas!=0) → preserve AutoRepair value
		if wasDefault {
			np.AutoRepair = true
		}
	}
}
