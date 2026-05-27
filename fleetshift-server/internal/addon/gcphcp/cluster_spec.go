package gcphcp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// ClusterSpec defines the declarative specification for a GCP HCP cluster.
// Name is derived from the managed resource ID and set by the addon.
type ClusterSpec struct {
	Name           string         `json:"name"`
	EndpointAccess string         `json:"endpointAccess"`
	ReleaseVersion string         `json:"releaseVersion"`
	ChannelGroup   string         `json:"channelGroup"`
	Nodepools      []NodepoolSpec `json:"nodepools"`
}

// NodepoolSpec defines the specification for a GCP HCP cluster nodepool.
// All fields are required.
type NodepoolSpec struct {
	ID             string `json:"id"`
	Replicas       int    `json:"replicas"`
	InstanceType   string `json:"instanceType"`
	RootVolumeSize int    `json:"rootVolumeSize"`
	RootVolumeType string `json:"rootVolumeType"`
	AutoRepair     *bool  `json:"autoRepair"`
	UpgradeType    string `json:"upgradeType"`
}

var lowerKebabPattern = regexp.MustCompile(`^[a-z][-a-z0-9]*$`)

// NodepoolName derives the full CLS nodepool name from the cluster name and nodepool id.
func NodepoolName(clusterName, poolID string) string {
	return clusterName + "-" + poolID
}

// ParseClusterSpec unmarshals and validates a ClusterSpec from JSON.
func ParseClusterSpec(raw json.RawMessage) (ClusterSpec, error) {
	var spec ClusterSpec
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&spec); err != nil {
		return ClusterSpec{}, fmt.Errorf("failed to unmarshal cluster spec: %w", err)
	}

	if err := validateClusterSpec(&spec); err != nil {
		return ClusterSpec{}, err
	}

	return spec, nil
}

// ValidateClusterName checks that a cluster name (derived from the managed
// resource ID) meets CLS backend constraints.
func ValidateClusterName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: cluster name is required", domain.ErrInvalidArgument)
	}
	if len(name) > 15 {
		return fmt.Errorf("%w: cluster name must be 15 characters or less (got %d)",
			domain.ErrInvalidArgument, len(name))
	}
	if !lowerKebabPattern.MatchString(name) {
		return fmt.Errorf("%w: cluster name must match pattern ^[a-z][-a-z0-9]*$ (got %q)",
			domain.ErrInvalidArgument, name)
	}
	return nil
}

func validateClusterSpec(spec *ClusterSpec) error {
	if spec.EndpointAccess == "" {
		return fmt.Errorf("%w: endpointAccess is required", domain.ErrInvalidArgument)
	}
	if spec.ReleaseVersion == "" {
		return fmt.Errorf("%w: releaseVersion is required", domain.ErrInvalidArgument)
	}
	if spec.ChannelGroup == "" {
		return fmt.Errorf("%w: channelGroup is required", domain.ErrInvalidArgument)
	}

	if len(spec.Nodepools) == 0 {
		return fmt.Errorf("%w: at least one nodepool is required", domain.ErrInvalidArgument)
	}

	seenIDs := make(map[string]struct{}, len(spec.Nodepools))
	for i, np := range spec.Nodepools {
		if np.ID == "" {
			return fmt.Errorf("%w: nodepools[%d].id is required",
				domain.ErrInvalidArgument, i)
		}
		if len(np.ID) > 10 {
			return fmt.Errorf("%w: nodepools[%d].id must be 10 characters or less (got %d)",
				domain.ErrInvalidArgument, i, len(np.ID))
		}
		if !lowerKebabPattern.MatchString(np.ID) {
			return fmt.Errorf("%w: nodepools[%d].id must match pattern ^[a-z][-a-z0-9]*$ (got %q)",
				domain.ErrInvalidArgument, i, np.ID)
		}
		if _, exists := seenIDs[np.ID]; exists {
			return fmt.Errorf("%w: duplicate nodepool id %q",
				domain.ErrInvalidArgument, np.ID)
		}
		seenIDs[np.ID] = struct{}{}
		if np.Replicas <= 0 {
			return fmt.Errorf("%w: nodepools[%d].replicas must be > 0 (got %d)",
				domain.ErrInvalidArgument, i, np.Replicas)
		}
		if np.InstanceType == "" {
			return fmt.Errorf("%w: nodepools[%d].instanceType is required",
				domain.ErrInvalidArgument, i)
		}
		if np.RootVolumeSize <= 0 {
			return fmt.Errorf("%w: nodepools[%d].rootVolumeSize must be > 0 (got %d)",
				domain.ErrInvalidArgument, i, np.RootVolumeSize)
		}
		if np.RootVolumeType == "" {
			return fmt.Errorf("%w: nodepools[%d].rootVolumeType is required",
				domain.ErrInvalidArgument, i)
		}
		if np.AutoRepair == nil {
			return fmt.Errorf("%w: nodepools[%d].autoRepair is required",
				domain.ErrInvalidArgument, i)
		}
		if np.UpgradeType == "" {
			return fmt.Errorf("%w: nodepools[%d].upgradeType is required",
				domain.ErrInvalidArgument, i)
		}
	}

	return nil
}
