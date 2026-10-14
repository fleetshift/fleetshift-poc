package gcphcp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// clusterSpecJSON is the inner managed-resource spec wire shape
// ([GCPHCPClusterSpec]). Cluster identity is not part of this payload.
type clusterSpecJSON struct {
	EndpointAccess string         `json:"endpointAccess"`
	ReleaseVersion string         `json:"releaseVersion"`
	ChannelGroup   string         `json:"channelGroup"`
	Nodepools      []NodepoolSpec `json:"nodepools"`
}

// ClusterSpec is the parsed cluster specification used by the agent and
// reconciler. ResourceName is bound from the managed resource envelope
// (e.g. clusters/c1), not from the inner JSON payload.
type ClusterSpec struct {
	ResourceName   domain.ResourceName
	EndpointAccess string
	ReleaseVersion string
	ChannelGroup   string
	Nodepools      []NodepoolSpec
}

// NodepoolSpec defines the specification for a GCP HCP cluster nodepool.
// All fields are required. JSON tags are used only while decoding the
// inner wire spec into [clusterSpecJSON].
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

// clusterName returns the guest-target cluster name (resource ID).
func (s ClusterSpec) clusterName() string {
	return string(s.ResourceName.ID())
}

// ParseClusterSpec unmarshals the inner cluster JSON from mrs and binds
// identity from mrs.Name (e.g. clusters/c1).
func ParseClusterSpec(mrs *domain.ManagedResourceSpecManifest) (ClusterSpec, error) {
	if mrs == nil {
		return ClusterSpec{}, fmt.Errorf("%w: managed resource spec is required", domain.ErrInvalidArgument)
	}
	resourceName, err := kubernetes.ParseClusterResourceName(string(mrs.Name))
	if err != nil {
		return ClusterSpec{}, fmt.Errorf("%w: %v", domain.ErrInvalidArgument, err)
	}
	if err := ValidateClusterName(string(resourceName.ID())); err != nil {
		return ClusterSpec{}, err
	}

	var raw clusterSpecJSON
	dec := json.NewDecoder(bytes.NewReader(mrs.Spec))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&raw); err != nil {
		return ClusterSpec{}, fmt.Errorf("failed to unmarshal cluster spec: %w", err)
	}

	spec := ClusterSpec{
		ResourceName:   resourceName,
		EndpointAccess: raw.EndpointAccess,
		ReleaseVersion: raw.ReleaseVersion,
		ChannelGroup:   raw.ChannelGroup,
		Nodepools:      raw.Nodepools,
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
