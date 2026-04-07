// Package hcp implements a [domain.DeliveryAgent] for managing HCP
// (Hosted Control Plane) clusters via AWS and HyperShift. Manifests
// are interpreted as HCP cluster specifications; delivery creates or
// updates clusters, and removal deletes them.
package hcp

import (
	"encoding/json"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// TargetType is the [domain.TargetType] for HCP-managed targets.
const TargetType domain.TargetType = "hcp"

// ClusterResourceType is the [domain.ResourceType] for HCP cluster
// specifications.
const ClusterResourceType domain.ResourceType = "api.hcp.cluster"

// KubernetesTargetType is the [domain.TargetType] for Kubernetes
// clusters provisioned by the HCP addon.
const KubernetesTargetType domain.TargetType = "kubernetes"

// ClusterSpec is the manifest payload accepted by the HCP delivery
// agent.
type ClusterSpec struct {
	Name                     string         `json:"name"`
	InfraID                  string         `json:"infraID,omitempty"`
	RoleARN                  string         `json:"roleArn"`
	Region                   string         `json:"region,omitempty"`
	BaseDomain               string         `json:"baseDomain,omitempty"`
	ReleaseImage             string         `json:"releaseImage,omitempty"`
	NodePools                []NodePoolSpec `json:"nodePools"`
	IDP                      *IDPSpec       `json:"idp,omitempty"`
	ControlPlaneAvailability string         `json:"controlPlaneAvailability,omitempty"`
}

// NodePoolSpec describes a node pool within an HCP cluster.
type NodePoolSpec struct {
	Name         string   `json:"name"`
	Replicas     int32    `json:"replicas"`
	InstanceType string   `json:"instanceType,omitempty"`
	Arch         string   `json:"arch,omitempty"`
	Zones        []string `json:"zones,omitempty"`
}

// IDPSpec configures an identity provider for the HCP cluster.
type IDPSpec struct {
	Name   string `json:"name"`
	Issuer string `json:"issuer"`
}

// validateManifests parses and validates a slice of manifests as HCP
// cluster specs. It applies defaults for optional fields.
func validateManifests(manifests []domain.Manifest) ([]ClusterSpec, error) {
	specs := make([]ClusterSpec, len(manifests))
	for i, m := range manifests {
		if err := json.Unmarshal(m.Raw, &specs[i]); err != nil {
			return nil, fmt.Errorf("unmarshal hcp cluster spec: %w", err)
		}
		if specs[i].Name == "" {
			return nil, fmt.Errorf("%w: hcp cluster spec requires a name", domain.ErrInvalidArgument)
		}
		if specs[i].RoleARN == "" {
			return nil, fmt.Errorf("%w: hcp cluster spec requires a roleArn", domain.ErrInvalidArgument)
		}
		if len(specs[i].NodePools) == 0 {
			return nil, fmt.Errorf("%w: hcp cluster spec requires at least one nodePool", domain.ErrInvalidArgument)
		}

		// Apply defaults.
		if specs[i].ControlPlaneAvailability == "" {
			specs[i].ControlPlaneAvailability = "HighlyAvailable"
		}
		for j := range specs[i].NodePools {
			if specs[i].NodePools[j].InstanceType == "" {
				specs[i].NodePools[j].InstanceType = "m6i.xlarge"
			}
			if specs[i].NodePools[j].Arch == "" {
				specs[i].NodePools[j].Arch = "amd64"
			}
		}
	}
	return specs, nil
}
