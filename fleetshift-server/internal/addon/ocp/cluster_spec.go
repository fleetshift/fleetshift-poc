package ocp

import (
	"encoding/json"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"gopkg.in/yaml.v3"
)

const TargetType domain.TargetType = "ocp"
const ClusterResourceType domain.ResourceType = "api.ocp.cluster"

// ClusterSpec defines the declarative specification for an OCP cluster.
type ClusterSpec struct {
	Name          string         `json:"name"`
	BaseDomain    string         `json:"base_domain"`
	ReleaseImage  string         `json:"release_image,omitempty"`
	InstallConfig map[string]any `json:"install_config,omitempty"`
}

// ParseClusterSpec extracts the ClusterSpec from deployment manifests.
// Returns an error if no OCP cluster manifest is found or if required
// fields (Name, BaseDomain) are missing.
func ParseClusterSpec(manifests []domain.Manifest) (*ClusterSpec, error) {
	// Find the first manifest with ClusterResourceType
	var clusterManifest *domain.Manifest
	for i := range manifests {
		if manifests[i].ResourceType == ClusterResourceType {
			clusterManifest = &manifests[i]
			break
		}
	}

	if clusterManifest == nil {
		return nil, fmt.Errorf("no OCP cluster manifest found (expected ResourceType=%s)", ClusterResourceType)
	}

	// Unmarshal the Raw JSON into ClusterSpec
	var spec ClusterSpec
	if err := json.Unmarshal(clusterManifest.Raw, &spec); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cluster spec: %w", err)
	}

	// Validate required fields
	if spec.Name == "" {
		return nil, fmt.Errorf("cluster spec missing required field: name")
	}
	if spec.BaseDomain == "" {
		return nil, fmt.Errorf("cluster spec missing required field: base_domain")
	}

	return &spec, nil
}

// BuildClusterYAML generates ocp-engine's cluster.yaml configuration.
// The output includes:
// - ocp_engine section with pull_secret_file and optional release_image
// - baseDomain from spec
// - credentialsMode: Manual (for CCO STS mode)
// - metadata.name from spec
// - platform.aws.region from the region parameter
// - sshKey from sshPublicKey parameter
// - All fields from spec.InstallConfig merged in (pass-through fields)
//
// InstallConfig fields are applied first, then base keys are set on top,
// so base keys (baseDomain, credentialsMode, metadata, ocp_engine, sshKey)
// are always authoritative. The platform key is deep-merged so that
// user-provided platform fields (e.g. platform.aws.type) coexist with
// the base region without clobbering each other.
func BuildClusterYAML(spec *ClusterSpec, region, pullSecretFile, sshPublicKey string) ([]byte, error) {
	// Apply InstallConfig pass-through fields first
	config := make(map[string]any, len(spec.InstallConfig)+6)
	for key, value := range spec.InstallConfig {
		config[key] = value
	}

	// Set base keys on top — these are authoritative and cannot be
	// overridden by InstallConfig.
	config["ocp_engine"] = map[string]any{
		"pull_secret_file": pullSecretFile,
	}
	config["baseDomain"] = spec.BaseDomain
	config["credentialsMode"] = "Manual"
	config["metadata"] = map[string]any{
		"name": spec.Name,
	}
	config["sshKey"] = sshPublicKey

	// Deep-merge platform: preserve user-provided platform fields (e.g.
	// platform.aws.type) while ensuring region is always set.
	basePlatformAWS := map[string]any{"region": region}
	if userPlatform, ok := config["platform"].(map[string]any); ok {
		if userAWS, ok := userPlatform["aws"].(map[string]any); ok {
			for k, v := range userAWS {
				if _, reserved := basePlatformAWS[k]; !reserved {
					basePlatformAWS[k] = v
				}
			}
		}
	}
	config["platform"] = map[string]any{
		"aws": basePlatformAWS,
	}

	// Add release_image to ocp_engine section if specified
	if spec.ReleaseImage != "" {
		ocpEngine := config["ocp_engine"].(map[string]any)
		ocpEngine["release_image"] = spec.ReleaseImage
	}

	// Marshal to YAML
	yamlBytes, err := yaml.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal cluster config to YAML: %w", err)
	}

	return yamlBytes, nil
}
