package kind

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// normalizeClusterManifest turns a kind cluster delivery manifest into
// the canonical [ClusterSpec]. The two manifest types use different
// envelopes and are handled on separate switch arms; once normalized,
// delivery and removal share one ClusterSpec code path.
//
//   - [ClusterManifestType]: bare [ClusterSpec] JSON
//   - [ManagedClusterManifestType]: [domain.ManagedResourceSpecManifest]
//     envelope; unwrap, decode the inner spec with the same decoder as
//     the bare path, then set identity from the envelope name
func normalizeClusterManifest(m domain.Manifest) (ClusterSpec, error) {
	switch m.ManifestType {
	case ClusterManifestType:
		return parseBareClusterSpec(m.Raw)
	case ManagedClusterManifestType:
		return parseManagedClusterSpec(m.Raw)
	default:
		return ClusterSpec{}, fmt.Errorf("%w: unsupported kind cluster manifest type %q", domain.ErrInvalidArgument, m.ManifestType)
	}
}

// parseBareClusterSpec decodes a bare [ClusterSpec] payload
// ([ClusterManifestType]). Unknown JSON fields are rejected so a
// managed-resource envelope cannot be silently misread as a bare spec.
func parseBareClusterSpec(raw json.RawMessage) (ClusterSpec, error) {
	spec, err := decodeClusterSpec(raw)
	if err != nil {
		return ClusterSpec{}, err
	}
	if err := validateClusterSpec(spec); err != nil {
		return ClusterSpec{}, err
	}
	rn, err := domain.NewResourceName("clusters", domain.ResourceID(spec.Name))
	if err != nil {
		return ClusterSpec{}, fmt.Errorf("%w: cluster resource name: %v", domain.ErrInvalidArgument, err)
	}
	spec.ResourceName = rn
	return spec, nil
}

// parseManagedClusterSpec unwraps a [domain.ManagedResourceSpecManifest]
// ([ManagedClusterManifestType]), decodes the inner spec with the same
// decoder as the bare path, then overlays cluster identity from the
// envelope resource name (the inner "name", if any, is ignored).
func parseManagedClusterSpec(raw json.RawMessage) (ClusterSpec, error) {
	mrs, err := domain.UnwrapManagedResourceSpec(raw)
	if err != nil {
		return ClusterSpec{}, fmt.Errorf("%w: %v", domain.ErrInvalidArgument, err)
	}

	spec, err := decodeClusterSpec(mrs.Spec)
	if err != nil {
		return ClusterSpec{}, err
	}
	spec.Name = string(mrs.Name.ID())
	spec.ResourceName = mrs.Name
	if err := validateClusterSpec(spec); err != nil {
		return ClusterSpec{}, err
	}
	return spec, nil
}

func decodeClusterSpec(raw json.RawMessage) (ClusterSpec, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var spec ClusterSpec
	if err := dec.Decode(&spec); err != nil {
		return ClusterSpec{}, fmt.Errorf("%w: unmarshal kind cluster spec: %v", domain.ErrInvalidArgument, err)
	}
	return spec, nil
}

func validateClusterSpec(spec ClusterSpec) error {
	if strings.TrimSpace(spec.Name) == "" {
		return fmt.Errorf("%w: kind cluster spec requires a name", domain.ErrInvalidArgument)
	}
	for _, n := range spec.Nodes {
		switch n.Role {
		case "control-plane", "worker":
		default:
			return fmt.Errorf("%w: invalid node role %q (must be \"control-plane\" or \"worker\")", domain.ErrInvalidArgument, n.Role)
		}
	}
	return nil
}

// buildKindConfig generates kind's v1alpha4 cluster configuration JSON
// from the structured [ClusterSpec] fields. The output is equivalent to
// what kind create cluster --config accepts.
func buildKindConfig(spec ClusterSpec) (json.RawMessage, error) {
	config := toKindConfig(spec)
	raw, err := json.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("marshal kind config: %w", err)
	}
	return raw, nil
}

// toKindConfig converts a [ClusterSpec] into the internal kindConfig
// representation. If no nodes are specified, a single control-plane
// node is defaulted.
func toKindConfig(spec ClusterSpec) kindConfig {
	config := kindConfig{
		Kind:       "Cluster",
		APIVersion: "kind.x-k8s.io/v1alpha4",
	}

	if len(spec.Nodes) == 0 {
		config.Nodes = []kindNode{{Role: "control-plane"}}
	} else {
		for _, n := range spec.Nodes {
			node := kindNode{Role: n.Role}
			if n.Image != "" {
				node.Image = n.Image
			}
			config.Nodes = append(config.Nodes, node)
		}
	}

	if spec.Networking != nil {
		net := &kindNetworking{}
		if spec.Networking.APIServerPort > 0 {
			net.APIServerPort = spec.Networking.APIServerPort
		}
		if spec.Networking.PodSubnet != "" {
			net.PodSubnet = spec.Networking.PodSubnet
		}
		if spec.Networking.ServiceSubnet != "" {
			net.ServiceSubnet = spec.Networking.ServiceSubnet
		}
		config.Networking = net
	}

	return config
}

// applyOIDCOverlay adds OIDC-related kubeadm patches and CA cert
// mounts to a kindConfig. It mutates config in place.
func applyOIDCOverlay(config *kindConfig, oidcSpec *OIDCSpec, issuerURL string, audience string, caCertHostPath string) {
	patch := fmt.Sprintf(`kind: ClusterConfiguration
apiServer:
  extraArgs:
    oidc-issuer-url: %q
    oidc-client-id: %q
    oidc-username-claim: %q
    oidc-groups-claim: %q
    oidc-signing-algs: "RS256,ES256"`, issuerURL, audience, oidcSpec.usernameClaim(), oidcSpec.groupsClaim())

	if caCertHostPath != "" {
		patch += fmt.Sprintf("\n    oidc-ca-file: %q", oidcCACertContainerPath)
	}

	config.KubeadmConfigPatches = append(config.KubeadmConfigPatches, patch)

	if caCertHostPath != "" {
		mount := kindMount{
			HostPath:      caCertHostPath,
			ContainerPath: oidcCACertContainerPath,
			ReadOnly:      true,
		}
		for i := range config.Nodes {
			if config.Nodes[i].Role == "control-plane" {
				config.Nodes[i].ExtraMounts = append(config.Nodes[i].ExtraMounts, mount)
			}
		}
	}
}

// kindConfig mirrors the subset of kind.x-k8s.io/v1alpha4 Cluster that
// we generate from the spec.
type kindConfig struct {
	Kind                 string          `json:"kind"`
	APIVersion           string          `json:"apiVersion"`
	Nodes                []kindNode      `json:"nodes,omitempty"`
	Networking           *kindNetworking `json:"networking,omitempty"`
	KubeadmConfigPatches []string        `json:"kubeadmConfigPatches,omitempty"`
}

type kindNode struct {
	Role        string      `json:"role"`
	Image       string      `json:"image,omitempty"`
	ExtraMounts []kindMount `json:"extraMounts,omitempty"`
}

type kindMount struct {
	HostPath      string `json:"hostPath"`
	ContainerPath string `json:"containerPath"`
	ReadOnly      bool   `json:"readOnly,omitempty"`
}

type kindNetworking struct {
	APIServerPort int32  `json:"apiServerPort,omitempty"`
	PodSubnet     string `json:"podSubnet,omitempty"`
	ServiceSubnet string `json:"serviceSubnet,omitempty"`
}
