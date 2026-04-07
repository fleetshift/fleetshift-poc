package kind

import (
	"encoding/json"
	"fmt"

	"k8s.io/client-go/tools/clientcmd"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// KubernetesTargetType is the [domain.TargetType] for Kubernetes
// clusters provisioned by the kind addon. The kubernetes-direct
// delivery agent handles delivery to these targets.
const KubernetesTargetType domain.TargetType = "kubernetes"

// ClusterOutput captures what a successful kind cluster creation
// produces: connection info for the provisioned cluster and,
// optionally, a platform ServiceAccount token for attested delivery.
type ClusterOutput struct {
	TargetID  domain.TargetID
	Name      string
	APIServer string // e.g. "https://127.0.0.1:PORT"
	CACert    []byte // PEM-encoded cluster CA certificate

	// SATokenRef and SAToken are set when platform SA bootstrapping
	// succeeds. SATokenRef is a vault key; SAToken is the raw bearer
	// token stored under that key.
	SATokenRef domain.SecretRef
	SAToken    []byte

	// TrustBundles are the IdP trust configs to embed in the
	// provisioned target's properties. Set by the kind agent from
	// its in-memory trust store at provisioning time.
	TrustBundles []domain.TrustBundleEntry
}

// Target returns a [domain.ProvisionedTarget] with connection info
// stored as properties. When a platform SA token was provisioned, the
// target includes a service_account_token_ref pointing at the vault
// secret rather than embedding the raw credential.
func (o *ClusterOutput) Target() domain.ProvisionedTarget {
	props := map[string]string{
		"api_server": o.APIServer,
	}
	if len(o.CACert) > 0 {
		props["ca_cert"] = string(o.CACert)
	}
	if o.SATokenRef != "" {
		props["service_account_token_ref"] = string(o.SATokenRef)
	}
	if len(o.TrustBundles) > 0 {
		if data, err := json.Marshal(o.TrustBundles); err == nil {
			props["trust_bundle"] = string(data)
		}
	}
	return domain.ProvisionedTarget{
		ID:                    o.TargetID,
		Type:                  KubernetesTargetType,
		Name:                  o.Name,
		Properties:            props,
		AcceptedResourceTypes: []domain.ResourceType{kubernetes.ManifestResourceType},
	}
}

// Secrets returns the [domain.ProducedSecret] entries that should be
// stored in the vault for this cluster. Returns nil when no platform
// SA was bootstrapped.
func (o *ClusterOutput) Secrets() []domain.ProducedSecret {
	if o.SATokenRef == "" {
		return nil
	}
	return []domain.ProducedSecret{{
		Ref:   o.SATokenRef,
		Value: o.SAToken,
	}}
}

// ExtractClusterConnInfo parses a kubeconfig to extract the API server
// URL and CA certificate for the first cluster. This is used to capture
// connection info from kind's admin kubeconfig without storing the full
// kubeconfig (which contains privileged credentials).
func ExtractClusterConnInfo(kubeconfig []byte) (apiServer string, caCert []byte, err error) {
	cfg, err := clientcmd.Load(kubeconfig)
	if err != nil {
		return "", nil, fmt.Errorf("parse kubeconfig: %w", err)
	}
	for _, cluster := range cfg.Clusters {
		return cluster.Server, cluster.CertificateAuthorityData, nil
	}
	return "", nil, fmt.Errorf("kubeconfig contains no clusters")
}
