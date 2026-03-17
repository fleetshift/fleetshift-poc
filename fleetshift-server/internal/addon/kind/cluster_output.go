package kind

import (
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
// produces: connection info for the provisioned cluster. No secrets
// are stored; the admin kubeconfig is used ephemerally for bootstrap
// and then discarded.
type ClusterOutput struct {
	TargetID  domain.TargetID
	Name      string
	APIServer string // e.g. "https://127.0.0.1:PORT"
	CACert    []byte // PEM-encoded cluster CA certificate
}

// Target returns a [domain.ProvisionedTarget] with connection info
// stored as properties. No vault secrets are produced.
func (o *ClusterOutput) Target() domain.ProvisionedTarget {
	props := map[string]string{
		"api_server": o.APIServer,
	}
	if len(o.CACert) > 0 {
		props["ca_cert"] = string(o.CACert)
	}
	return domain.ProvisionedTarget{
		ID:                    o.TargetID,
		Type:                  KubernetesTargetType,
		Name:                  o.Name,
		Properties:            props,
		AcceptedResourceTypes: []domain.ResourceType{kubernetes.ManifestResourceType},
	}
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
