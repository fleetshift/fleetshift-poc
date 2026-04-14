package ocp

import (
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// KubernetesTargetType is the [domain.TargetType] for OCP clusters
// provisioned by the ocp addon. The kubernetes-direct delivery agent
// handles delivery to these targets.
const KubernetesTargetType domain.TargetType = "kubernetes"

// ClusterOutput captures what a successful OCP cluster provisioning
// produces: connection info for the provisioned cluster, vault-stored
// credentials (ServiceAccount token, kubeconfig, SSH key), and OCP
// cluster metadata (InfraID, ClusterID, Region).
type ClusterOutput struct {
	TargetID   domain.TargetID
	Name       string
	APIServer  string // e.g. "https://api.example.openshiftapps.com:6443"
	CACert     []byte // PEM-encoded cluster CA certificate
	InfraID    string // OCP infrastructure ID
	ClusterID  string // OCP cluster UUID
	Region     string // cloud region (e.g. "us-east-1")

	// SATokenRef and SAToken are set when platform SA bootstrapping
	// succeeds. SATokenRef is a vault key; SAToken is the raw bearer
	// token stored under that key.
	SATokenRef domain.SecretRef
	SAToken    []byte

	// KubeconfigRef and Kubeconfig hold the admin kubeconfig. The ref
	// is a vault key; Kubeconfig is the raw YAML stored under that key.
	KubeconfigRef domain.SecretRef
	Kubeconfig    []byte

	// SSHKeyRef and SSHPrivateKey hold the SSH private key for cluster
	// node access. The ref is a vault key; SSHPrivateKey is the raw PEM.
	SSHKeyRef     domain.SecretRef
	SSHPrivateKey []byte
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
	if o.InfraID != "" {
		props["infra_id"] = o.InfraID
	}
	if o.ClusterID != "" {
		props["cluster_id"] = o.ClusterID
	}
	if o.Region != "" {
		props["region"] = o.Region
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
// stored in the vault for this cluster. Returns up to 3 secrets (SA
// token, kubeconfig, SSH key), omitting any whose refs are not set.
func (o *ClusterOutput) Secrets() []domain.ProducedSecret {
	var secrets []domain.ProducedSecret
	if o.SATokenRef != "" {
		secrets = append(secrets, domain.ProducedSecret{
			Ref:   o.SATokenRef,
			Value: o.SAToken,
		})
	}
	if o.KubeconfigRef != "" {
		secrets = append(secrets, domain.ProducedSecret{
			Ref:   o.KubeconfigRef,
			Value: o.Kubeconfig,
		})
	}
	if o.SSHKeyRef != "" {
		secrets = append(secrets, domain.ProducedSecret{
			Ref:   o.SSHKeyRef,
			Value: o.SSHPrivateKey,
		})
	}
	if len(secrets) == 0 {
		return nil
	}
	return secrets
}
